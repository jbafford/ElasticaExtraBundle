<?php

namespace Bafford\ElasticaExtraBundle\Provider;

use Symfony\Component\DependencyInjection\ContainerAware;

class ElasticSearchProvider extends ContainerAware
{
    protected $index;
    
    public function __construct($config)
    {
    	$this->index = $config['defaultIndex'];
    }
    
    public function setIndex($indexName)
    {
        $this->index = $indexName;
    }
    
    public function doSearch($type, array $terms)
    {
        $query = new \Elastica_Query($terms);
        
        $itemType = $this->container->get("foq_elastica.index.$this->index.$type");
        
        return $itemType->search($query);
    }
    
    public function search($type, array $terms)
    {
        $resultType = 'source';
        if(!empty($terms['fields']))
        {
            $hasID = (array_search('_id', $terms['fields']) !== false);
            
            if(count($terms['fields']) == 1)
            {
                $field = array_values($terms['fields'])[0];
                $resultType = ($hasID ? 'id' : 'field');
            }
            else
                $resultType = ($hasID ? 'fields+id' : 'fields');
        }
        
        switch($resultType)
        {
            case 'id':
                $resultMapFn = function($v) { return $v->getId(); };
                break;
            
            case 'source':
                $resultMapFn = function($v) { return array_merge(['_id' => $v->getId()], $v->getSource()); };
                break;
            
            case 'fields':
                $resultMapFn = function($v) { return $v->getFields(); };
                break;
            
            case 'fields+id':
                $resultMapFn = function($v) { return array_merge(['_id' => $v->getId()], $v->getFields()); };
                break;
            
            case 'field':
                $resultMapFn = function($v) use($field) { return $v->getFields()[$field]; };
        }
        
        $stopwatch = $this->container->get('debug.stopwatch');
        $stopwatch->start('elasticsearchQuery', 'ElasticaExtraBundle');
        
        $results = $this->doSearch($type, $terms);
        
        $stopwatch->stop('elasticsearchQuery');
        
        $arr = array_map($resultMapFn, $results->getResults());
        
        return array(
            'total' => $results->getTotalHits(),
            'results' => $arr,
        );
    }
}
