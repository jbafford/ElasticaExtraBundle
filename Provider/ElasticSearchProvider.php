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
    
    public function basicSearchTerms(array $queries = [], array $filters = [])
    {
        if(!$queries)
            $queries = [['match_all' => []]];
        else
            $queries = array_values($queries);
        
        if($filters)
        {
            $filters = array_values($filters);
            
            if(count($filters) == 1)
                $filter = $filters[0];
            else
                $filter = ['and' => $filters];
            
            $filters = $filter;
        }
        
        if(count($queries) > 1)
            $queries = ['bool' => ['must' => $queries]];
        else
            $queries = $queries[0];
        
        if(!$filters)
            return ['query' => $queries];
        else
            return [
                'query' => [
                    'filtered' => [
                        'filter' => $filters,
                        'query' => $queries,
                    ],
                ],
            ];
    }
    
    protected function makeResultMap($terms)
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
        
        return $resultMapFn;
    }
    
    public function doSearch($type, array $terms, array $options = array())
    {
        $query = new \Elastica\Query($terms);
        
        $itemType = $this->container->get("fos_elastica.index.$this->index.$type");
        
        return $itemType->search($query, $options);
    }
    
    public function searchWithScroll($type, array $terms, array $options = array())
    {
        $resultMapFn = $this->makeResultMap($terms);
        
        $options = array_merge(array(
            'scroll' => '1m',
        ), $options);
        
        $query = new \Elastica\Query($terms);
        $itemType = $this->container->get("fos_elastica.index.$this->index.$type");
        $results = $itemType->search($query, ['search_type' => 'scan', 'scroll' => $options['scroll']]);
    	
    	$scrollID = $results->getResponse()->getScrollId();
        
        return new ElasticScrollSearch($itemType, $scrollID, $resultMapFn, $options);
    }
    
    public function searchPaginated($type, array $terms)
    {
        $query = new \Elastica\Query($terms);
        
        $finder = $this->container->get("fos_elastica.finder.$this->index.$type");
        
        return $finder->createPaginatorAdapter($query);
    }
    
    public function search($type, array $terms, array $options = array())
    {
        $resultMapFn = $this->makeResultMap($terms);
        
        $results = $this->doSearch($type, $terms);
        
        $arr = array_map($resultMapFn, $results->getResults());
        
        return array(
            'total' => $results->getTotalHits(),
            'results' => $arr,
        );
    }
}
