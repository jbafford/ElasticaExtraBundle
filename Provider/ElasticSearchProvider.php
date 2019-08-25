<?php declare(strict_types=1);

namespace Bafford\ElasticaExtraBundle\Provider;

use Symfony\Component\DependencyInjection\ContainerAwareInterface;
use Symfony\Component\DependencyInjection\ContainerAwareTrait;

class ElasticSearchProvider implements ContainerAwareInterface
{
    use ContainerAwareTrait;
    
    public function basicSearchTerms(array $queries = [], array $filters = [])
    {
        if(!$queries) {
            $queries = [['match_all' => (object)null]];
        } else {
            $queries = array_values($queries);
        }
        
        if($filters)
        {
            $filters = array_values($filters);
            
            if(count($filters) == 1) {
                $filter = $filters[0];
            } else {
                $filter = $filters;
            }
            
            $filters = $filter;
        }
        
        if(count($queries) > 1) {
            $queries = ['bool' => ['must' => $queries]];
        } else {
            $queries = $queries[0];
        }
        
        if(!$filters) {
            return ['query' => $queries];
        } else {
            return [
                'query' => [
                    'bool' => [
                        'must' => $queries,
                        'filter' => $filters,
                    ],
                ],
            ];
        }
    }
    
    protected function makeResultMap($terms) : \Closure
    {
        $resultType = 'source';
        if(!empty($terms['fields'])) {
            $hasID = (array_search('_id', $terms['fields']) !== false);
            
            if(count($terms['fields']) == 1) {
                $field = array_values($terms['fields'])[0];
                $resultType = ($hasID ? 'id' : 'field');
            } else {
                $resultType = ($hasID ? 'fields+id' : 'fields');
            }
        }
        
        switch($resultType) {
            case 'id':
                return function($v) { return $v->getId(); };
                break;
            
            case 'source':
                return function($v) { return array_merge(['_id' => $v->getId()], $v->getSource()); };
                break;
            
            case 'fields':
                return function($v) { return $v->getFields(); };
                break;
            
            case 'fields+id':
                return function($v) { return array_merge(['_id' => $v->getId()], $v->getFields()); };
                break;
            
            case 'field':
                return function($v) use($field) { return $v->getFields()[$field]; };
            
            default:
                throw new \InvalidArgumentException("Unknown resultType '$resultType'");
        }
    }
    
    public function doSearch(string $index, string $type, array $terms, array $options = [])
    {
        $query = new \Elastica\Query($terms);
        
        $itemType = $this->container->get("fos_elastica.index.$index.$type");
        
        return $itemType->search($query, $options);
    }
    
    public function searchWithScroll(string $index, string $type, array $terms, array $options = []) : ElasticScrollSearch
    {
        $resultMapFn = $this->makeResultMap($terms);
        
        $options = array_merge([
            'scroll' => '1m',
        ], $options);
        
        $query = new \Elastica\Query($terms);
        $itemType = $this->container->get("fos_elastica.index.$index.$type");
        $results = $itemType->search($query, ['search_type' => 'scan', 'scroll' => $options['scroll']]);
    	
    	$scrollID = $results->getResponse()->getScrollId();
        
        return new ElasticScrollSearch($itemType, $scrollID, $resultMapFn, $options);
    }
    
    public function searchPaginated(string $index, string $type, array $terms)
    {
        $query = new \Elastica\Query($terms);
        
        $finder = $this->container->get("fos_elastica.finder.$index.$type");
        
        return $finder->createPaginatorAdapter($query);
    }
    
    public function search(string $index, string $type, array $terms, array $options = []) : array
    {
        $resultMapFn = $this->makeResultMap($terms);
        
        $results = $this->doSearch($index, $type, $terms, $options);
        
        $arr = array_map($resultMapFn, $results->getResults());
        
        return [
            'total' => $results->getTotalHits(),
            'results' => $arr,
        ];
    }
}
