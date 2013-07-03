<?php

namespace Bafford\ElasticaExtraBundle\Provider;

use Symfony\Component\DependencyInjection\ContainerAware;

class ElasticScrollSearch implements \Iterator
{
    protected $itemType = null;
    protected $scrollID = null;
    protected $resultMap = null;
    protected $options = null;
    
    protected $hasStarted = false;
    protected $results = false;
    protected $step = false;
    protected $cnt = 0;
    
    /*
        $options must contain ['scroll' => scroll duration]
    */
    public function __construct($itemType, $scrollID, $resultMap, array $options)
    {
        $this->itemType = $itemType;
        $this->scrollID = $scrollID;
        $this->resultMap = $resultMap;
        $this->options = $options;
    }
    
    protected function getMoreResults()
    {
        $this->hasStarted = true;
        
        $results = $this->itemType->search([], ['scroll' => $this->options['scroll'], 'scroll_id' => $this->scrollID]);
        
        if(!$results)
            $this->results = null;
        else
        {
            $this->results = $results->getResults();
            $this->step = 0;
        }
    }
    
    public function current()
    {
        $rm = $this->resultMap;
        
        return $rm($this->results[$this->step]);
    }
    
    public function key()
    {
        return $this->cnt;
    }
    
    public function next()
    {
        ++$this->cnt;
        ++$this->step;
    }
    
    public function rewind()
    {
        if($this->hasStarted)
            throw new \Exception('Iteration has already started; can\'t restart.');
    }
    
    public function valid()
    {
        if(!$this->results || !isset($this->results[$this->step]))
            $this->getMoreResults();
        
        return ($this->results && isset($this->results[$this->step]));
    }
}