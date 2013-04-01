package com.rallydev.datamover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.rallydev.datamover.DataMover.*;


abstract class GatherResults
  {
    public GatherResults(String schema,String sequencerName)
    {
      this.schema = schema;
      this.sequencerName = sequencerName;
    }
    public Map<String,TableRenumberInfo> tablesToRenumber = new HashMap<String,TableRenumberInfo>();
    public int totalRows = 0;
    public String schema;
    public ConcurrentLinkedQueue<Long> newIdBuffer = new ConcurrentLinkedQueue<Long>();
    public String sequencerName;
  }