package com.rallydev.datamover;

import java.util.ArrayList;
import java.util.List;
import com.rallydev.datamover.DataMover.*;


class PrdGatherResults extends GatherResults
  {
    public PrdGatherResults(String schema)
    {
      super(schema,"oid_seq");
    }
    public List<PrdTableFKInfo> fkTables = new ArrayList<PrdTableFKInfo>();


  }