package com.rallydev.datamover;

import java.util.ArrayList;
import java.util.List;
import com.rallydev.datamover.DataMover.*;

class AnalyticsGatherResults extends GatherResults
  {
    public AnalyticsGatherResults(String schema)
    {
      super(schema,"hibernate_sequence");
    }
    public List<AnalyticsTableFKInfo> fkTables = new ArrayList<AnalyticsTableFKInfo>();
  }