package com.rallydev.datamover;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataMover
{

  long maxBufferSize = 500000L;
  private DatabaseConnection dbconnection = new DatabaseConnection();
  private long remainingIdsNeeded = 0L;
  private ConcurrentHashMap<Long,Long> newPrdOids = new ConcurrentHashMap<Long,Long>();
  private ConcurrentHashMap<Long,Long> newAnalyticsOids = new ConcurrentHashMap<Long,Long>();
  private static final int NTHREDS = 16;
  private DataMover dm = null;
  private RenumberPrdUnstructuredExportedKeys unstructured = new RenumberPrdUnstructuredExportedKeys();
  private PopulatePrdGatherResults populatePrdGatherResults = new PopulatePrdGatherResults();
  private PopulateAnalyticsGatherResults populateAnalyticsGatherResults = new PopulateAnalyticsGatherResults();

  
  public static void main(String[] args) throws Exception 
  {
    System.out.println("Get the required database connection details");
    DatamoverConfig dbconfig = new DatamoverConfig();
    new DataMover().go(dbconfig.getProperty("prdSchemaName"), dbconfig.getProperty("analyticsSchemaName"));
  }

  

  public void go(String prdSchemaName,String analyticsSchemaName) throws Exception
  {
    System.out.println(new Date());
    
    PrdGatherResults prodResults = populatePrdGatherResults.gatherPrd(prdSchemaName);
    System.out.format(" %tc Total PRD Rows =  %d\n",new Date(),prodResults.totalRows);
    renumber(prodResults, analyticsSchemaName, "prod");
    renumberPrdForeignKeys(prodResults,newPrdOids);    
    unstructured.renumberPrdUnstructuredExportedKeys(newPrdOids, prdSchemaName);
    
    AnalyticsGatherResults analyticsResults = populateAnalyticsGatherResults.gatherAnalytics(analyticsSchemaName);
    renumber(analyticsResults,analyticsSchemaName, "analytics");
    renumberAnalyticsForeignKeys(analyticsResults,newAnalyticsOids,newPrdOids);

    System.out.println(new Date());
  }


  public void renumber(GatherResults info,String analyticsSchema, String schema) throws Exception
      {
        remainingIdsNeeded = info.totalRows;
        DataMover dm = this;
        ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);

        for (TableRenumberInfo t:info.tablesToRenumber.values())
        {
            Runnable worker ;
            if(schema.equalsIgnoreCase("prod"))
            {
            worker = new RenumberProdOidsRunnable(info,newPrdOids, t, analyticsSchema, dm, "prod");
            }
            else
            {
            worker = new RenumberProdOidsRunnable(info,newAnalyticsOids, t, analyticsSchema, dm, "analytics");
            }
            executor.execute(worker);

        }

          executor.shutdown();
          while (!executor.isTerminated()) {

               }
           System.out.println("Finished all threads");

      }


  private void renumberPrdForeignKeys(PrdGatherResults results, ConcurrentHashMap<Long,Long> newPrdOids) throws Exception
  {
    DataMover dm = this;
    ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);

    for (PrdTableFKInfo t:results.fkTables)
    {

       Runnable worker = new RenumberProdFKOidsRunnable(results,newPrdOids, t, dm);
       executor.execute(worker);

    }

      executor.shutdown();
      while (!executor.isTerminated()) {

      }
      System.out.println("Finished all threads");
  }

  private void renumberAnalyticsForeignKeys(AnalyticsGatherResults results, ConcurrentHashMap<Long,Long> newAnalyticsOids, ConcurrentHashMap<Long,Long> newPrdOids) throws Exception
  {
    ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
    for (AnalyticsTableFKInfo t:results.fkTables)
    {
                        Runnable worker = new RenumberAnalyticsFKOidsRunnable(results,newPrdOids,newAnalyticsOids, t, dm);
                        executor.execute(worker);
    }
    executor.shutdown();
    while (!executor.isTerminated()) {

                                 }
    System.out.println("Finished all threads");
  }
  

  public long getNewId(GatherResults gatherResults) throws Exception
  {
    Long newId = null;
    remainingIdsNeeded--;
    if(!gatherResults.newIdBuffer.isEmpty())
    {
      newId = gatherResults.newIdBuffer.poll();
    }
    else
    {
      long bufferSize = Math.min(remainingIdsNeeded,maxBufferSize);
      System.out.format("Getting %d new ids\n",bufferSize);
      Connection c = null;
      String sequencerName = gatherResults.sequencerName;
      if (gatherResults instanceof PrdGatherResults)
      {
          c = dbconnection.getSeqDBConnection();
      }
      else
      {
          c = dbconnection.getAnalyticsSeqDBConnection();
      }
      Statement newOIDStatment = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      ResultSet rsNewIds = newOIDStatment.executeQuery(String.format("select %s.nextval from dual connect by level <= %d",sequencerName,bufferSize));

      while (rsNewIds.next())
      {
        gatherResults.newIdBuffer.add(rsNewIds.getLong(1));
      }
      
      rsNewIds.close();
      newOIDStatment.close();
      c.close();

      System.out.println("no new ids returned the buffer size is " + gatherResults.newIdBuffer.size() + " for sequencer  " + sequencerName);

      if (gatherResults.newIdBuffer.size() == 0)
        throw new Exception("no new ids returned the buffer size is " + gatherResults.newIdBuffer.size() + " for sequencer  " + sequencerName);
      
      newId = gatherResults.newIdBuffer.poll();
    }
    return newId;
  }

}
