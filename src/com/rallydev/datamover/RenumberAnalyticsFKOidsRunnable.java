package com.rallydev.datamover;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 1:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class RenumberAnalyticsFKOidsRunnable implements Runnable{

    private DatabaseConnection dbconnection = new DatabaseConnection();
    private ConcurrentHashMap<Long,Long> newPrdOids;
    private ConcurrentHashMap<Long,Long> newAnalyticsOids;
    private AnalyticsTableFKInfo t;
    private GatherResults info;
    private DataMover dm;

    RenumberAnalyticsFKOidsRunnable(GatherResults info,ConcurrentHashMap<Long,Long> newPrdOids, ConcurrentHashMap<Long,Long> newAnalyticsOids, AnalyticsTableFKInfo tableinfo, DataMover dm)
    {
        this.t = tableinfo;
        this.info = info;
        this.dm = dm;
        this.newAnalyticsOids = newAnalyticsOids;
        this.newPrdOids = newPrdOids;
    }

    public void run()
    {
        try
        {
            StringBuffer colClause = new StringBuffer();
                  Set<String> allFKCols = new HashSet<String>();
                  allFKCols.addAll(t.prdFKCols);
                  allFKCols.addAll(t.analyticsFKCols);
                  for (String col:allFKCols)
                  {
                    System.out.format("FK %s.%s.%s\n",t.schema,t.name,col);
                    if (colClause.length() != 0)
                      colClause.append(",");
                    colClause.append(col);
                  }
                  //Connection c = getRenumConn();
                  Connection c = dbconnection.getRenumAnalyticsDBConnection();
                  Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                  String select = String.format("select %s from %s",colClause.toString(),t.fqn());
                  System.out.println(select);
                  ResultSet rsRows = s.executeQuery(select);
                  while (rsRows.next())
                  {
                    boolean save = false;
                    for (String col:allFKCols)
                    {
                      long oldId = rsRows.getLong(col);
                      if (oldId < 0)
                      {
                        System.out.format("negative FK %s.%s.%s not renumbered\n",t.schema,t.name,col);
                        continue;
                      }
                      else if (oldId == 0)
                      {
                        System.out.format("null FK %s.%s.%s not renumbered\n",t.schema,t.name,col);
                        continue;
                      }

                      Long newId = null;
                      if (t.prdFKCols.contains(col))
                        newId = newPrdOids.get(oldId);
                      else
                        newId = newAnalyticsOids.get(oldId);

                      if (newId == null)
                      {
                        System.out.format("Can't find FK %s.%s.%s %d\n",t.schema,t.name,col,oldId);
                        continue;
                      }
                      rsRows.updateLong(col,newId);
                      save = true;
                    }
                    if (save)
                      rsRows.updateRow();
                  }
                  rsRows.close();
                  s.close();
                  c.close();
        }
        catch(Exception e)
        {
            System.out.println("Catch all other exceptions");
            System.out.println(e.fillInStackTrace());
        }
    }
}
