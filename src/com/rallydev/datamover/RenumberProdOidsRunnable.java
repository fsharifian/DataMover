package com.rallydev.datamover;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 12:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class RenumberProdOidsRunnable implements Runnable {
    
    private DatabaseConnection dbconnection = new DatabaseConnection();
    private ConcurrentHashMap<Long,Long> newPrdOids;
    private TableRenumberInfo t;
    private String analyticsSchema;
    private GatherResults info;
    private DataMover dm;
    private String schema;

    RenumberProdOidsRunnable(GatherResults info,ConcurrentHashMap<Long,Long> newPrdOids, TableRenumberInfo tableinfo, String analyticsSchema, DataMover dm, String schema)
    {
           this.newPrdOids = newPrdOids;
           this.t = tableinfo;
           this.info = info;
           this.analyticsSchema = analyticsSchema;
           this.dm = dm;
           this.schema = schema;
    }

    public void run()
    {
        Connection c = null;
        try
        {
            if(schema.equalsIgnoreCase("prod"))
            {
            c = dbconnection.getRenumDBConnection();
            }
            else
            {
            c = dbconnection.getRenumAnalyticsDBConnection();
            }
        }
        catch(Exception e)
        {
            System.out.println("Could not establish database connection ");
            System.out.println(e.fillInStackTrace());
            return;
        }

        System.out.format("%tc %s.%s(%s)(%d)\n", new Date(),t.schema,t.name,t.renumCol,t.size);

        try
        {
        if (t.schema.equals(analyticsSchema) && t.name.equals("REPORT"))
            {
                    Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rsRows = s.executeQuery(String.format("select %s,SUBSCRIPTION_ID,WORKSPACE_OID from %s",t.renumCol,t.fqn()));
                    while (rsRows.next())
                    {
                      long oldId = rsRows.getLong(t.renumCol);
                      long subscriptionId = rsRows.getLong("SUBSCRIPTION_ID");
                      long workspaceOid = rsRows.getLong("WORKSPACE_OID");

                      if (oldId < 0)
                      {
                        System.out.format("negative PK %s.%s.%s not renumbered\n",t.schema,t.name,t.renumCol);
                        continue;
                      }
                      else if (subscriptionId < 0 || subscriptionId == 1)
                      {
                        System.out.format("SUBSCRIPTION_ID negative %s.%s.%s not renumbered\n",t.schema,t.name,t.renumCol);
                        continue;
                      }
                      else if (workspaceOid < 0)
                      {
                        System.out.format("WORKSPACE_OID negative %s.%s.%s not renumbered\n",t.schema,t.name,t.renumCol);
                        continue;
                      }

                      Statement s2 = c.createStatement();
                      ResultSet rs2 = s2.executeQuery(String.format("select ID from %s.RALLY_REPORT where ID = %d",analyticsSchema,oldId));
                      if (rs2.next())
                      {
                        System.out.format("negative PK %s.%s.%s not renumbered\n",t.schema,t.name,t.renumCol);
                        continue;
                      }
                      rs2.close();
                      s2.close();

                        long newId;
                        if(newPrdOids.containsKey(oldId))
                        {
                            newId = newPrdOids.get(oldId);
                        }
                        else
                        {
                            newId = dm.getNewId(info);
                        }
                      newPrdOids.put(oldId,newId);
                      rsRows.updateLong(t.renumCol,newId);
                      rsRows.updateRow();

                      System.out.format("%s.%s.%s.%d:%d\n",t.schema,t.name,t.renumCol,oldId,newId);
                    }
                    rsRows.close();
                    s.close();
                  }
                  else if (t.hasSubscriptionId)
                  {
                    Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rsRows = s.executeQuery(String.format("select %s,SUBSCRIPTION_ID from %s",t.renumCol,t.fqn()));
                    while (rsRows.next())
                    {
                      long oldId = rsRows.getLong(t.renumCol);
                      long subscriptionId = rsRows.getLong("SUBSCRIPTION_ID");
                      if (oldId < 0)
                      {
                        System.out.format("negative PK %s.%s.%s not renumbered\n",t.schema,t.name,t.renumCol);
                        continue;
                      }
                      else if (subscriptionId < 0 || subscriptionId == 1)
                      {
                        System.out.format("negative SUBSCRIPTION_ID %s.%s.%s not renumbered\n",t.schema,t.name,t.renumCol);
                        continue;
                      }
                      long newId;
                      if(newPrdOids.containsKey(oldId))
                      {
                          newId = newPrdOids.get(oldId);
                      }
                      else
                      {
                      newId = dm.getNewId(info);
                      }
                      newPrdOids.put(oldId,newId);
                      rsRows.updateLong(t.renumCol,newId);
                      rsRows.updateRow();

                      System.out.format("%s.%s.%s.%d:%d\n",t.schema,t.name,t.renumCol,oldId,newId);
                    }
                    rsRows.close();
                    s.close();
                  }
                  else
                  {
                    Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rsRows = s.executeQuery(String.format("select %s from %s",t.renumCol,t.fqn()));
                    while (rsRows.next())
                    {
                      long oldId = rsRows.getLong(t.renumCol);
                      if (oldId < 0)
                      {
                        System.out.format("negative PK %s.%s.%s not renumbered\n",t.schema,t.name,t.renumCol);
                        continue;
                      }
                        long newId;
                        if(newPrdOids.containsKey(oldId))
                        {
                            newId = newPrdOids.get(oldId);
                        }
                        else
                        {
                            newId = dm.getNewId(info);
                        }
                      newPrdOids.put(oldId,newId);
                      rsRows.updateLong(t.renumCol,newId);
                      rsRows.updateRow();

                      System.out.format("%s.%s.%s.%d:%d\n",t.schema,t.name,t.renumCol,oldId,newId);
                    }
                    rsRows.close();
                    s.close();
                  }
                  c.close();
        }
        catch (Exception e)
        {
            System.out.println("Catch all other exceptions");
            System.out.println(e.fillInStackTrace());
        }
    }

}
