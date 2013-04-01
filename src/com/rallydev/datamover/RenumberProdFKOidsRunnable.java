package com.rallydev.datamover;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 1:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class RenumberProdFKOidsRunnable implements Runnable {

    private DatabaseConnection dbconnection = new DatabaseConnection();
    private ConcurrentHashMap<Long,Long> newPrdOids;
    private PrdTableFKInfo t;
    private GatherResults info;
    private DataMover dm;

    RenumberProdFKOidsRunnable(GatherResults info,ConcurrentHashMap<Long,Long> newPrdOids, PrdTableFKInfo tableinfo, DataMover dm)
    {
        this.t = tableinfo;
        this.info = info;
        this.dm = dm;
        this.newPrdOids = newPrdOids;
    }

    public void run()
    {
        try
        {
        StringBuffer colClause = new StringBuffer();
              for (String col:t.prdFKCols)
              {
                System.out.format("FK %s.%s.%s\n",t.schema,t.name,col);
                if (colClause.length() != 0)
                  colClause.append(",");
                colClause.append(col);
              }

              Connection c = dbconnection.getRenumDBConnection();
              Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
              String select = String.format("select %s from %s",colClause.toString(),t.fqn());
              System.out.println(select);
              ResultSet rsRows = s.executeQuery(select);
              while (rsRows.next())
              {
                boolean save = false;
                for (String col:t.prdFKCols)
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
                  Long newId = newPrdOids.get(oldId);
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
