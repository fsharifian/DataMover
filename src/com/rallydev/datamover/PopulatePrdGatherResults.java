package com.rallydev.datamover;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 3:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class PopulatePrdGatherResults {

    private DatabaseConnection dbconnection = new DatabaseConnection();
    private PrdGatherResults results;

    public PrdGatherResults gatherPrd(String schema) throws Exception
     {
       Connection c = dbconnection.getRenumDBConnection();
       DatabaseMetaData meta = c.getMetaData();
       results = new PrdGatherResults(schema);

       // get renumbering info
       System.out.format("Gathering metadata for schema %s\n",schema);
       ResultSet rsTables = null;
       rsTables = meta.getTables(null, schema, "%", new String[]{"TABLE"});
       while (rsTables.next())
       {
         String tableName = rsTables.getString("TABLE_NAME");
         System.out.format("Evaluating table %s.%s\n",schema,tableName);

         if ("TIME_DIMENSION".equals(tableName))
         {
           System.out.format("Ignoring table %s.%s\n",schema,tableName);
           continue;
         }

         // get tables with FK columns
         PrdTableFKInfo tableFKInfo = new PrdTableFKInfo();
         ResultSet rsCols = meta.getColumns(null,schema, tableName, null);
         while (rsCols.next())
         {
           String col = rsCols.getString("COLUMN_NAME");
           String type = rsCols.getString("TYPE_NAME");

           if (col.endsWith("_OID") && type.equals("NUMBER"))
             tableFKInfo.prdFKCols.add(col);
         }
         rsCols.close();

         if ("SCM_CHANGE_SET".equals(tableName))
         {
           tableFKInfo.prdFKCols.add("AUTHOR");
         }
         else if ("METRICS_COLLECTIONS".equals(tableName))
         {
           tableFKInfo.prdFKCols.add("RELATED_TO");
         }
         else if ("TIME_ENTRY_VALUE".equals(tableName))
         {
           tableFKInfo.prdFKCols.add("TIME_ID");
         }

         if (!tableFKInfo.prdFKCols.isEmpty())
         {
           tableFKInfo.schema = schema;
           tableFKInfo.name = tableName;
           results.fkTables.add(tableFKInfo);
         }

         // hard-coded tables to renumber
         if (tableName.equals("PANEL_PREFERENCE"))
         {
           TableRenumberInfo tableInfo = new TableRenumberInfo();
           tableInfo.name = tableName;
           tableInfo.schema = schema;
           tableInfo.renumCol = "OID";
           tableInfo.hasSubscriptionId = true;
           Statement s = c.createStatement();
           ResultSet r = s.executeQuery("SELECT COUNT(*) AS rowcount FROM "+ tableInfo.fqn());
           r.next();
           tableInfo.size = r.getInt("rowcount") ;
           r.close() ;
           s.close();

           results.tablesToRenumber.put(tableInfo.fqn(),tableInfo);
           results.totalRows += tableInfo.size;
           continue;
         }

         // find tables to renumber
         ResultSet rsPK = meta.getPrimaryKeys(null, schema, tableName);
         int numPk = 0;
         String pkCol = null;
         while (rsPK.next())
         {
           pkCol = rsPK.getString("COLUMN_NAME");
           numPk++;
         }
         rsPK.close();

         if (numPk != 1 ||
             //FIX - this is too convoluted
             (!"OID".equals(pkCol) && !"ID".equals(pkCol) && !"SUBSCRIPTION".equals(tableName)) ||
             "SUBSCRIPTION_TYPE".equals(tableName))
         {
           System.out.format("***Will Not Renumber*** %s.%s PKs(%d)\n", schema,tableName,numPk);
           continue;
         }

         TableRenumberInfo tableInfo = new TableRenumberInfo();

         // Does this table have a SUBSCRIPTION_ID column
         ResultSet rsSubscriptionColumn = meta.getColumns(null, schema, tableName, "SUBSCRIPTION_ID");
         if (rsSubscriptionColumn.next())
           tableInfo.hasSubscriptionId = true;
         rsSubscriptionColumn.close();

         tableInfo.name = tableName;
         tableInfo.schema = schema;
         tableInfo.renumCol = "SUBSCRIPTION".equals(tableName)?"OID":pkCol;
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("SELECT COUNT(*) AS rowcount FROM "+ tableInfo.fqn());
         r.next();
         tableInfo.size = r.getInt("rowcount") ;
         r.close() ;
         s.close();

         results.tablesToRenumber.put(tableInfo.fqn(),tableInfo);
         results.totalRows += tableInfo.size;
       }
       rsTables.close();
       c.close();
       return results;
     }
}
