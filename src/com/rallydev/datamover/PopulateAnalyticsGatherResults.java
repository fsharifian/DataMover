package com.rallydev.datamover;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class PopulateAnalyticsGatherResults {
      private DatabaseConnection dbconnection = new DatabaseConnection();
      private AnalyticsGatherResults results;

    protected AnalyticsGatherResults gatherAnalytics(String schema) throws Exception
      {
        //Connection c = getRenumConn();
        Connection c = dbconnection.getRenumDBConnection();
        DatabaseMetaData meta = c.getMetaData();
        results = new AnalyticsGatherResults(schema);

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
          AnalyticsTableFKInfo tableFKInfo = new AnalyticsTableFKInfo();
          ResultSet rsCols = meta.getColumns(null,schema, tableName, null);
          while (rsCols.next())
          {
            String col = rsCols.getString("COLUMN_NAME");
            String type = rsCols.getString("TYPE_NAME");

            if (col.endsWith("_OID") && type.equals("NUMBER"))
            {
              tableFKInfo.prdFKCols.add(col);
            }
            else if (col.endsWith("_ID") &&
                     type.equals("NUMBER") &&
                     !col.equals("SUBSCRIPTION_ID") &&
                     !(tableName.equals("ARIFACT_TRED_DATA") && col.equals("VALID_FROM_ID")))
            {
              tableFKInfo.analyticsFKCols.add(col);
            }
          }
          rsCols.close();

          if ("CUSTOM_REPORT".equals(tableName))
          {
            tableFKInfo.analyticsFKCols.add("ID");
          }

          if (!tableFKInfo.prdFKCols.isEmpty() || !tableFKInfo.analyticsFKCols.isEmpty())
          {
            tableFKInfo.schema = schema;
            tableFKInfo.name = tableName;
            results.fkTables.add(tableFKInfo);
          }

          // find tables to renumber
          if ("DATABASECHANGELOG".equals(tableName) ||
              "DATABASECHANGELOGLOCK".equals(tableName) ||
              "QUERY_OIDS_TMP".equals(tableName) ||
              "CUSTOM_REPORT".equals(tableName) ||
              "CUSTREP_COLUMN_METADATA".equals(tableName) ||
              "CUSTREP_ROW_METADATA".equals(tableName) ||
              "RALLY_REPORT".equals(tableName))
          {
            System.out.format("***Will Not Renumber*** %s.%s\n", schema,tableName);
            continue;
          }

          TableRenumberInfo tableInfo = new TableRenumberInfo();
          tableInfo.name = tableName;
          tableInfo.schema = schema;
          tableInfo.renumCol = "ID";
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
        return results;
      }

}
