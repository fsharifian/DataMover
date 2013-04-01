package com.rallydev.datamover;

import com.sun.jndi.cosnaming.ExceptionMapper;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 11:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class DatabaseConnection {

    private String seqDBJDBCUrl = null;
    private String prdSeqDBUsername = null;
    private String analyticsSeqDBUsername = null;
    private String prdSeqDBPassword = null;
    private String analyticsSeqDBPassword = null;
    private String renumDBJDBCUrl = null;
    private String renumDBUsername = null;
    private String renumDBPassword = null;
    private String prdSchemaName = null;
    private String analyticsSchemaName = null;
    private String renumAnalyticsDBUsername = null;
    private String renumAnalyticsDBPassword = null;
    private Properties dbproperties = new java.util.Properties();
    private DatamoverConfig dbconfig = new DatamoverConfig();

    DatabaseConnection()
    {
          dbproperties = dbconfig.getConfigFile();
          setProperties();
    }

    public Connection getConnection(String url, String username, String password) throws Exception
      {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection conn = null;
        int tries = 5;
        for (int i=1;i<=tries;i++)
        {
          try
          {
            conn = DriverManager.getConnection(url, username, password);
            break;
          }
          catch (SQLException e)
          {
            if (i == tries)
              throw e;
          }
        }
        return conn;
      }


    private void setProperties()
    {
        seqDBJDBCUrl = dbproperties.getProperty("seqDBJDBCUrl");
        prdSeqDBUsername = dbproperties.getProperty("prdSeqDBUsername");
        analyticsSeqDBUsername = dbproperties.getProperty("analyticsSeqDBUsername");
        prdSeqDBPassword =  dbproperties.getProperty("prdSeqDBPassword");
        analyticsSeqDBPassword = dbproperties.getProperty("analyticsSeqDBPassword");
        renumDBJDBCUrl = dbproperties.getProperty("renumDBJDBCUrl");
        renumDBUsername = dbproperties.getProperty("renumDBUsername");
        renumDBPassword = dbproperties.getProperty("renumDBPassword");
        prdSchemaName = dbproperties.getProperty("prdSchemaName");
        analyticsSchemaName = dbproperties.getProperty("analyticsSchemaName");
        renumAnalyticsDBUsername = dbproperties.getProperty("renumAnalyticsDBUsername");
        renumAnalyticsDBPassword = dbproperties.getProperty("renumAnalyticsDBPassword");
    }

    public Connection getSeqDBConnection() throws Exception
    {
        return getConnection(seqDBJDBCUrl, prdSeqDBUsername, prdSeqDBPassword);
    }

    public Connection getAnalyticsSeqDBConnection() throws Exception
    {
        return getConnection(seqDBJDBCUrl, analyticsSeqDBUsername, analyticsSeqDBPassword);
    }

    public Connection getRenumDBConnection() throws Exception
    {
        return getConnection(renumDBJDBCUrl, renumDBUsername, renumDBPassword);
    }

    public Connection getRenumAnalyticsDBConnection() throws Exception
    {
        return getConnection(renumDBJDBCUrl, renumAnalyticsDBUsername, renumAnalyticsDBPassword);
    }

}
