package com.rallydev.datamover;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;


/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/2/13
 * Time: 3:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class DatamoverConfig{

    private Properties dbproperties = new Properties();

    DatamoverConfig()
    {
        loadconfigfile();
    }

    private void loadconfigfile()
       {
           try
           {
               File config = new File("datamover.cfg");
               FileInputStream fs = new FileInputStream(config);
               dbproperties.load(fs);

           }
           catch(Exception e)
           {
               System.out.println(e.fillInStackTrace());
           }
       }

    public Properties getConfigFile()
    {
        return dbproperties;
    }

    public String getProperty(String prop)
    {
          return dbproperties.getProperty(prop);
    }

}
