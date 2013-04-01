package com.rallydev.datamover;

/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 10:34 AM
 * To change this template use File | Settings | File Templates.
 */
abstract public class TableInfo
{
  public String name;
  public String schema;
  public String fqn()
  {
    return schema+"."+name;
  }
}
