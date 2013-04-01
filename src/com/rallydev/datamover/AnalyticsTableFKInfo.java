package com.rallydev.datamover;

import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class AnalyticsTableFKInfo extends PrdTableFKInfo
{
  public Set<String> analyticsFKCols = new HashSet<String>();
}