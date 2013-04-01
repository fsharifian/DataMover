package com.rallydev.datamover;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created with IntelliJ IDEA.
 * User: vshivamurthy
 * Date: 2/1/13
 * Time: 2:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class RenumberPrdUnstructuredExportedKeys {

    private DatabaseConnection dbconnection = new DatabaseConnection();

    public String replaceOID(CharSequence string, Pattern oidPattern, int oidGroup, Map<Long, Long> newOids)
      {
        Matcher m = oidPattern.matcher(string);
        StringBuffer sb = new StringBuffer(string.length());
        while (m.find())
        {
          String existingText = m.group(oidGroup);
          System.out.format("Found oid '%s' in substring '%s' in value '%s'\n",existingText,m.group(),string);
          Long oldOid = Long.parseLong(existingText);
          Long newOid = newOids.get(oldOid);

          if (newOid==null)
          {
            String replacement = Matcher.quoteReplacement(m.group());
            m.appendReplacement(sb,replacement);
            System.out.format("oid '%s' in substring '%s' in value '%s' was not found in the hash map\n",existingText,m.group(),string);
          }
          else
          {
            String match = m.group();
            StringBuffer replacement = new StringBuffer();
            replacement.append(match, 0, m.start(oidGroup)-m.start());
            replacement.append(newOid);
            replacement.append(match,m.end(oidGroup)-m.start(),match.length());

            m.appendReplacement(sb, Matcher.quoteReplacement(replacement.toString()));
          }
        }
        System.out.format("original string '%s' was replaced by '%s' \n",string ,sb.toString());
        m.appendTail(sb);
        return sb.toString();
      }

      public void renumberPrdUnstructuredExportedKeys(Map<Long,Long> newPrdOids,String prdSchemaName) throws Exception
      {
        //Connection c = getRenumConn();
        Connection c = dbconnection.getRenumDBConnection();
        c.setAutoCommit(false);

        //Look for OIDs in CLOB_FIELD
        {
          System.out.format("%tc renumber CLOBs\n", new Date());
          Pattern patt = Pattern.compile("(oid=\"|/slm/detail/.*/|/slm/attachment/)(\\d+)([\"/]?)");
          Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
          ResultSet rsRows = s.executeQuery(String.format("select VALUE from %s.CLOB_FIELD for update",prdSchemaName));
          while (rsRows.next())
          {
            Clob clobValue = rsRows.getClob("VALUE");
            String origValue = clobValue.getSubString(1L,(int)clobValue.length());
            String newValue = replaceOID(origValue,patt,2,newPrdOids);
            clobValue.setString(1,newValue);
            rsRows.updateClob("VALUE",clobValue);
            rsRows.updateRow();
          }
          rsRows.close();
          s.close();
          c.commit();
        }

        //Look for OIDs in PREFERENCE
        {
          System.out.format("%tc renumber PREFERENCEs\n", new Date());
          // matches "workspace.12345.blah" and "project.12345.blah" or /app/12345/Kanban/WipSettings and capture 12345 as group 2
          //Pattern patt = Pattern.compile("^(workspace|project)\\.(\\d+)\\..+$");
         // Pattern patt = Pattern.compile("^(workspace|project|/app)[\\./](\\d+)[\\./].+$");
          Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
          ResultSet rsRows = s.executeQuery(String.format("select NAME,VALUE from %s.PREFERENCE for update",prdSchemaName));
          while (rsRows.next())
          {
            String origValue = rsRows.getString("NAME");
           // Pattern patt = Pattern.compile("^(workspace|project|/app)[\\./](\\d+)[\\./].+$");
              Pattern patt = Pattern.compile("^(workspace|project|/app)[\\./](\\d+)[\\./&].+$");
            String newValue = replaceOID(origValue,patt,2,newPrdOids);
            rsRows.updateString("NAME",newValue);
           // if (newValue.startsWith("workspace.") && newValue.endsWith(".mruprojects"))
              if ((newValue.startsWith("workspace.") && newValue.endsWith(".mruprojects")) || (newValue.startsWith("project.") && newValue.endsWith(".testsetchooser")) || newValue.equals("resource.users.meta.workspaceScope"))
              {
              Clob clobValue = rsRows.getClob("VALUE");
              String value = clobValue.getSubString(1L,(int)clobValue.length());
              StringBuffer sb = new StringBuffer();
              for (String oid:value.split(","))
              {
                Long oldOid = Long.decode(oid);
                Long newOid = newPrdOids.get(oldOid);
                if (newOid == null)
                {
                  System.out.format("Can't find FK in PREFERENCE mruproject %d\n",oldOid);
                  newOid = oldOid;
                }

                oid = newOid.toString();
                if (sb.length() > 0)
                  sb.append(",");
                sb.append(oid);
              }
              clobValue.setString(1,sb.toString());
              rsRows.updateClob("VALUE",clobValue);
            }
            rsRows.updateRow();
          }
          rsRows.close();
          s.close();
          c.commit();
        }

        //Look for OIDs in PANEL_PREFERENCEs
        {
          System.out.format("%tc renumber PANEL_PREFERENCEs\n", new Date());
          // matches "project.oid = 12345" and capture 12345 as group 2
         // Pattern patt = Pattern.compile("^([pP]roject.oid = )(\\d+)$");
          Pattern patt = Pattern.compile("(project|Project|Iteration|iteration|WorkProduct|workproduct)\\.(oid|ObjectID|OID)( = )(\\d+)");
          Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
          ResultSet rsRows = s.executeQuery(String.format("select NAME,VALUE from %s.PANEL_PREFERENCE for update",prdSchemaName));
          while (rsRows.next())
          {
            String name = rsRows.getString("NAME");
            Clob clobValue = rsRows.getClob("VALUE");
            String origValue = clobValue.getSubString(1L,(int)clobValue.length());
            String newValue = null;
           // if (name.equals("project"))
            if (name.equals("project") || name.equals("tags") || name.equals("iterationOid") || name.equals("releaseOid"))
            {
              Long newOid = null;
              try
              {
                newOid = newPrdOids.get(Long.parseLong(origValue));
                if (newOid == null)
                  System.out.format("The foreign key %s is an orphan in PANEL_PREFERENCE.VALUE",origValue);
                else
                  newValue = newOid.toString();
              }
              catch (Exception e)
              {
                System.out.format("The foreign key %s is not a valid number in PANEL_PREFERENCE.VALUE",origValue);
              }
            }
            else
            {
              newValue = replaceOID(origValue,patt,4,newPrdOids);
            }
            clobValue.setString(1,newValue);
            rsRows.updateClob("VALUE",clobValue);
            rsRows.updateRow();
          }
          rsRows.close();
          s.close();
          c.commit();
        }

        {
            System.out.format("%tc renumber DASHBOARDs\n", new Date());
            // matches "project.oid = 12345" and capture 12345 as group 2
            Pattern patt = Pattern.compile("^(.*?)(\\d+)$");
            Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            ResultSet rsRows = s.executeQuery(String.format("select NAME from %s.DASHBOARD for update",prdSchemaName));
            while (rsRows.next())
            {
                String origValue = rsRows.getString("NAME");
                String newValue = null;
                newValue = replaceOID(origValue,patt,2,newPrdOids);
                rsRows.updateString("NAME",newValue);
                rsRows.updateRow();
            }
            rsRows.close();
            s.close();
            c.commit();
        }
        c.close();
      }
}
