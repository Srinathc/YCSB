/*
 * Copyright (c) 2010, Apigee Corporation.  All rights reserved.
 * Apigee(TM) and the Apigee logo are trademarks or
 * registered trademarks of Apigee Corp. or its subsidiaries.  All other
 * trademarks are the property of their respective owners.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Cassandra 0.8 client for YCSB framework
 */
public class CassandraClient1 extends DB
{
  static Random random = new Random();
  public static final int Ok = 0;
  public static final int Error = -1;
  public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

  public int ConnectionRetries;
  public int OperationRetries;
  public String column_family;

  public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
  public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
  public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
  public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

  public static final String CONSISTENCY_LEVEL_PROPERTY = "cassandra.consistencylevel";
  public static final String CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  TTransport tr;
  Cassandra.Client client;

  boolean _debug = false;
  
  String _table = "";
  Exception errorexception = null;
  
  List<Mutation> mutations = new ArrayList<Mutation>();
  Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
  Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
  
  ColumnParent parent;
  ConsistencyLevel consistencyLevel;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException
  {
    String hosts = getProperties().getProperty("hosts");
    if (hosts == null)
    {
      throw new DBException("Required property \"hosts\" missing for CassandraClient");
    }

    column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
    parent = new ColumnParent(column_family);

    consistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(CONSISTENCY_LEVEL_PROPERTY, CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

    ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
        CONNECTION_RETRY_PROPERTY_DEFAULT));
    OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
        OPERATION_RETRY_PROPERTY_DEFAULT));

    String username = getProperties().getProperty(USERNAME_PROPERTY);
    String password = getProperties().getProperty(PASSWORD_PROPERTY);

    _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

    String[] allhosts = hosts.split(",");
    String myhost = allhosts[random.nextInt(allhosts.length)];

    Exception connectexception = null;

    for (int retry = 0; retry < ConnectionRetries; retry++)
    {
      tr = new TFramedTransport(new TSocket(myhost, 9160));
      TProtocol proto = new TBinaryProtocol(tr);
      client = new Cassandra.Client(proto);
      try
      {
        tr.open();
        connectexception = null;
        break;
      } catch (Exception e)
      {
        connectexception = e;
      }
      try
      {
        Thread.sleep(1000);
      } catch (InterruptedException e)
      {
      }
    }
    if (connectexception != null)
    {
      System.err.println("Unable to connect to " + myhost + " after " + ConnectionRetries
          + " tries");
      System.out.println("Unable to connect to " + myhost + " after " + ConnectionRetries
          + " tries");
      throw new DBException(connectexception);
    }

    if (username != null && password != null) 
    {
        Map<String,String> cred = new HashMap<String,String>();
        cred.put("username", username);
        cred.put("password", password);
        AuthenticationRequest req = new AuthenticationRequest(cred);
        try 
        {
            client.login(req);
        }
        catch (Exception e)
        {
            throw new DBException(e);
        }
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  public void cleanup() throws DBException
  {
    tr.close();
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public int read(String table, String key, Set<String> fields, HashMap<String, String> result)
  {
    if (!_table.equals(table)) {
      try 
      {
        client.set_keyspace(table);
        _table = table;
      }
      catch (Exception e) 
      {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Error;
      }
    }

    for (int i = 0; i < OperationRetries; i++)
    {

      try
      {
        SlicePredicate predicate;
        if (fields == null)
        {
          predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));
          
        } else {
          ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields)
          {
            fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
          }

          predicate = new SlicePredicate().setColumn_names(fieldlist);
        }

        List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, consistencyLevel);

        if (_debug)
        {
          System.out.print("Reading key: " + key);
        }

        Column column;
        String name;
        String value;
        for (ColumnOrSuperColumn oneresult : results)
        {

          column = oneresult.column;
	        name = new String(column.name.array(), column.name.position()+column.name.arrayOffset(), column.name.remaining());
      	  value = new String(column.value.array(), column.value.position()+column.value.arrayOffset(), column.value.remaining());

          result.put(name,value);

          if (_debug)
          {
            System.out.print("(" + name + "=" + value + ")");
          }
        }

        if (_debug)
        {
          System.out.println();
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }

      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public int scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, String>> result)
  {
    if (!_table.equals(table)) {
      try 
      {
        client.set_keyspace(table);
        _table = table;
      }
      catch (Exception e) 
      {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Error;
      }
    }
    
    for (int i = 0; i < OperationRetries; i++)
    {

      try
      {
        SlicePredicate predicate;
        if (fields == null)
        {
          predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));
          
        } else {
          ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields)
          {
      	    fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
          }
          
          predicate = new SlicePredicate().setColumn_names(fieldlist);
        }
        
        KeyRange kr = new KeyRange().setStart_key(startkey.getBytes("UTF-8")).setEnd_key(new byte[] {}).setCount(recordcount);

        List<KeySlice> results = client.get_range_slices(parent, predicate, kr, consistencyLevel);

        if (_debug)
        {
          System.out.println("Scanning startkey: " + startkey);
        }

        HashMap<String, String> tuple;
        for (KeySlice oneresult : results)
        {
          tuple = new HashMap<String, String>();
          
          Column column;
          String name;
          String value;
          for (ColumnOrSuperColumn onecol : oneresult.columns)
          {
	          column = onecol.column;
      	    name = new String(column.name.array(), column.name.position()+column.name.arrayOffset(), column.name.remaining());
      	    value = new String(column.value.array(), column.value.position()+column.value.arrayOffset(), column.value.remaining());
            
      	    tuple.put(name, value);

            if (_debug)
            {
              System.out.print("(" + name + "=" + value + ")");
            }
          }

          result.add(tuple);
          if (_debug)
          {
            System.out.println();
          }
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int update(String table, String key, HashMap<String, String> values)
  {
    return insert(table, key, values);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int insert(String table, String key, HashMap<String, String> values)
  {
    if (!_table.equals(table)) {
      try 
      {
        client.set_keyspace(table);
        _table = table;
      }
      catch (Exception e) 
      {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Error;
      }
    }
    
    for (int i = 0; i < OperationRetries; i++)
    {
      if (_debug)
      {
        System.out.println("Inserting key: " + key);
      }
      
      try
      { 
        ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));

        Column col;
        ColumnOrSuperColumn column;
        for (Map.Entry<String, String> entry : values.entrySet())
        {
          col = new Column();
          col.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
          col.setValue(ByteBuffer.wrap(entry.getValue().getBytes("UTF-8")));
          col.setTimestamp(System.currentTimeMillis());

          column = new ColumnOrSuperColumn();
          column.setColumn(col);
                                        
          mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }
        
        mutationMap.put(column_family, mutations);
        record.put(wrappedKey, mutationMap);

        client.batch_mutate(record, consistencyLevel);
        
        mutations.clear();
        mutationMap.clear();
        record.clear();

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }

    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
  }

  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public int delete(String table, String key)
  {
    if (!_table.equals(table)) {
      try 
      {
        client.set_keyspace(table);
        _table = table;
      }
      catch (Exception e) 
      {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Error;
      }
    }

    for (int i = 0; i < OperationRetries; i++)
    {
      try
      {
        client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")), 
                      new ColumnPath(column_family), 
                      System.currentTimeMillis(),
                      consistencyLevel);

        if (_debug)
        {
          System.out.println("Delete key: " + key);
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
  }

  public static void main(String[] args) throws Exception {
    CassandraClient1 cli = new CassandraClient1();

    Properties props = new Properties();

    props.setProperty("hosts", args[0]);
    cli.setProperties(props);

    try
    {
      cli.init();
    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }

    HashMap<String, String> vals = new HashMap<String, String>();
    vals.put("age", "57");
    vals.put("middlename", "bradley");
    vals.put("favoritecolor", "blue");
    int res = cli.insert("usertable", "BrianFrankCooper", vals);
    System.out.println("Result of insert: " + res);

    HashMap<String, String> result = new HashMap<String, String>();
    HashSet<String> fields = new HashSet<String>();
    fields.add("middlename");
    fields.add("age");
    fields.add("favoritecolor");
    res = cli.read("usertable", "BrianFrankCooper", null, result);
    System.out.println("Result of read: " + res);
    for (String s : result.keySet())
    {
      System.out.println("[" + s + "]=[" + result.get(s) + "]");
    }

    res = cli.delete("usertable", "BrianFrankCooper");
    System.out.println("Result of delete: " + res);
  }

  /*
   * public static void main(String[] args) throws TException,
   * InvalidRequestException, UnavailableException,
   * UnsupportedEncodingException, NotFoundException {
   * 
   * 
   * 
   * String key_user_id = "1";
   * 
   * 
   * 
   * 
   * client.insert("Keyspace1", key_user_id, new ColumnPath("Standard1", null,
   * "age".getBytes("UTF-8")), "24".getBytes("UTF-8"), timestamp,
   * consistencyLevel);
   * 
   * 
   * // read single column ColumnPath path = new ColumnPath("Standard1", null,
   * "name".getBytes("UTF-8"));
   * 
   * System.out.println(client.get("Keyspace1", key_user_id, path,
   * consistencyLevel));
   * 
   * 
   * // read entire row SlicePredicate predicate = new SlicePredicate(null, new
   * SliceRange(new byte[0], new byte[0], false, 10));
   * 
   * ColumnParent parent = new ColumnParent("Standard1", null);
   * 
   * List<ColumnOrSuperColumn> results = client.get_slice("Keyspace1",
   * key_user_id, parent, predicate, consistencyLevel);
   * 
   * for (ColumnOrSuperColumn result : results) {
   * 
   * Column column = result.column;
   * 
   * System.out.println(new String(column.name, "UTF-8") + " -> " + new
   * String(column.value, "UTF-8"));
   * 
   * }
   * 
   * 
   * 
   * 
   * }
   */
}
