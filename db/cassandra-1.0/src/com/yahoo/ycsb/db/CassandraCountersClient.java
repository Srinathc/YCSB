/*
 * Copyright (c) 2010, Apigee Corporation.  All rights reserved.
 * Apigee(TM) and the Apigee logo are trademarks or
 * registered trademarks of Apigee Corp. or its subsidiaries.  All other
 * trademarks are the property of their respective owners.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DBException;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * insert - create counter and add given value to the counter
 * update - add a given value to the counter
 * read - get counter value
 * scan - N/A
 * delete - remove counter
 */
public class CassandraCountersClient extends CassandraClient1 {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private String counterName;

    private static final long DEFAULT_COUNTER_VALUE = 1;

    @Override
    public void init() throws DBException {
        super.init();
        this.counterName = getProperties().getProperty("cassandra.countername");
    }

    private void queryRing() throws InvalidRequestException, TException {
        List<TokenRange> tokens = client.describe_ring("dbtest");
        for (TokenRange token : tokens) {
            System.out.println(token.start_token);
            System.out.println(token.end_token);
            System.out.println(token.endpoints);
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, String> values) {
        Exception errorException = null;
        try {
            client.set_keyspace(table);
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return Error;
        }
        for (int i = 0; i < OperationRetries; i++) {
            try {
                ColumnParent counterParent = new ColumnParent(column_family);
                long value = parseCounterValue(values.get(counterName));
                CounterColumn column = new CounterColumn(bytes(counterName), value);
                client.add(bytes(key), counterParent, column, ConsistencyLevel.ONE);
                if (_debug) {
                    System.out.print("value : " + value);
                }
                return Ok;
            } catch (Exception e) {
                errorException = e;
            }
        }
        errorException.printStackTrace();
        errorException.printStackTrace(System.out);
        return Error;
    }

    private long parseCounterValue(String value) {
        return value == null ? DEFAULT_COUNTER_VALUE : Long.parseLong(value);
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
        Exception errorException = null;
        try {
            client.set_keyspace(table);
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return Error;
        }
        for (int i = 0; i < OperationRetries; i++) {
            try {
                ColumnPath counterPath = new ColumnPath(column_family);
                    counterPath.column = bytes(counterName);
                ColumnOrSuperColumn column = client.get(bytes(key), counterPath, ConsistencyLevel.ONE);
                String counterValue = String.valueOf(column.counter_column.getValue());
                result.put(counterName, counterValue);
                if (_debug) {
                    System.out.print("value : " + counterValue);
                }
                return Ok;
            } catch (Exception e) {
                errorException = e;
            }
        }
        errorException.printStackTrace();
        errorException.printStackTrace(System.out);
        return Error;
    }

    @Override
    public int scan(String table, String startKey, int recordCount, Set<String> fields, Vector<HashMap<String, String>> result) {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public int update(String table, String key, HashMap<String, String> values) {
        return insert(table, key, values);
    }

    @Override
    public int delete(String table, String key) {
        Exception errorexception = null;
        try {
            client.set_keyspace(table);
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return Error;
        }
        for (int i = 0; i < OperationRetries; i++) {
            try {
                ColumnPath counterPath = new ColumnPath(column_family);
                counterPath.column = bytes(counterName);
                client.remove_counter(bytes(key), counterPath, ConsistencyLevel.ONE);
                if (_debug) {
                    System.out.println("DELETE");
                }
                return Ok;
            } catch (Exception e) {
                errorexception = e;
            }
        }
        errorexception.printStackTrace();
        errorexception.printStackTrace(System.out);
        return Error;
    }

    private static ByteBuffer bytes(String arg) {
        return ByteBuffer.wrap(arg.getBytes(CHARSET));
    }

    public static void main(String[] args) throws Exception {
        CassandraCountersClient client = new CassandraCountersClient();
        Properties properties = new Properties();
        properties.setProperty("cassandra.columnfamily", "counterCF");
        properties.setProperty("cassandra.countername", "c1");
        properties.setProperty("hosts", "localhost");
        client.setProperties(properties);
        client.init();
        
//        HashMap<String, String> rowValues = new HashMap<String, String>();
//        rowValues.put("c1", "1");
//        client.insert("dbtest", "key1", rowValues);

//        HashMap<String, String> results = new HashMap<String, String>();
//        client.read("dbtest", "key1", Collections.singleton("c1"), results);
//        System.out.println(results.get("c1"));
        
//        HashMap<String, String> rowValues = new HashMap<String, String>();
//        rowValues.put("c1", "1");
//        client.update("dbtest", "key1", rowValues);

//        client.delete("dbtest", "key1");
    }
}
