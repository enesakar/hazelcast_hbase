package com.hazelcast.hbase;

import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;


public class HBaseService {
    private static HBaseService ourInstance = new HBaseService();
    private HTable htable;


    public static HBaseService getInstance() {
        return ourInstance;
    }

    public HTable getHtable() {
        return htable;
    }

    public void setHtable(HTable htable) {
        this.htable = htable;
    }

    private HBaseService() {
        try {
            htable = new HTable("user");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
