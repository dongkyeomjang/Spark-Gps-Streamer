package com.dongkyeom.spark.streamer;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseWriter {

    private final Connection connection;

    public HBaseWriter() throws Exception {
        org.apache.hadoop.conf.Configuration config = org.apache.hadoop.hbase.HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hbase");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        this.connection = ConnectionFactory.createConnection(config);
    }

    public void writeToHBase(String tripId, String json) throws Exception {
        Table table = connection.getTable(TableName.valueOf("gps_data"));

        Put put = new Put(Bytes.toBytes(tripId));
        put.addColumn(Bytes.toBytes("raw"), Bytes.toBytes("data"), Bytes.toBytes(json));
        table.put(put);

        table.close();
    }

    public void close() throws Exception {
        connection.close();
    }
}

