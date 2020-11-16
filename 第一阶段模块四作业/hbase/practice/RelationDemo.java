package hbase.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;

public class RelationDemo {
     static Configuration conf = null;
     static Connection connection = null;
     static HBaseAdmin admin = null;
     //创建表
    public static void createTable() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01,linux02");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        connection = ConnectionFactory.createConnection(conf);
        admin = (HBaseAdmin) connection.getAdmin();
        TableName tableName = TableName.valueOf("user");
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        //指定列族
        descriptor.addFamily(new HColumnDescriptor("friends"));
        admin.createTable(descriptor);
        System.out.println("user表创建成功");
        admin.close();
    }
    //插入数据
    public static void initRelationData() throws IOException {
        final Table relation = connection.getTable(TableName.valueOf("user"));
        final Put uid1 = new Put(Bytes.toBytes("uid1"));
        uid1.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid2"), Bytes.toBytes("uid2"));
        final Put uid2 = new Put(Bytes.toBytes("uid2"));
        uid2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid1"), Bytes.toBytes("uid1"));
        final ArrayList<Put> puts = new ArrayList<>();
        puts.add(uid1);
        puts.add(uid2);
        relation.put(puts);
    }
    //删除好友
    public static void deleteData(String uid, String friend) throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux01,linux02");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        connection = ConnectionFactory.createConnection(conf);
        final Table relation = connection.getTable(TableName.valueOf("user"));
        Delete delete = new Delete(Bytes.toBytes(uid));
        delete.addColumn(Bytes.toBytes("friends"), Bytes.toBytes(friend));
        relation.delete(delete);
    }
    public static void main(String[] args) throws IOException {
      createTable();
      initRelationData();
      deleteData("uid1","uid2");
    }
}
