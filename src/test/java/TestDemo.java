
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TestDemo {


    public static Connection connection=null;
    public static Admin admin=null;
    static {
        try {
            //1、获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            //configuration.set("hbase.rootdir", "hdfs://119.3.174.87:9000/HBase");
            configuration.set("hbase.zookeeper.quorum","119.3.174.87");
            configuration.set("hbase.zookeeper.property.clientPort","2181");
            //2、创建连接对象
            connection= ConnectionFactory.createConnection(configuration);
            //3、创建Admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //判断表是否存在
    public static boolean isTableExiat(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    public static void close(){
        if (admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        System.out.println("start");
        System.out.println(isTableExiat("student"));
        System.out.println("end");
        //关闭资源
        close();
    }
}