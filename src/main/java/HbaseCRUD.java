import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HbaseCRUD {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;

    public HbaseCRUD() {
        init();
    }

    /**
     * 建立连接
     */
    public void init(){
        configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","119.3.174.87");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        try{
            connection=ConnectionFactory.createConnection(configuration);
            admin=connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    /**
     * 关闭连接
     */
    public void close(){
        try{
            if(admin!=null)
                admin.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param myTableName 表名
     * @param fields      字段
     * @throws IOException ioexception
     */
    public void createTable(String myTableName,String[]fields)throws IOException{
        TableName tablename = TableName.valueOf(myTableName);
        if(admin.tableExists(tablename)){
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tablename);
        for(String str:fields){
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(str)).build();
            tableDescriptor.setColumnFamily(columnFamilyDescriptor);
        }
        admin.createTable(tableDescriptor.build());
        System.out.println("建表成功");
    }

    /**
     * 插入数据
     *
     * @param tableName 表名
     * @param fields    列族(或列族:列限定符)
     * @param values    值
     * @param row       行
     * @throws IOException ioexception
     */
    public void addRecord(String tableName,String row,String[] fields,String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < fields.length; i++) {
            Put put = new Put(row.getBytes());
            String [] cols = fields[i].split(":");
            if(cols.length == 1)
                put.addColumn(cols[0].getBytes(), "".getBytes(), values[i].getBytes());
            else
                put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
            table.put(put);
        }
        table.close();
    }

    /**
     * 查询数据
     * @param tableName 表名
     * @param column 列族(或列族:列限定符)
     * @throws IOException
     */
    public void scanColumn (String tableName,String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String [] cols = column.split(":");
        if(cols.length==1)
            scan.addFamily(column.getBytes());
        else
            scan.addColumn(Bytes.toBytes(cols[0]), cols[1].getBytes());
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            for(Cell cell:result.rawCells()){
                System.out.print("列族："+new String(CellUtil.cloneFamily(cell)));
                System.out.print(",行键："+new String(CellUtil.cloneRow(cell)));
                System.out.print(",列名："+new String(CellUtil.cloneQualifier(cell)));
                System.out.print(",值："+new String(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        }
        table.close();
    }

    /**
     * 修改数据
     *
     * @param tableName 表名
     * @param column    列族(或列族:列限定符)
     * @param value     值
     * @param row       行
     * @throws IOException ioexception
     */
    public void modifyData(String tableName,String row,String column,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(row.getBytes());
        String [] cols = column.split(":");
        if(cols.length==1)
            put.addColumn(column.getBytes(),"".getBytes() , value.getBytes());
        else
            put.addColumn(cols[0].getBytes(),cols[1].getBytes() , value.getBytes());
        table.put(put);
        table.close();
    }

    /**
     * 删除行
     *
     * @param tableName 表名
     * @param row       行
     * @throws IOException ioexception
     */
    public void deleteRow(String tableName,String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(row.getBytes());
        table.delete(delete);
        table.close();
    }

    /**
     * 获取所有表名
     *
     * @throws IOException ioexception
     */
    public void listTable() throws IOException{
        List<TableDescriptor> tableDescriptorList = admin.listTableDescriptors();
        for(TableDescriptor tableDescriptor:tableDescriptorList){
            System.out.println(tableDescriptor.getTableName().getNameAsString());
        }
    }

    /**
     * 查找表
     *
     * @param tableName 表名
     * @throws IOException ioexception
     */
    public void scanTable(String tableName) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            for(Cell cell:result.rawCells()){
                System.out.print("列族："+new String(CellUtil.cloneFamily(cell)));
                System.out.print(",行键："+new String(CellUtil.cloneRow(cell)));
                System.out.print(",列名："+new String(CellUtil.cloneQualifier(cell)));
                System.out.print(",值："+new String(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        }
        table.close();
    }

    /**
     * 数行
     *
     * @param tableName 表名
     * @throws IOException ioexception
     */
    public void countRow(String tableName) throws IOException{
        int rowCount = 0;
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            rowCount++;
        }
        System.out.println("行数为:"+rowCount);
    }

    /**
     * 截断表
     *
     * @param tableNameStr 表名str
     * @throws IOException ioexception
     */
    public void truncateTable(String tableNameStr) throws IOException{
        TableName tableName = TableName.valueOf(tableNameStr);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("T.No")).build();
        tableDescriptor.setColumnFamily(columnFamilyDescriptor);
        admin.createTable(tableDescriptor.build());
    }

    /**
     * 添加一列
     *
     * @param tableNameStr  表名str
     * @param columnNameStr 列名str
     * @throws IOException ioexception
     */
    public void addColumn(String tableNameStr,String columnNameStr) throws IOException{
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            for(Cell cell:result.rawCells()){
                if(columnNameStr.equals(new String(CellUtil.cloneRow(cell)))){
                    System.out.println("已存在该列族");
                    return;
                }
            }
        }
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                .newBuilder(columnNameStr.getBytes()).build();
        admin.addColumnFamily(tableName,columnFamilyDescriptor);
        System.out.println("添加列族:"+columnNameStr+"到表:"+tableNameStr+"中");
        scanAllColumn(tableNameStr);
    }

    /**
     * 删除列
     *
     * @param tableNameStr  表名str
     * @param columnNameStr 列名str
     * @throws IOException ioexception
     */
    public void deleteColumn(String tableNameStr,String columnNameStr) throws IOException{
        TableName tableName = TableName.valueOf(tableNameStr);
        admin.disableTable(tableName);
        admin.deleteColumnFamily(tableName,columnNameStr.getBytes());
        admin.enableTable(tableName);
        System.out.println("将列族:"+columnNameStr+"从表:"+tableNameStr+"中删除");
        scanAllColumn(tableNameStr);
    }

    /**
     * 扫描列
     *
     * @param tableName 表名
     * @throws IOException ioexception
     */
    public void scanAllColumn(String tableName) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        TableDescriptor tableDescriptor = table.getDescriptor();
        ColumnFamilyDescriptor[] columnFamilyDescriptorList = tableDescriptor.getColumnFamilies();
        ResultScanner scanner = table.getScanner(scan);
        Set<String> columnSet = new HashSet<>();
        for(int i=0;i<columnFamilyDescriptorList.length;i++){
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorList[i];
            columnSet.add(columnFamilyDescriptor.getNameAsString());
        }
        table.close();
        System.out.println("表"+tableName+"中的列族有："+columnSet);
    }
}