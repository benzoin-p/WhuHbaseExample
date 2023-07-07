import java.io.IOException;

public class Task {

    public void firstTask(HbaseCRUD hbaseCRUD) throws IOException {
        System.out.println("正在初始化第一题所需要的表中数据");
        String[] cols=new String[]{"T_No","T_Name","T_Sex","T_Age"};
        hbaseCRUD.createTable("teacher",cols);
        hbaseCRUD.addRecord("teacher","2023001",cols,new String[]{"2023001","Zhang","male","43"});
        hbaseCRUD.addRecord("teacher","2023002",cols,new String[]{"2023002","Ma","female","42"});
        hbaseCRUD.addRecord("teacher","2023003",cols,new String[]{"2023003","Li","male","44"});
        System.out.println("（1）列出HBase所有的表的相关信息，例如表名");
        hbaseCRUD.listTable();
        System.out.println("（2）在终端打印出指定的表的所有记录数据");
        hbaseCRUD.scanTable("teacher");
        System.out.println("（3）向已经创建好的表添加和删除指定的列族或列");
        hbaseCRUD.addColumn("teacher","T_Salary");
        hbaseCRUD.deleteColumn("teacher","T_Salary");
        System.out.println("（4）清空指定的表的所有记录数据");
        hbaseCRUD.truncateTable("teacher");
        System.out.println("（5）统计表的行数");
        hbaseCRUD.countRow("student");
    }

    public void secondTask(HbaseCRUD hbaseCRUD) throws IOException{
        String[] studentCols=new String[]{"S_No","S_Name","S_Sex","S_Age"};
        String[] courseCols=new String[]{"C_No","C_Name","C_Credits"};
        String[] scCols=new String[]{"SC_Sno","SC_Cno","SC_Score"};
        String[] scCols1=new String[]{"SC_Sno","SC_Cno","SC_Score:usual"};
        String[] scCols2=new String[]{"SC_Sno","SC_Cno","SC_Score:final"};

        System.out.println("（1）createTable(String tableName, String[] fields)");
        hbaseCRUD.createTable("student",studentCols);
        hbaseCRUD.createTable("course",courseCols);
        hbaseCRUD.createTable("sc",scCols);

        System.out.println("（2）addRecord(String tableName, String row, String[] fields, String[] values)");
        hbaseCRUD.addRecord("student","202301",studentCols,new String[]{"202301","Zhao","male","16"});
        hbaseCRUD.addRecord("student","202302",studentCols,new String[]{"202302","Qian","male","17"});
        hbaseCRUD.addRecord("student","202303",studentCols,new String[]{"202303","Sun","female","16"});
        hbaseCRUD.addRecord("course","65535",courseCols,new String[]{"65535","Algebra","3.0"});
        hbaseCRUD.addRecord("course","65536",courseCols,new String[]{"65536","Probability","2.0"});
        hbaseCRUD.addRecord("course","65537",courseCols,new String[]{"65537","Complex","1.0"});
        hbaseCRUD.addRecord("sc","000001",scCols1,new String[]{"202301","65535","94"});
        hbaseCRUD.addRecord("sc","000002",scCols1,new String[]{"202301","65536","87"});
        hbaseCRUD.addRecord("sc","000003",scCols1,new String[]{"202302","65535","77"});
        hbaseCRUD.addRecord("sc","000004",scCols1,new String[]{"202302","65537","91"});
        hbaseCRUD.addRecord("sc","000005",scCols1,new String[]{"202303","65536","43"});
        hbaseCRUD.addRecord("sc","000006",scCols1,new String[]{"202303","65537","64"});
        hbaseCRUD.addRecord("sc","000001",scCols2,new String[]{"202301","65535","92"});
        hbaseCRUD.addRecord("sc","000002",scCols2,new String[]{"202301","65536","95"});
        hbaseCRUD.addRecord("sc","000003",scCols2,new String[]{"202302","65535","66"});
        hbaseCRUD.addRecord("sc","000004",scCols2,new String[]{"202302","65537","87"});
        hbaseCRUD.addRecord("sc","000005",scCols2,new String[]{"202303","65536","20"});
        hbaseCRUD.addRecord("sc","000006",scCols2,new String[]{"202303","65537","72"});

        System.out.println("（3）scanColumn(String tableName, String column)");
        hbaseCRUD.scanColumn("course","C_Name");
        hbaseCRUD.scanColumn("sc","SC_Score:usual");

        System.out.println("（4）modifyData(String tableName, String row, String column)");
        hbaseCRUD.modifyData("course","65537","C_Name","ComplexVariablesFunctions");
        hbaseCRUD.scanColumn("course","C_Name");

        System.out.println("（5）deleteRow(String tableName, String row)");
        hbaseCRUD.deleteRow("sc","000006");
        hbaseCRUD.scanColumn("sc","SC_Score");
    }


}
