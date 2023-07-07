## 武汉大学《大数据分析与处理》 熟悉HBase的基础操作

---
该程序为武汉大学计算机专业选修课程《大数据分析与处理》的课后作业1，希望可以帮助到有需要的同学，具体作业题目如下：

1. 编程实现以下指定功能，并用HBase Shell命令完成同样的任务：
   - (1) 列出HBase所有表的相关信息，例如表名
   - (2) 在终端打印出指定的表的所有记录数据
   - (3) 向已经创建好的表添加和删除指定的列族或列
   - (4) 清空指定的表的所有记录数据
   - (5) 统计表的行数


2. 现有以下关系型数据库中的表和数据,要求将其转换为适合于HBase存储的表并插入数据。请按照样例数据个性化的插入3条学生记录，3门课程信息,6条选课信息。

学生表(Student)

| S_No    | S_Name   | S_Sex  | S_Age |
|---------|----------|--------|-------|
| 2021001 | Zhangsan | male   | 23    |
| 2021002 | Mary     | female | 22    |
| 2021003 | Lisi     | male   | 24    |

课程表(Course)

| C_No    | C_Name           | C_Credit |
|---------|------------------|----------|
| 123001  | Math             | 2.0      |
| 123002  | Computer Science | 5.0      |
| 123003  | English          | 3.0      |

选课表(SC)

| SC_Sno  | SC_Cno | SC_Score |
|---------|--------|----------|
| 2021001 | 123001 | 86       |
| 2021001 | 123003 | 69       |
| 2021002 | 123002 | 77       |
| 2021002 | 123003 | 99       |
| 2021003 | 123001 | 98       |
| 2021003 | 123002 | 95       |

同时，请编程完成以下指定功能：
* （1）createTable(String tableName, String[] fields)
创建表，参数tableName为表的名称，字符串数组fields为存储记录各个域名称的数组。要求当HBase已经存在名为tableName的表的时候，先删除原有的表，然后再创建新的表。
* （2）adRecord(String tableName, String row, String[] fields, String[] values)
向表tableName、行row（用S_Name表示）和字符串数组files指定的单元格中添加对应的数据values。其中fields中每个元素如果对应的列族下还有相应的列限定符的话，用“columnFamily:column”表示。例如，同时向“Math”、“Computer Science”、“English”三列添加成绩时，字符串数组fields为{“Score:Math”,”Score；Computer Science”,”Score:English”}，数组values存储这三门课的成绩。
* （3）scanColumn(String tableName, String column)
浏览表tableName某一列的数据，如果某一行记录中该列数据不存在，则返回null。要求当参数column为某一列族名称时，如果底下有若干个列限定符，则要列出每个列限定符代表的列的数据；当参数column为某一列具体名称（例如“Score:Math”）时，只需要列出该列的数据。
* （4）modifyData(String tableName, String row, String column)
修改表tableName，行row（可以用学生姓名S_Name表示），列column指定的单元格的数据。
* （5）deleteRow(String tableName, String row)
删除表tableName中row指定的行的记录。

---

#### 项目语言为java，注意存在教师指定使用python作为编程语言的情况。

### 由于作业要求，表中数据存在因时变化的可能，故不一定与上表一致，若有使用需要可修改Task.java中的代码

#### 第一次写md文档，可能存在格式上不合理的地方，不过看得懂就好！


