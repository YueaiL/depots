using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Windows.Forms;
using depots.src.zyz;
using depots.src.zyz.pojo;
using depots.zyz;
using depots.zyz.utils;

namespace depots
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        #region 测试连接
        private void button1_Click(object sender, EventArgs e)
        {
            logBox.AppendText("测试连接中\n");
            try
            {
                SqlUtils.Executor().joinConnStr(url.Text, name.Text, username.Text, password.Text).opening().close();
                if (!(string.IsNullOrEmpty(newName.Text) && string.IsNullOrEmpty(newUrl.Text) && string.IsNullOrEmpty(newUsername.Text)))
                {
                    //为空则无需测试分库
                    SqlUtils.Executor().joinConnStr(newUrl.Text, newName.Text, newUsername.Text, newPassword.Text).opening().close();
                }
                MessageBox.Show(@"测试连接通过");
                logBox.AppendText("测试连接通过\n");
            }
            catch (Exception e1)
            {

                MessageBox.Show(e1.Message);
                logBox.AppendText(e1.Message+ "\n");
            }
        }
        #endregion

        #region 执行分表
        private void doing_Click(object sender, EventArgs e)
        {
            logBox.AppendText("连接数据库中....\n");
            SqlConnection newConn = null;
            SqlConnection conn = null;
            try
            {
                conn = SqlUtils.Executor().joinConnStr(url.Text, name.Text, username.Text, password.Text).opening().GetSqlConnection();
                if (!(string.IsNullOrEmpty(newName.Text) && string.IsNullOrEmpty(newUrl.Text) && string.IsNullOrEmpty(newUsername.Text)))
                {
                    //为空则无需测试分库
                    newConn = SqlUtils.Executor().joinConnStr(newUrl.Text, newName.Text, newUsername.Text, newPassword.Text).opening().GetSqlConnection();
                }
                else
                {
                    newConn = conn;
                }
                if (string.IsNullOrEmpty(mainName.Text) || string.IsNullOrEmpty(mainValue.Text))
                {
                    logBox.AppendText("执行失败,主表名称和主表切分字段必填\n");
                    return;
                }
                if (string.IsNullOrEmpty(subName.Text))
                {
                    subName.Text = mainName.Text;
                }
                logBox.AppendText("查询表结构相关数据中....\n");
                string findTableName = "select format(" + mainValue.Text + ",'yyyyMM') as tableName  from " + mainName.Text + " with(nolock)  GROUP BY format(" + mainValue.Text + ",'yyyyMM')";
                ArrayList arrayList = SqlUtils.Executor().query(conn, findTableName);
                ArrayList tableCloums = SqlUtils.Executor().queryTable(conn, mainName.Text);
                StringBuilder stringBuilder = new StringBuilder("CREATE TABLE[dbo].[?](\n");
                List<String> list = null;
                if(!string.IsNullOrEmpty(delAttribute.Text))
                {
                    list = new List<string>(delAttribute.Text.Split(','));
                }
                for (int i = 0; i < tableCloums.Count; i++) {
                    TableStruct v = (TableStruct)tableCloums[i];
                    if (list != null &&  list.Contains(v.name))
                    {
                        continue;
                    }
                    
                    if (v.type == "varchar")
                    {
                        v.type = v.type + "(" + v.len + ")";
                    }else if (v.type == "numeric")
                    {
                        v.type = v.type + "(" + v.len + "," + v.lessLen + ")";
                    }
                    stringBuilder.Append("\n").Append(v.name).Append("\t")
                        .Append(v.type).Append("\t").Append(v.main == 1 ? "IDENTITY(1,1) PRIMARY KEY" : "").Append("\t")
                        .Append(v.isnull == 1 ? "null" : "not null");
                    if (i != tableCloums.Count -1)
                    {
                        stringBuilder.Append(",");
                    }
                }
                stringBuilder.Append(")");
                for (int i = 0; i < arrayList.Count; i++)
                {
                    /*SqlUtils.Executor().create(newConn);*/
                    string str = stringBuilder.ToString();
                    str = str.Replace("?", subName.Text+arrayList[i].ToString());
                    SqlUtils.Executor().execute(newConn, str);
                }
                logBox.AppendText("生成分表完成....\n");
            }
            catch (Exception e1)
            {
                MessageBox.Show(e1.Message);
                logBox.AppendText(e1.Message+ "\n");
            }
            finally
            {
                SqlUtils.Executor().closeConn(conn);
                SqlUtils.Executor().closeConn(newConn);
            }


        }
        #endregion

        static DataTable GetTableSchema(ArrayList arrayList, List<string> list)
        {
            DataTable dt = new DataTable();
            foreach (TableStruct d in arrayList)
            {
                Type t = null;
                if (d.type.Equals("varchar"))
                {
                    t = typeof(string);
                }else if (d.type.Equals("int"))
                {
                    t = typeof(int);
                }else if (d.type.Equals("numeric"))
                {
                    t = typeof(decimal);
                }else if (d.type.Equals("datetime"))
                {
                    t = typeof(DateTime);
                }
                dt.Columns.Add(new DataColumn(d.name,t));
            }
            return dt;
        }

        public static void RunAsync(Action action)
        {
            ((Action)(delegate ()
            {
                action.Invoke();
            })).BeginInvoke(null, null);
        }

        public void RunInMainThread(Action action)
        {
            this.BeginInvoke((Action)(delegate ()
            {
                action.Invoke();
            }));

        }

        #region 插入分表数据
        private void insertTables_Click(object sender, EventArgs e)
        {
            //Func<string> wait = () =>
            //{
            //    return "abc";
            //};
            
            RunAsync(() =>
            {
                RunInMainThread(() =>
                {
                    logBox.AppendText("连接数据库中....\n");
                });
                SqlConnection newConn = null;
                SqlConnection conn = null;
                SqlDataReader sqlDataReader = null;
                try
                {
                    conn = SqlUtils.Executor().joinConnStr(url.Text, name.Text, username.Text, password.Text).opening().GetSqlConnection();
                    if (!(string.IsNullOrEmpty(newName.Text) && string.IsNullOrEmpty(newUrl.Text) && string.IsNullOrEmpty(newUsername.Text)))
                    {
                        //为空则无需测试分库
                        newConn = SqlUtils.Executor().joinConnStr(newUrl.Text, newName.Text, newUsername.Text, newPassword.Text).opening().GetSqlConnection();
                    }
                    else
                    {
                        newConn = SqlUtils.Executor().joinConnStr(url.Text, name.Text, username.Text, password.Text).opening().GetSqlConnection();
                    }
                    if (string.IsNullOrEmpty(mainName.Text))
                    {
                        logBox.AppendText("执行失败,主表名称必填\n");
                        return;
                    }
                    if (string.IsNullOrEmpty(subName.Text))
                    {
                        subName.Text = mainName.Text;
                    }

                    logBox.AppendText("查询相关数据中....\n");
                    /*第二种*/
                    ArrayList arrayList = SqlUtils.Executor().queryTable(conn, mainName.Text);
                    List<String> list = null;
                    if (!string.IsNullOrEmpty(delAttribute.Text))
                    {
                        list = new List<string>(delAttribute.Text.Split(','));
                    }
                    //有效属性
                    StringBuilder sb = new StringBuilder();
                    ArrayList attrs = new ArrayList();
                    foreach (TableStruct tableStruct in arrayList)
                    {
                        if (!list.Contains(tableStruct.name))
                        {
                            attrs.Add(tableStruct);
                            sb.Append(tableStruct.name).Append(",");
                        }
                    }


                    string attrsSb = sb.ToString().Remove(sb.ToString().LastIndexOf(","));

                    string findData = "SELECT " + attrsSb + " FROM " + mainName.Text + " ORDER BY " + mainValue.Text + " OFFSET ";
                    int size = 100000;
                    int count = SqlUtils.Executor().queryCount(conn, mainName.Text);
                    int rows = 0;
                    int page = count / size + (count % size > 0 ? 1 : 0);
                    int c = 1;
                    logBox.AppendText("需处理数据：" + count + ",处理次数:" + page + "....\n");
                    //SqlTransaction sqlTran = newConn.BeginTransaction();
                    SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(newConn, SqlBulkCopyOptions.KeepNulls, null);
                    sqlBulkCopy.BatchSize = 100000;
                    sqlBulkCopy.BulkCopyTimeout = 600000;
                    // sqlBulkCopy.DestinationTableName = "";
                    DataTable dt = GetTableSchema(attrs, list);

                    string oldName = null;
                    for (int i = 0; i < page; i++)
                    {
                        string sql = findData + (i * size) + " ROWS FETCH NEXT " + size + " ROWS ONLY";
                        sqlDataReader = SqlUtils.Executor().queryBase(conn, sql);
                        while (sqlDataReader.Read())
                        {
                            string time = sqlDataReader[mainValue.Text].ToString();
                            string name = subName.Text + DateTime.Parse(time).ToString("yyyyMM");
                            if (c % size == 0 || !sqlDataReader.HasRows || (oldName != null && !oldName.Equals(name)))
                            {
                                sqlBulkCopy.DestinationTableName = oldName;

                                sqlBulkCopy.WriteToServer(dt);
                                //sqlTran.Commit();
                                dt.Rows.Clear();
                            }
                            oldName = name;
                            DataRow dataRow = dt.NewRow();
                            for (int j = 0; j < attrs.Count; j++)
                            {
                                TableStruct v = (TableStruct)attrs[j];
                                if (sqlDataReader[v.name].Equals("") && v.isnull == 1)
                                {
                                    dataRow[j] = DBNull.Value;
                                }
                                else
                                {
                                    dataRow[j] = sqlDataReader[v.name];
                                }
                            }
                            dt.Rows.Add(dataRow);
                            c++;
                        }
                        sqlDataReader.Close();
                        if (dt.Rows.Count > 0)
                        {
                            sqlBulkCopy.DestinationTableName = oldName;

                            sqlBulkCopy.WriteToServer(dt);
                            //sqlTran.Commit();
                            dt.Rows.Clear();
                        }

                    }
                    #region

                    /*第一种*/
                    /*
                     ArrayList arrayList = SqlUtils.Executor().queryTable(conn,mainName.Text);
                    List<String> list = null;
                    if(!string.IsNullOrEmpty(delAttribute.Text))
                    {
                        list = new List<string>(delAttribute.Text.Split(','));
                    }
                    //有效属性
                    StringBuilder sb = new StringBuilder();
                    ArrayList attrs = new ArrayList();
                    foreach (TableStruct tableStruct in arrayList)
                    {
                        if (!list.Contains(tableStruct.name) && !(tableStruct.bs == 1))
                        {   
                            attrs.Add(tableStruct);
                            sb.Append(tableStruct.name).Append(",");
                        }
                    }


                      string attrsSb = sb.ToString().Remove(sb.ToString().LastIndexOf(","));

                    string findData = "SELECT "+attrsSb+" FROM " + mainName.Text + " ORDER BY " + mainValue.Text + " OFFSET ";

                    int c = 0;
                    for (int i = 0; i < page; i++)
                    {
                        string sql = findData + (i * size) + " ROWS FETCH NEXT " + size + " ROWS ONLY";
                        sqlDataReader = SqlUtils.Executor().queryBase(conn, sql);
                        //插入次数
                        int insertCount = 0;
                        //插入语句
                        StringBuilder insertSql = new StringBuilder("insert into ");
                        //插入数据值
                        StringBuilder valueSql = new StringBuilder("values");
                        //旧表名
                        string oldName = "";
                        while (sqlDataReader.Read())
                        {
                            insertCount++;
                            //获取表名
                            string time = sqlDataReader[mainValue.Text].ToString();
                            string name = subName.Text + DateTime.Parse(time).ToString("yyyyMM");
                            //第一次添加表名
                            if (insertCount == 1)
                            {
                                insertSql .Append( name) .Append( "(").Append( attrsSb).Append(")");
                                oldName = name;
                            }
                            if (insertCount % 1000 == 0 || !sqlDataReader.HasRows || !oldName.Equals(name))
                            {
                                c++;
                                //执行
                                SqlUtils.Executor().execute(newConn, insertSql.ToString() + valueSql.ToString());
                                insertCount = 1;
                                insertSql = insertSql.Clear().Append( name) .Append( "(").Append( attrsSb).Append(")");
                                valueSql = new StringBuilder().Clear().Append("values");
                                Console.WriteLine("处理第"+c*1000+"插入语句："+insertSql.ToString() + valueSql.ToString());
                            }
                            else
                            {
                                if (insertCount != 1)
                                {
                                    valueSql.Append(",");

                                }
                            }
                            oldName = name;
                            valueSql .Append( "(");
                            for (int j = 0; j < attrs.Count; j++)
                            {

                                TableStruct v = (TableStruct)attrs[j];
                                if (sqlDataReader[v.name].ToString().Equals("") && v.isnull == 1)
                                {
                                    valueSql .Append( "null");
                                }
                                else
                                {
                                    valueSql .Append( "'").Append(sqlDataReader[v.name].ToString()).Append("'");
                                }
                                if (j == attrs.Count-1)
                                {
                                    valueSql.Append(")");
                                }
                                else
                                {
                                    valueSql.Append(",");
                                }
                            }
                        }
                        sqlDataReader.Close();
                        logBox.AppendText("处理第" + i + "次中\n");
                        Console.WriteLine("处理第"+i+"次");
                    }*/
                    #endregion
                }
                catch (Exception e1)
                {
                    //MessageBox.Show(e1.Message);
                    logBox.AppendText(e1.Message + "\n");
                    //logBox.AppendText(sqlDataReader.ToString());
                }
                //finally
                //{
                //    SqlUtils.Executor().closeConn(conn);
                //    SqlUtils.Executor().closeConn(newConn);
                //}
                
                
            });
            
        }
        #endregion
    }
}