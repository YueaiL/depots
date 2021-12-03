using System;
using System.Collections;
using System.Data.SqlClient;
using System.Text;
using depots.src.zyz.pojo;

namespace depots.zyz
{
    class SqlUtils
    {
        private string sql;
        private SqlConnection conn;
        private static SqlUtils sqlUtils = new SqlUtils();
        public static SqlUtils Executor()
        {
            return sqlUtils;
        }

        internal SqlUtils joinConnStr(string url, string name, string username, string password)
        {

            if ((string.IsNullOrEmpty(name) && string.IsNullOrEmpty(url) && string.IsNullOrEmpty(username)))
            {
                throw new Exception("数据库连接信息为null");
            }
            StringBuilder sb = new StringBuilder();
            sb.Append("Data Source = ").Append(url)
                .Append(";Initial Catalog = ").Append(name)
                .Append(";User ID = ").Append(username)
                .Append(";Password = ").Append(password);
            sql = sb.ToString();
            return sqlUtils;
        }


        internal SqlUtils opening()
        {

            conn = new SqlConnection(sql);
            conn.Open();


            return sqlUtils;
        }

        internal SqlConnection GetSqlConnection()
        {
            return this.conn;
        }

        internal SqlUtils close()
        {
            if (conn != null)
            {
                conn.Close();
            }
            return sqlUtils;
        }

        public int queryCount(SqlConnection sqlConnection,string tableName)
        {
            string findCount = "select count(*) from " + tableName + " with(nolock)";
            return (int)new SqlCommand(findCount, sqlConnection).ExecuteScalar() ;
        }

        public ArrayList query(SqlConnection sqlConnection,string sql)
        {
            if (sqlConnection == null)
            {
                sqlConnection = conn;
            }
            SqlCommand sqlCommand = new SqlCommand(sql, sqlConnection);
            SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
            ArrayList arrayList = new ArrayList();
            while (sqlDataReader.Read())
            {
                arrayList.Add(sqlDataReader["tableName"].ToString());
            }
            sqlDataReader.Close();
            return arrayList;
        }

        public void create(SqlConnection sqlConnection,string sql)
        {
            if (sqlConnection == null)
            {
                sqlConnection = conn;
            }
            
        }

        #region 查询表的列信息
        public ArrayList queryTable(SqlConnection conn,string name)
        {
            string sql = @"SELECT  CASE WHEN col.colorder = 1 THEN obj.name  
                  ELSE ''  
             END AS 表名,  
        col.colorder AS 序号 ,  
        col.name AS 列名 ,  
        ISNULL(ep.[value], '') AS 列说明 ,  
        t.name AS 数据类型 ,  
        case when t.name = 'numeric' then col.xprec 
				else col.length end AS 长度 ,  
        ISNULL(COLUMNPROPERTY(col.id, col.name, 'Scale'), 0) AS 小数位数 ,  
        CASE WHEN COLUMNPROPERTY(col.id, col.name, 'IsIdentity') = 1 THEN 1  
             ELSE 0  
        END AS 标识 ,  
        CASE WHEN EXISTS ( SELECT   1  
                           FROM     dbo.sysindexes si  
                                    INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id  
                                                              AND si.indid = sik.indid  
                                    INNER JOIN dbo.syscolumns sc ON sc.id = sik.id  
                                                              AND sc.colid = sik.colid  
                                    INNER JOIN dbo.sysobjects so ON so.name = si.name  
                                                              AND so.xtype = 'PK'  
                           WHERE    sc.id = col.id  
                                    AND sc.colid = col.colid ) THEN 1  
             ELSE 0  
        END AS 主键 ,  
        CASE WHEN col.isnullable = 1 THEN 1 
             ELSE 0 
        END AS 允许空 ,  
        ISNULL(comm.text, '') AS 默认值  
FROM    dbo.syscolumns col  
        LEFT  JOIN dbo.systypes t ON col.xtype = t.xusertype  
        inner JOIN dbo.sysobjects obj ON col.id = obj.id  
                                         AND obj.xtype = 'U'  
                                         AND obj.status >= 0  
        LEFT  JOIN dbo.syscomments comm ON col.cdefault = comm.id  
        LEFT  JOIN sys.extended_properties ep ON col.id = ep.major_id  
                                                      AND col.colid = ep.minor_id  
                                                      AND ep.name = 'MS_Description'  
        LEFT  JOIN sys.extended_properties epTwo ON obj.id = epTwo.major_id  
                                                         AND epTwo.minor_id = 0  
                                                         AND epTwo.name = 'MS_Description'  
WHERE   obj.name = '?'
ORDER BY col.colorder ";
            sql = sql.Replace("?", name);
            int v = sql.IndexOf("\r\n");
            while (sql.IndexOf("\r\n") > 0)
            {
                sql = sql.Replace("\r\n", " ");
            }
            SqlCommand sqlCommand = new SqlCommand(sql, conn);
            SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
            ArrayList arrayList = new ArrayList();
            while (sqlDataReader.Read())
            {
                TableStruct tableStruct = new TableStruct();
                tableStruct.name = sqlDataReader["列名"].ToString();
                tableStruct.remark = sqlDataReader["列说明"].ToString();
                tableStruct.type = sqlDataReader["数据类型"].ToString();
                tableStruct.len = sqlDataReader["长度"].ToString();
                tableStruct.lessLen = sqlDataReader["小数位数"].ToString();
                tableStruct.bs = int.Parse(sqlDataReader["标识"].ToString());
                tableStruct.main = int.Parse(sqlDataReader["主键"].ToString());
                tableStruct.isnull = int.Parse(sqlDataReader["允许空"].ToString());
                tableStruct.defaultVal = sqlDataReader["默认值"].ToString();
                arrayList.Add(tableStruct);
            }
            sqlDataReader.Close();
            return arrayList;
        }
        #endregion
        
        internal void closeConn(SqlConnection conn)
        {
            if (conn != null)
            {
                conn.Close();
            }
        }

        internal void execute(SqlConnection conn, string v)
        {
            SqlCommand sqlCommand = new SqlCommand(v, conn);
            sqlCommand.ExecuteNonQuery();
        }

        internal SqlDataReader queryBase(SqlConnection conn, string findData)
        {
            SqlCommand sqlCommand = new SqlCommand(findData, conn);
            return sqlCommand.ExecuteReader();
        }
    }
}
