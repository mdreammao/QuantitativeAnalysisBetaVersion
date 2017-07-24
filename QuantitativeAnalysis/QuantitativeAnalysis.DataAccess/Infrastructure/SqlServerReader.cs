using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class SqlServerReader
    {
        private readonly ConnectionType connType;

        public SqlServerReader(ConnectionType connType)
        {
            this.connType = connType;
        }

        public DataTable GetDataTable(string sqlStr,SqlParameter[] sqlPam=null)
        {
            SqlDataAdapter adapter = new SqlDataAdapter(sqlStr, SqlConnectionFactory.Create(connType));
            if(sqlPam!=null) adapter.SelectCommand.Parameters.AddRange(sqlPam);
            var dt = new DataTable();
            adapter.Fill(dt);
            return dt;
        }

        public T ExecuteScalar<T>(string sqlStr, SqlParameter[] pams = null)
        {
            using (var conn = SqlConnectionFactory.Create(connType))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sqlStr;
                if (pams != null) cmd.Parameters.AddRange(pams);
                object obj = cmd.ExecuteScalar();
                if (obj == null||obj==DBNull.Value)
                    return default(T);
                return (T)Convert.ChangeType(obj, typeof(T));
            }
        }
        public T ExecuteScriptScalar<T>(string sqlScript)
        {
            using (var conn = SqlConnectionFactory.Create(connType))
            {
                Server server = new Server(new ServerConnection(conn));
                var obj = server.ConnectionContext.ExecuteScalar(sqlScript);
                if (obj == null || obj == DBNull.Value)
                    return default(T);
                return (T)Convert.ChangeType(obj, typeof(T));
            }
        }

    }
}
