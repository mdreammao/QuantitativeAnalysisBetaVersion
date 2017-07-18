using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class SqlServerWriter
    {
        private readonly ConnectionType connType;
        public SqlServerWriter(ConnectionType connType)
        {
            this.connType = connType;
        }
        public int WriteChanges(string sqlStr,SqlParameter[] pams=null)
        {
            int rowsChanged = 0;
            using (var conn = SqlConnectionFactory.Create(connType))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sqlStr;
                if (pams != null) cmd.Parameters.AddRange(pams);
                rowsChanged = cmd.ExecuteNonQuery();
            }
            return rowsChanged;
        }

        public void InsertBulk(DataTable source,string destinationTbName,List<SqlBulkCopyColumnMapping> mappings=null)
        {
            using (var conn = SqlConnectionFactory.Create(connType))
            {
                using (var bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = destinationTbName;
                    if (mappings == null)
                        foreach (DataColumn col in source.Columns)
                            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    else
                        mappings.ForEach(m => bulkCopy.ColumnMappings.Add(m));
                    conn.Open();
                    bulkCopy.WriteToServer(source);
                }
            }
        }
        public void ExecuteSqlScript(string sqlScript)
        {
            using (var conn = SqlConnectionFactory.Create(connType))
            {
                Server server = new Server(new ServerConnection(conn));
                server.ConnectionContext.ExecuteNonQuery(sqlScript);
            }
        }

    }
}
