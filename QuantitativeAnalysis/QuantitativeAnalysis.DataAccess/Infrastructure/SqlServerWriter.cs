using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class SqlServerWriter
    {
        private readonly string connStr;
        public SqlServerWriter(string connStr)
        {
            this.connStr = connStr;
        }
        public int WriteChanges(string sqlStr,SqlParameter[] pams=null)
        {
            int rowsChanged = 0;
            using (var conn = new SqlConnection(connStr))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = sqlStr;
                if (pams != null) cmd.Parameters.AddRange(pams);
                rowsChanged = cmd.ExecuteNonQuery();
            }
            return rowsChanged;
        }
    }
}
