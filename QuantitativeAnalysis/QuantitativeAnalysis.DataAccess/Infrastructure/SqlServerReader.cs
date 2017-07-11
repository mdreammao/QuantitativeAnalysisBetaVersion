using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class SqlServerReader
    {
        private readonly string connStr;

        public SqlServerReader(string connStr)
        {
            this.connStr = connStr;
        }

        public DataTable GetDataTable(string sqlStr,SqlParameter[] sqlPam=null)
        {
            var conn = new SqlConnection(connStr);
            SqlDataAdapter adapter = new SqlDataAdapter(sqlStr, conn);
            if(sqlPam!=null) adapter.SelectCommand.Parameters.AddRange(sqlPam);
            var dt = new DataTable();
            adapter.Fill(dt);
            return dt;
        }

    }
}
