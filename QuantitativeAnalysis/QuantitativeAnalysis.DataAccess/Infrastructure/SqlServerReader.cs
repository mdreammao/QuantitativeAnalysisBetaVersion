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

    }
}
