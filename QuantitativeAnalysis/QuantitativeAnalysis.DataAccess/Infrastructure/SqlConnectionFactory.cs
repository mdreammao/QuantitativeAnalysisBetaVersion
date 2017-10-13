using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Configuration;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class SqlConnectionFactory
    {
        public static SqlConnection Create(ConnectionType connType=ConnectionType.Default)
        {
            var connString =ConfigurationManager.ConnectionStrings[connType.ToString()].ConnectionString;
            return new SqlConnection(connString);
        }
    }

    public enum ConnectionType
    {
        Default,
        Local,
        Server170,
        Server217,
        Server84,
        Server182
    }
}
