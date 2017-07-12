using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using System.Threading;
using System.Configuration;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class RedisClientSingleton
    {
        public  static ConnectionMultiplexer conn = null;
        public static ConnectionMultiplexer Instance
        {
            get
            {
                if (conn == null || !conn.IsConnected)
                {
                    var connString = ConfigurationManager.ConnectionStrings["RedisServer"].ConnectionString;
                    conn = ConnectionMultiplexer.Connect(connString);
                }
                return conn;
            }
        }
    }
}
