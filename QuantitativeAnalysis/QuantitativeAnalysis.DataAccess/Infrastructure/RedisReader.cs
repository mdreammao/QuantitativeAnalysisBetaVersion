using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class RedisReader
    {
        ConnectionMultiplexer redisConn = null;
        public RedisReader(ConfigurationOptions config)
        {
            redisConn = ConnectionMultiplexer.Connect(config);
        }

        public void HSet(string key,string field,string value)
        {
            var db = redisConn.GetDatabase();

            db.HashSet(key, field, value);
        }

    }
}
