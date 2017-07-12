using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class RedisWriter
    {
        public bool HSet(string key,string field,string value,int dbId = 0)
        {
            var db = RedisClientSingleton.Instance.GetDatabase(dbId);
            return db.HashSet(key, field, value);
        }

        public void HSetBulk(string key,HashEntry[] entries,int dbId=0)
        {
            var db = RedisClientSingleton.Instance.GetDatabase(dbId);
            db.HashSet(key, entries);
        }

        public bool HDelete(string key,string field,int dbId = 0)
        {
            var db = RedisClientSingleton.Instance.GetDatabase(dbId);
            return db.HashDelete(key, field);
        }
    }
}
