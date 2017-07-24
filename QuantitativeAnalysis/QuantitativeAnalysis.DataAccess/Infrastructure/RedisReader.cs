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
        public string HGet(string key,string field,int dbId=0)
        {
            var db = RedisClientSingleton.Instance.GetDatabase(dbId);

            return db.HashGet(key, field);
        }

        public HashEntry[] HGetAll(string key,int dbId = 0)
        {
            var db = RedisClientSingleton.Instance.GetDatabase(dbId);
            return db.HashGetAll(key);
        }

        public RedisValue[] HGetAllFields(string key,int dbId = 0)
        {
            var db = RedisClientSingleton.Instance.GetDatabase(0);
            return db.HashKeys(key);
        }

        public RedisValue SGet(string key,int dbId = 0)
        {
            var db = RedisClientSingleton.Instance.GetDatabase(dbId);
            return db.StringGet(key);
        }

        public bool ContainsKey(string key,int dbId=0)
        {
            var db = RedisClientSingleton.Instance.GetDatabase(dbId);
            return db.KeyExists(key);
        }
    }
}
