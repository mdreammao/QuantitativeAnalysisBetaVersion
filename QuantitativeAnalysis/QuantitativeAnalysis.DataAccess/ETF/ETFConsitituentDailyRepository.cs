using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Configuration;
using System.Data;
using QuantitativeAnalysis.Utilities;
using QuantitativeAnalysis.DataAccess.Stock;
using NLog;

namespace QuantitativeAnalysis.DataAccess.ETF
{
    public class ETFConsitituentDailyRepository
    {
        private const string RedisFieldFormat = "yyyy-MM-dd";
        private RedisReader redisReader = new RedisReader();
        private RedisWriter redisWriter = new RedisWriter();
        private SqlServerReader sqlReader;
        private SqlServerWriter sqlWriter;
        private TransactionDateTimeRepository dateTimeRepo;
        private DefaultETFConstituentDailyDataSource dataSource;
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public ETFConsitituentDailyRepository(ConnectionType type, DefaultETFConstituentDailyDataSource ds)
        {
            dateTimeRepo = new TransactionDateTimeRepository(type);
            sqlWriter = new SqlServerWriter(type);
            sqlReader = new SqlServerReader(type);
            redisReader = new RedisReader();
            redisWriter = new RedisWriter();
            dataSource = ds;
        }

        //public List<ETFConsitituent> GetETFConsitituent(string code, DateTime today)
        //{
        //    logger.Info(string.Format("begin to fetch ETF{0} Consitituent date: {1}...", code, today));
        //    var consitituent = new List<ETFConsitituent>();
        //    //consitituent = FetchETFConsitituentFromRedis(code, today);
        //    //if (consitituent == null)
        //    //{
        //    //    LoadETFConsitituentToSqlFromSource(code, today);
        //    //    LoadETFConsitituentToRedisFromSql(code, today);
        //    //    consitituent = FetchETFConsitituentFromRedis(code, today);
        //    //}
        //    logger.Info(string.Format("completed fetching ETF{0} Consitituent date: {1}", code,today));
        //    return consitituent;
        //}

        //private void LoadETFConsitituentToSqlFromSource(string code, DateTime today)
        //{
        //    var dt = dataSource.Get(code, today);
        //    sqlWriter.InsertBulk(dt, string.Format(@"[ETFConsitituent].[dbo].[{1}]",code));
        //}



    }
}
