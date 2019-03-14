using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using StackExchange.Redis;
using System.Data.SqlClient;
using QuantitativeAnalysis.Utilities;
using System.Data;
using Newtonsoft.Json;
using System.Configuration;
using System.IO;
using NLog;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class StockInfoDailyRepository 
    {
        private const string RedisFieldFormat = "yyyy-MM-dd";
        private RedisReader redisReader = new RedisReader();
        private RedisWriter redisWriter = new RedisWriter();
        private SqlServerReader sqlReader;
        private SqlServerWriter sqlWriter;
        private TransactionDateTimeRepository dateRepo;
        private IDataSource dataSource;
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();
        public StockInfoDailyRepository(QuantitativeAnalysis.DataAccess.Infrastructure.ConnectionType type, IDataSource dataSource)
        {
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
            dateRepo = new TransactionDateTimeRepository(type);
            this.dataSource = dataSource;
        }

        
    }
}
