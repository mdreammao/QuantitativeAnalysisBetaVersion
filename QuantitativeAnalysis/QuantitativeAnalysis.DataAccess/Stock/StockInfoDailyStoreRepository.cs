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
using NLog;
using static QuantitativeAnalysis.Utilities.DateTimeExtension;
namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class StockInfoDailyStoreRepository
    {
        private TransactionDateTimeRepository dateTimeRepo;
        private RedisReader redisReader;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private RedisWriter redisWriter;
        private IDataSource dataSource;
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public StockInfoDailyStoreRepository(ConnectionType type, IDataSource ds)
        {
            dateTimeRepo = new TransactionDateTimeRepository(type);
            sqlWriter = new SqlServerWriter(type);
            sqlReader = new SqlServerReader(type);
            redisReader = new RedisReader();
            redisWriter = new RedisWriter();
            dataSource = ds;
        }

    }
}
