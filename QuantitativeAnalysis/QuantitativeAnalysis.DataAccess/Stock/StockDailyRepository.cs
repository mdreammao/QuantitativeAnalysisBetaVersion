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
    public class StockDailyRepository : IStockRepository
    {
        private const string RedisFieldFormat = "yyyy-MM-dd";
        private RedisReader redisReader = new RedisReader();
        private RedisWriter redisWriter = new RedisWriter();
        private SqlServerReader sqlReader;
        private SqlServerWriter sqlWriter;
        private TransactionDateTimeRepository dateRepo;
        private IDataSource dataSource;
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();
        public StockDailyRepository(QuantitativeAnalysis.DataAccess.Infrastructure.ConnectionType type,IDataSource dataSource)
        {
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
            dateRepo = new TransactionDateTimeRepository(type);
            this.dataSource = dataSource;
        }

        public List<StockTransaction> GetStockTransaction(string code, DateTime begin,DateTime end)
        {
            logger.Info(string.Format("begin to fetch stock{0} daily data from {1} to {2}...",code,begin,end));
            var stocks = new List<StockTransaction>();
            var tradingDates = dateRepo.GetStockTransactionDate(begin, end);
            if (tradingDates != null && tradingDates.Count > 0)
            {
                foreach (var date in tradingDates)
                {
                    StockTransaction trans = FetchStockFromRedis(code, date);
                    if (trans == null)//just run once 
                    {
                        LoadStockTransactionToSqlFromSource(code, tradingDates);
                        LoadStockTransactionToRedisFromSql(code, tradingDates);
                        trans = FetchStockFromRedis(code, date);
                    }
                   stocks.Add(trans);
                }
            }
            logger.Info(string.Format("completed fetching stock{0} daily data from {1} to {2}...", code, begin, end));
            return stocks;
        }

        #region internal method
        private StockTransaction FetchStockFromRedis(string code, DateTime date)
        {
            var jsonStr = redisReader.HGet(code,date.ToString(RedisFieldFormat));
            if (string.IsNullOrEmpty(jsonStr))
                return null;
            var st = JsonConvert.DeserializeObject<StockTransaction>(jsonStr);
            st.Code = code;
            st.DateTime = date;
            st.Level = StockTransactionLevel.Daily;
            return st;
        }

        private void LoadStockTransactionToRedisFromSql(string code, List<DateTime> tradingDates)
        {
            var existedDateInRedis = GetExistedDateInRedis(code, tradingDates.First(), tradingDates.Last());
            var nonExistedDateIntervalInRedis = Computor.GetNoExistedInterval<DateTime>(tradingDates, existedDateInRedis);
            if (nonExistedDateIntervalInRedis !=null && nonExistedDateIntervalInRedis.Count > 0)
            {
                string sqlStr = GenerateSqlString(code, nonExistedDateIntervalInRedis);
                var dt = sqlReader.GetDataTable(sqlStr);
                WriteToRedis(code, dt);
            }
        }

        private void WriteToRedis(string code, DataTable dt)
        {
            HashEntry[] entries = new HashEntry[dt.Rows.Count];
            for(int i=0;i<dt.Rows.Count;i++)
            {
                var field = dt.Rows[i]["DateTime"].ToString().ToDateTime().ToString(RedisFieldFormat);
                DataRowConverter converter = new DataRowConverter();
                converter.AddExceptFields("DateTime");
                var value =JsonConvert.SerializeObject(dt.Rows[i], converter);
                entries[i] = new HashEntry(field, value);
            }
            redisWriter.HSetBulk(code, entries);
        }

        private string GenerateSqlString(string code, List<KeyValuePair<DateTime, DateTime>> nonExistedDateIntervalnInRedis)
        {
            var sqlStr = "select DATETIME,[OPEN],HIGH,LOW,[CLOSE],VOLUME,AMT,ADJFACTOR,TRADE_STATUS from [DailyTransaction].[dbo].[Stock] where Code='{0}' and ({1});";
            var dateConditions = new StringBuilder();
            foreach (var pair in nonExistedDateIntervalnInRedis)
            {
                string dateCondition;
                if(pair.Key==pair.Value)
                    dateCondition = string.Format("datetime='{0}'", pair.Value);
                else
                    dateCondition = string.Format("datetime>='{0}' and datetime<='{1}'", pair.Key, pair.Value);
                if (dateConditions.Length > 0)
                    dateConditions.Append(" or " + dateCondition);
                else
                    dateConditions.Append(dateCondition);
            }
            return string.Format(sqlStr,code,dateConditions.ToString());
        }

        private List<DateTime> GetExistedDateInRedis(string code, DateTime begin, DateTime end)
        {
            var allExistedInRedis = redisReader.HGetAllFields(code).ConvertTo<DateTime>();
            return allExistedInRedis.Where(c=>c>=begin && c<=end).ToList();
        }

        private void LoadStockTransactionToSqlFromSource(string code, List<DateTime> tradingDates)
        {
            var existedDateInSql = GetExistedDateInSql(code, tradingDates.First(), tradingDates.Last());
            var nonExistedDateIntervalInSql = Computor.GetNoExistedInterval<DateTime>(tradingDates, existedDateInSql);
            foreach (var item in nonExistedDateIntervalInSql)
            {
                if (item.Value<=new DateTime(2019,3,8))
                {

                }
                else
                {
                    var dt = dataSource.Get(code, item.Key, item.Value);
                    sqlWriter.InsertBulk(dt, "[DailyTransaction].[dbo].[Stock]");
                }
                
            }
        }

        private List<DateTime> GetExistedDateInSql(string code, DateTime start, DateTime end)
        {
            var sqlStr = string.Format("select DateTime from [DailyTransaction].[dbo].[Stock] where Code='{0}' and DateTime>='{1}' and DateTime<='{2}'",code,start.ToShortDateString(),end.ToShortDateString());
            return sqlReader.GetDataTable(sqlStr).ToList<DateTime>();
        }

        private void GetStockDailyTransactionFromSqlServer(string code,DateTime begin,DateTime end)
        {
            var sqlString = "select * from [DailyTransaction].[dbo].[Stock] where code='@code' and DateTime>=@begin and DateTime<=@end";
            var pars = new SqlParameter[]
            {
                new SqlParameter("@code",code),
                new SqlParameter("@begin",begin.Date),
                new SqlParameter("@end",end.Date)
            };
            var res=sqlReader.GetDataTable(sqlString,pars);
        }
        #endregion
    }
}
