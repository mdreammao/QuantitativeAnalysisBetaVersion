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
using QuantitativeAnalysis.DataAccess.Stock;
namespace QuantitativeAnalysis.DataAccess.Option
{
    public class StockOptionDailyRepository : IStockOptionRepository
    {
        private const string RedisFieldFormat = "yyyy-MM-dd";
        private RedisReader redisReader = new RedisReader();
        private RedisWriter redisWriter = new RedisWriter();
        private SqlServerReader sqlReader;
        private SqlServerWriter sqlWriter;
        private TransactionDateTimeRepository dateRepo;
        private IDataSource dataSource;
        public StockOptionDailyRepository(QuantitativeAnalysis.DataAccess.Infrastructure.ConnectionType type, IDataSource dataSource)
        {
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
            dateRepo = new TransactionDateTimeRepository(type);
            this.dataSource = dataSource;
        }

        public List<StockOptionTransaction> GetStockOptionTransaction(string code, DateTime begin, DateTime end)
        {
            var stocks = new List<StockOptionTransaction>();
            var tradingDates = dateRepo.GetStockTransactionDate(begin, end);
            if (tradingDates != null && tradingDates.Count > 0)
            {
                foreach (var date in tradingDates)
                {
                    StockOptionTransaction trans = FetchStockFromRedis(code, date);
                    if (trans == null)//just run once 
                    {
                        LoadStockTransactionToSqlFromSource(code, tradingDates);
                        LoadStockTransactionToRedisFromSql(code, tradingDates);
                        trans = FetchStockFromRedis(code, date);
                    }
                    stocks.Add(trans);
                }
            }
            return stocks;
        }

        #region internal method
        private StockOptionTransaction FetchStockFromRedis(string code, DateTime date)
        {
            var jsonStr = redisReader.HGet(code, date.ToString(RedisFieldFormat));
            if (string.IsNullOrEmpty(jsonStr))
                return null;
            var st = JsonConvert.DeserializeObject<StockOptionTransaction>(jsonStr);
            st.Code = code;
            st.DateTime = date;
            st.Level = StockOptionTransactionLevel.Daily;
            return st;
        }

        private void LoadStockTransactionToRedisFromSql(string code, List<DateTime> tradingDates)
        {
            var existedDateInRedis = GetExistedDateInRedis(code, tradingDates.First(), tradingDates.Last());
            var nonExistedDateIntervalInRedis = Computor.GetNoExistedInterval<DateTime>(tradingDates, existedDateInRedis);
            if (nonExistedDateIntervalInRedis != null && nonExistedDateIntervalInRedis.Count > 0)
            {
                string sqlStr = GenerateSqlString(code, nonExistedDateIntervalInRedis);
                var dt = sqlReader.GetDataTable(sqlStr);
                WriteToRedis(code, dt);
            }
        }

        private void WriteToRedis(string code, DataTable dt)
        {
            HashEntry[] entries = new HashEntry[dt.Rows.Count];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var field = dt.Rows[i]["DateTime"].ToString().ToDateTime().ToString(RedisFieldFormat);
                DataRowConverter converter = new DataRowConverter();
                converter.AddExceptFields("DateTime");
                var value = JsonConvert.SerializeObject(dt.Rows[i], converter);
                entries[i] = new HashEntry(field, value);
            }
            redisWriter.HSetBulk(code, entries);
        }

        private string GenerateSqlString(string code, List<KeyValuePair<DateTime, DateTime>> nonExistedDateIntervalnInRedis)
        {
            var sqlStr = "select DATETIME,[OPEN],HIGH,LOW,[CLOSE],VOLUME,AMT,SETTLE,OI from [DailyTransaction].[dbo].[StockOption] where Code='{0}' and {1};";
            var dateConditions = new StringBuilder();
            foreach (var pair in nonExistedDateIntervalnInRedis)
            {
                string dateCondition;
                if (pair.Key == pair.Value)
                    dateCondition = string.Format("datetime='{0}'", pair.Value);
                else
                    dateCondition = string.Format("datetime>='{0}' and datetime<='{1}'", pair.Key, pair.Value);
                if (dateConditions.Length > 0)
                    dateConditions.Append(" or " + dateCondition);
                else
                    dateConditions.Append(dateCondition);
            }
            return string.Format(sqlStr, code, dateConditions.ToString());
        }

        private List<DateTime> GetExistedDateInRedis(string code, DateTime begin, DateTime end)
        {
            var allExistedInRedis = redisReader.HGetAllFields(code).ConvertTo<DateTime>();
            return allExistedInRedis.Where(c => c >= begin && c <= end).ToList();
        }

        private void LoadStockTransactionToSqlFromSource(string code, List<DateTime> tradingDates)
        {
            IdentifyOrCreateDBandDataTable();
            var existedDateInSql = GetExistedDateInSql(code, tradingDates.First(), tradingDates.Last());
            var nonExistedDateIntervalInSql = Computor.GetNoExistedInterval<DateTime>(tradingDates, existedDateInSql);
            foreach (var item in nonExistedDateIntervalInSql)
            {
                var dt = dataSource.Get(code, item.Key, item.Value);
                //数据需要清洗一遍，为NaN的数据变成0
                foreach (DataRow dr in dt.Rows)
                {
                    for (int i = 2; i < dt.Columns.Count; i++)
                    {
                        if (double.IsNaN(Convert.ToDouble(dr[i]))==true)
                        {
                            //dr[i] = 0;
                            dr[i] = DBNull.Value;                        }
                    }
                }
                sqlWriter.InsertBulk(dt, "[DailyTransaction].[dbo].[StockOption]");
            }
        }

        private void IdentifyOrCreateDBandDataTable()
        {
            var sqlLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            if (!Directory.Exists(sqlLocation))
                Directory.CreateDirectory(sqlLocation);
            var sqlScript = string.Format(@"USE [master]
if db_id('DailyTransaction') is null
begin
CREATE DATABASE [DailyTransaction]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'DailyTransaction', FILENAME = N'{0}\DailyTransaction.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 10%)
 LOG ON 
( NAME = N'DailyTransaction_log', FILENAME = N'{0}\DailyTransaction_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [DailyTransaction] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [DailyTransaction].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('DailyTransaction.dbo.StockOption') is null
begin
CREATE TABLE [DailyTransaction].[dbo].[StockOption](
	[Code] [varchar](20) NOT NULL,
	[DateTime] [date] NOT NULL,
	[OPEN] [decimal](12, 4) NULL,
	[HIGH] [decimal](12, 4) NULL,
	[LOW] [decimal](12, 4) NULL,
	[CLOSE] [decimal](12, 4) NULL,
	[VOLUME] [decimal](20, 0) NULL,
	[AMT] [decimal](20, 3) NULL,
	[SETTLE] [decimal](20, 4) NULL,
	[OI] [decimal](20, 0) NULL,
	[UpdatedDateTime] [datetime] NULL CONSTRAINT [DF_StockOption_UpdatedDateTime]  DEFAULT (getdate()),
 CONSTRAINT [PK_StockOption_1] PRIMARY KEY CLUSTERED 
(
	[Code] ASC,
	[DateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
end", sqlLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private List<DateTime> GetExistedDateInSql(string code, DateTime start, DateTime end)
        {
            var sqlStr = string.Format("select DateTime from [DailyTransaction].[dbo].[StockOption] where Code='{0}' and DateTime>='{1}' and DateTime<='{2}'", code, start.ToShortDateString(), end.ToShortDateString());
            return sqlReader.GetDataTable(sqlStr).ToList<DateTime>();
        }

        private void GetStockDailyTransactionFromSqlServer(string code, DateTime begin, DateTime end)
        {
            var sqlString = "select * from [DailyTransaction].[dbo].[StockOption] where code='@code' and DateTime>=@begin and DateTime<=@end";
            var pars = new SqlParameter[]
            {
                new SqlParameter("@code",code),
                new SqlParameter("@begin",begin.Date),
                new SqlParameter("@end",end.Date)
            };
            var res = sqlReader.GetDataTable(sqlString, pars);
        }
        #endregion
    }
}

