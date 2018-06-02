using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Configuration;
using System.Data;
using StackExchange.Redis;
using QuantitativeAnalysis.Utilities;
using NLog;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class StockTickRepository
    {
        private readonly string RedisKeyFormat = "{0}-{1}";
        private TransactionDateTimeRepository transDateRepo;
        private SqlServerReader sqlReader;
        private SqlServerWriter sqlWriter;
        private IDataSource dataSource;
        private RedisWriter redisWriter;
        private RedisReader redisReader;
        private Logger logger = LogManager.GetCurrentClassLogger();

        public StockTickRepository(QuantitativeAnalysis.DataAccess.Infrastructure.ConnectionType type,IDataSource ds)
        {
            transDateRepo = new TransactionDateTimeRepository(type);
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
            dataSource = ds;
            redisWriter = new RedisWriter();
            redisReader = new RedisReader();
        }
        public List<StockTickTransaction> GetStockTransaction(string code, DateTime start, DateTime end)
        {
            logger.Info(string.Format("begin to fetch stock{0} tick data from {1} to {2}...", code, start, end));
            code = code.ToUpper();
            if (end.Date >= DateTime.Now.Date)
                throw new ArgumentException("结束时间只能小于当天时间");
            //处理开始时间和结束时间
            start = new DateTime(start.Year, start.Month, start.Day, 9, 30, 00);
            end = new DateTime(end.Year, end.Month, end.Day, 15, 00, 00);
            var transDates = transDateRepo.GetStockTransactionDate(start, end);
            foreach(var date in transDates)
            {
                LoadDataToSqlServerFromSourceIfNecessary(code, date);
                LoadDataToRedisFromSqlServerIfNecessary(code, date);
            }
            logger.Info(string.Format("completed fetching stock{0} tick data from {1} to {2}...", code, start, end));
            var ticks = FetchDataFromRedis(code, transDates).Where(c=>c.TransactionDateTime>=start&&c.TransactionDateTime<=end).OrderBy(c=>c.TransactionDateTime).ToList();
            return ticks;
        }

        private List<StockTickTransaction> FetchDataFromRedis(string code, List<DateTime> transDates)
        {
            var tickList = new List<StockTickTransaction>();
            foreach(var date in transDates)
            {
                var key = string.Format(RedisKeyFormat, code.ToUpper(), date.ToString("yyyy-MM-dd"));
                var hashEntries = redisReader.HGetAll(key);
                var ticks = ConvertToTicks(code,hashEntries);
                tickList.AddRange(ticks);
            }
            return tickList;
        }

        private List<StockTickTransaction> ConvertToTicks(string code,HashEntry[] hashEntries)
        {
            var ticks = new List<StockTickTransaction>();
           for(int i = 0; i < hashEntries.Length; i++)
            {
                var array = hashEntries[i].Value.ToString().Split(',');
                var tick = new StockTickTransaction()
                {
                    Code = code,
                    TransactionDateTime = hashEntries[i].Name.ToString().ToDateTime(),
                    LastPrice = array[0].ConvertTo<double>(),
                    Ask1 = array[1].ConvertTo<double>(),
                    Ask2 = array[2].ConvertTo<double>(),
                    Ask3 = array[3].ConvertTo<double>(),
                    Ask4 = array[4].ConvertTo<double>(),
                    Ask5 = array[5].ConvertTo<double>(),
                    Bid1 = array[6].ConvertTo<double>(),
                    Bid2 = array[7].ConvertTo<double>(),
                    Bid3 = array[8].ConvertTo<double>(),
                    Bid4 = array[9].ConvertTo<double>(),
                    Bid5 = array[10].ConvertTo<double>(),
                    AskV1 = array[11].ConvertTo<double>(),
                    AskV2 = array[12].ConvertTo<double>(),
                    AskV3 = array[13].ConvertTo<double>(),
                    AskV4 = array[14].ConvertTo<double>(),
                    AskV5 = array[15].ConvertTo<double>(),
                    BidV1 = array[16].ConvertTo<double>(),
                    BidV2 = array[17].ConvertTo<double>(),
                    BidV3 = array[18].ConvertTo<double>(),
                    BidV4 = array[19].ConvertTo<double>(),
                    BidV5 = array[20].ConvertTo<double>(),
                    Volume = array[21].ConvertTo<double>(),
                    Amount = array[22].ConvertTo<double>()
                };
                ticks.Add(tick);
            }
            return ticks;
        }

        private void LoadDataToRedisFromSqlServerIfNecessary(string code, DateTime date)
        {
            if(!ExistInRedis(code,date))
            {
                var sqlStr = string.Format(@"SELECT convert(varchar(30),[tdatetime],121) as tdatetime ,[cp] ,[S1] ,[S2]  ,[S3] ,[S4] ,[S5]
      ,[B1] ,[B2] ,[B3],[B4]  ,[B5] ,[SV1] ,[SV2] ,[SV3]  ,[SV4]  ,[SV5] ,[BV1] ,[BV2] ,[BV3],[BV4],[BV5],[ts]  ,[tt]
    FROM [StockTickTransaction{0}].[dbo].[{1}] where rtrim(stkcd)='{2}'", date.Year, date.ToString("yyyy-MM-dd"),code);
                var dt = sqlReader.GetDataTable(sqlStr);
                var key = string.Format(RedisKeyFormat, code.ToUpper(), date.ToString("yyyy-MM-dd"));
                BulkWriteToRedis(key, dt);
            }
        }

        private bool ExistInRedis(string code, DateTime date)
        {
            var key = string.Format(RedisKeyFormat, code.ToUpper(), date.ToString("yyyy-MM-dd"));
            return redisReader.ContainsKey(key);
        }

        private void BulkWriteToRedis(string key,DataTable dt)
        {
            if(dt!=null&&dt.Rows.Count > 0)
            {
                HashEntry[] entries = GenerateEnties(dt);
                redisWriter.HSetBulk(key, entries);
            }
        }

        private HashEntry[] GenerateEnties(DataTable dt)
        {
            HashEntry[] entries = new HashEntry[dt.Rows.Count];
            for(int i=0;i<dt.Rows.Count;i++)
            {
                var array = dt.Rows[i].ItemArray;
                var values = string.Join(",", array.Skip(1));
                entries[i] = new HashEntry(dt.Rows[i]["tdatetime"].ToString(), values);
            }
            return entries;
        }

        private void LoadDataToSqlServerFromSourceIfNecessary(string code, DateTime date, Infrastructure.ConnectionType type= Infrastructure.ConnectionType.Local)
        {
            if (!ExistInSqlServer(code,date))
            {
                CreateDBOrTableIfNecessary(date);
                var dt = dataSource.Get(code, new DateTime(date.Year,date.Month,date.Day,9,30,0,0), new DateTime(date.Year,date.Month,date.Day,15,0,0,0));
                if(dt.Rows.Count>0)
                    sqlWriter.InsertBulk(dt, string.Format("[StockTickTransaction{0}].[dbo].[{1}]",date.Year,date.ToString("yyyy-MM-dd")));
            }
            else
            {
                var dt = dataSource.GetFromSpecializedSQLServer(code, date, type);
            }
        }

        private void CreateDBOrTableIfNecessary(DateTime date)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript =string.Format(@"use master
if db_id('StockTickTransaction{0}') is null
begin
CREATE DATABASE [StockTickTransaction{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'StockTickTransaction{0}', FILENAME = N'{2}\StockTickTransaction{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'StockTickTransaction{0}_log', FILENAME = N'{2}\StockTickTransaction{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [StockTickTransaction{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [StockTickTransaction{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[StockTickTransaction{0}].dbo.[{1}]') is null
begin
CREATE TABLE [StockTickTransaction{0}].[dbo].[{1}](
	[stkcd] [char](10) NOT NULL,
	[tdatetime] [datetime] NOT NULL,
	[cp] [decimal](12, 4) NULL,
	[S1] [decimal](12, 4) NULL,
	[S2] [decimal](12, 4) NULL,
	[S3] [decimal](12, 4) NULL,
	[S4] [decimal](12, 4) NULL,
	[S5] [decimal](12, 4) NULL,
	[B1] [decimal](12, 4) NULL,
	[B2] [decimal](12, 4) NULL,
	[B3] [decimal](12, 4) NULL,
	[B4] [decimal](12, 4) NULL,
	[B5] [decimal](12, 4) NULL,
	[SV1] [decimal](10, 0) NULL,
	[SV2] [decimal](10, 0) NULL,
	[SV3] [decimal](10, 0) NULL,
	[SV4] [decimal](10, 0) NULL,
	[SV5] [decimal](10, 0) NULL,
	[BV1] [decimal](10, 0) NULL,
	[BV2] [decimal](10, 0) NULL,
	[BV3] [decimal](10, 0) NULL,
	[BV4] [decimal](10, 0) NULL,
	[BV5] [decimal](10, 0) NULL,
	[ts] [decimal](20, 0) NULL,
	[tt] [decimal](20, 3) NULL,
	[LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [StockTickTransaction{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}] ON [StockTickTransaction{0}].[dbo].[{1}]
(
	[stkcd] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [StockTickTransaction{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", date.Year,date.ToString("yyyy-MM-dd"),fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(string code, DateTime date)
        {
            var sqlScript = string.Format(@"use master
if db_id('StockTickTransaction{0}') is not null
begin
	if object_id('[StockTickTransaction{0}].dbo.[{1}]') is not null
	begin
		select 1 from [StockTickTransaction{0}].dbo.[{1}] where rtrim(stkcd)='{2}'
	end
end
else
begin
select 0
end ",date.Year,date.ToString("yyyy-MM-dd"),code.ToUpper());
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }
    }
}
