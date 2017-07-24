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

namespace QuantitativeAnalysis.DataAccess
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
        public StockTickRepository(ConnectionType type,IDataSource ds)
        {
            transDateRepo = new TransactionDateTimeRepository(type);
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
            dataSource = ds;
            redisWriter = new RedisWriter();
        }
        public List<StockTickTransaction> GetStockTransaction(string code, DateTime start, DateTime end)
        {
            if (end.Date >= DateTime.Now.Date)
                throw new ArgumentException("结束时间只能小于当天时间");
            var transDates = transDateRepo.GetStockTransactionDate(start, end);
            foreach(var date in transDates)
            {
                LoadDataToSqlServerFromSourceIfNecessary(code, date);
                LoadDataToRedisFromSqlServerIfNecessary(code, date);
            }
            return FetchDataFromRedis(code, transDates).Where(c=>c.TransactionDateTime>=start&&c.TransactionDateTime<=end).ToList();
        }

        private List<StockTickTransaction> FetchDataFromRedis(string code, List<DateTime> transDates)
        {
            transDates.Select(c =>
            {
                var key= string.Format(RedisKeyFormat, code, c.ToString("yyyy-MM-dd"));

            });
        }

        private void LoadDataToRedisFromSqlServerIfNecessary(string code, DateTime date)
        {
            if(!ExistInRedis(code,date))
            {
                var sqlStr = string.Format(@"SELECT [stkcd],[tdatetime] ,[cp] ,[S1] ,[S2]  ,[S3] ,[S4] ,[S5]
      ,[B1] ,[B2] ,[B3],[B4]  ,[B5] ,[SV1] ,[SV2] ,[SV3]  ,[SV4]  ,[SV5] ,[BV1] ,[BV2] ,[BV3],[BV4],[BV5],[ts]  ,[tt]
    FROM [StockTickTransaction{0}].[dbo].[{1}]", date.Year, date.ToString("yyyy-MM-dd");
                var dt = sqlReader.GetDataTable(sqlStr);
                var key = string.Format(RedisKeyFormat, code, date.ToString("yyyy-MM-dd"));
                BulkWriteToRedis(key, dt);
            }
        }

        private bool ExistInRedis(string code, DateTime date)
        {
            var key = string.Format(RedisKeyFormat, code, date.ToString("yyyy-MM-dd"));
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
                var values = string.Join(",", array.Skip(2));
                entries[i] = new HashEntry(dt.Rows[i]["tdatetime"].ToString(), values);
            }
            return entries;
        }

        private void LoadDataToSqlServerFromSourceIfNecessary(string code, DateTime date)
        {
            if (!ExistInSqlServer(code,date))
            {
                CreateDBOrTableIfNecessary(date);
                var dt = dataSource.Get(code, date, date.AddHours(23));
                sqlWriter.InsertBulk(dt, string.Format("[StockTickTransaction{0}].[dbo].[{1}]",date.Year,date.ToString("yyyy-MM-dd")));
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
if object_id('[StockTickTransaction{0}].dbo.[{1}]') is null
begin
CREATE TABLE [dbo].[{1}](
	[stkcd] [char](10) NULL,
	[tdatetime] [datetime] NULL,
	[cp] [decimal](9, 3) NULL,
	[S1] [decimal](9, 3) NULL,
	[S2] [decimal](9, 3) NULL,
	[S3] [decimal](9, 3) NULL,
	[S4] [decimal](9, 3) NULL,
	[S5] [decimal](9, 3) NULL,
	[B1] [decimal](9, 3) NULL,
	[B2] [decimal](9, 3) NULL,
	[B3] [decimal](9, 3) NULL,
	[B4] [decimal](9, 3) NULL,
	[B5] [decimal](9, 3) NULL,
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
ALTER TABLE [dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}] ON [dbo].[{1}]
(
	[stkcd] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [dbo].[{1}]
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
		select 1 from [StockTickTransaction{0}].dbo.[{1}]
	end
end
else
begin
select 0
end ",date.Year,date.ToString("yyyy-MM-dd"));
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > 0;
        }
    }
}
