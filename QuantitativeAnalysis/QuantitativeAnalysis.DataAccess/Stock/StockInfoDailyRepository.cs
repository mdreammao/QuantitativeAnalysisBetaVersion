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
        private WindReader windReader = new WindReader();

        public StockInfoDailyRepository(QuantitativeAnalysis.DataAccess.Infrastructure.ConnectionType type, IDataSource dataSource)
        {
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
            dateRepo = new TransactionDateTimeRepository(type);
            this.dataSource = dataSource;
        }

        //先从万德获取数据，然后存入本地sql
        public List<StockBasicInfo> getStockBasicInfoDaily(string code, DateTime start, DateTime end)
        {
            List<StockBasicInfo> infoList = new List<StockBasicInfo>();
            var list = loadInfoDailyFromSql(code, start, end);
            return infoList;
        }

        private List<StockBasicInfo> loadInfoDailyFromSql(string code, DateTime start, DateTime end)
        {
            List<StockBasicInfo> infoList = new List<StockBasicInfo>();
            var timeList = SplitTimeYearly(start, end);
            foreach (var day in timeList)
            {
                DateTime dayStart = day;
                DateTime dayEnd =DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay( new DateTime(day.Year, 12, 31));
                if (dayEnd>end)
                {
                    dayEnd = end;
                }
                var sqlStr = string.Format(@"select  [Code],[DateTime] from [StockInfo].[dbo].[BasicInfoDaily{0}] 
where Code='{1}' and DateTime>='{2}' and DateTime<='{3}'",
dayStart.Year, code, dayStart, dayEnd);
                var dt = sqlReader.GetDataTable(sqlStr);
                var tradedays = dateRepo.GetStockTransactionDate(dayStart, dayEnd);
                if (dt.Rows.Count<tradedays.Count()) //数据不足，去万德拉取
                {
                    var dtAll = windReader.GetDailyDataTable(code, "", dayStart, dayEnd);
                    var dtLack = getLackData(dt, dtAll);
                    if (dtLack.Rows.Count>0)
                    {
                        WriteToSql(dtLack);
                    }
                    infoList.AddRange(dataTableToList(dtAll));
                }
                else
                {
                    infoList.AddRange(dataTableToList(dt));
                }
            }
            return infoList;
        }


        private List<StockBasicInfo> dataTableToList(DataTable dt)
        {
            List<StockBasicInfo> list = new List<StockBasicInfo>();
            return list;
        }


        private DataTable getLackData(DataTable dt,DataTable dtAll)
        {
            DataTable dtLack = new DataTable();
            return dtLack;
        }


        private List<DateTime> SplitTimeYearly(DateTime start, DateTime end)
        {
            List<DateTime> timeList = new List<DateTime>();
            var tradedays = dateRepo.GetStockTransactionDate(start, end);
            foreach (var day in tradedays)
            {
                DateTime year =DateTimeExtension.DateUtils.NextOrCurrentTradeDay(new DateTime(day.Year, 1, 1));
                if (year<start)
                {
                    year = start;
                }
                if (timeList.Contains(year)==false)
                {
                    timeList.Add(year);
                }
            }
            return timeList;
        }


        //将股票数据存到sql
        private void WriteToSql(DataTable dataTable)
        {
            Dictionary<DateTime, DataTable> yearData = SplitDataTableYearly(dataTable);
            foreach (var item in yearData)
            {
                IdentifyOrCreateDBAndTable(item.Key);
                sqlWriter.InsertBulk(item.Value, string.Format("[StockInfo].[dbo].[BasicInfoDaily{0}]", item.Key.Year));
            }
        }



        private Dictionary<DateTime, DataTable> SplitDataTableYearly(DataTable dataTable)
        {
            var yearData = new Dictionary<DateTime, DataTable>();
            foreach (DataRow r in dataTable.Rows)
            {
                var date = r["DateTime"].ToString().ConvertTo<DateTime>();
                var key = new DateTime(date.Year, 1, 1);
                if (!yearData.ContainsKey(key))
                {
                    var dt = dataTable.Clone();
                    dt.ImportRow(r);
                    yearData.Add(key, dt);
                }
                else
                    yearData[key].ImportRow(r);
            }
            return yearData;
        }


        //构造SQL表结构
        private void IdentifyOrCreateDBAndTable(DateTime date)
        {
            var sqlLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"USE [master]
if db_id('StockInfo') is null
begin
CREATE DATABASE [StockInfo]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'StockInfo', FILENAME = N'{0}\StockInfo.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'StockInfo_log', FILENAME = N'{0}\StockInfo.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [StockInfo] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [StockInfo].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[StockInfo].[dbo].[BasicInfoDaily{1}]') is null
begin
CREATE TABLE [StockInfo].[dbo].[BasicInfoDaily{1}](
	[Code] [varchar](20) NOT NULL,
	[DateTime] [datetime] NOT NULL,
	[UpdatedDateTime] [datetime] NULL,
 CONSTRAINT [PK_BasicInfoDaily{1}] PRIMARY KEY CLUSTERED 
(
	[Code] ASC,
	[DateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
SET ANSI_PADDING OFF
ALTER TABLE [StockInfo].[dbo].[BasicInfoDaily{1}] ADD  CONSTRAINT [DF_BasicInfoDaily{1}_UpdatedDateTime]  DEFAULT (getdate()) FOR [UpdatedDateTime]
end ", sqlLocation,date.Year);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(string code, DateTime date)
        {
            var sqlScript = string.Format(@"use master
if db_id('StockInfo') is not null
begin
	if object_id('[StockInfo].dbo.[BasicInfoDaily{0}]') is not null
	begin
		select 1 from [StockInfo].dbo.[BasicInfoDaily{0}] where rtrim(Code)='{1}'
	end
end
else
begin
select 0
end ", date.Year, code);
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }


    }
}
