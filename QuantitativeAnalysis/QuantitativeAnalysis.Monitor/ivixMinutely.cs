using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Utilities;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.DataAccess.Option;
using QuantitativeAnalysis;
using NLog;
using Autofac;
using QuantitativeAnalysis.Transaction;
using System.Data;
using System.Configuration;

namespace QuantitativeAnalysis.Monitor
{
    public class ivixMinutely
    {
        private double rate;
        private double cashVega = 10000;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string underlying = "510050.SH";
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private OptionInfoRepository infoRepo;
        private StockMinuteRepository stockRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;

        public ivixMinutely(OptionInfoRepository infoRepo, StockMinuteRepository stockRepo, double rate = 0.04)
        {
            this.infoRepo = infoRepo;
            this.stockRepo = stockRepo;
            this.rate = rate;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void record(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //CreateDBOrTableIfNecessary(startDate);
            //CreateDBOrTableIfNecessary(startDate.AddYears(1));
            //var start = startDate;
            //while (start < endDate)
            //{
            //    if (!ExistInSqlServer(start))
            //    {
            //        CreateDBOrTableIfNecessary(start);
            //    }
            //    start = start.AddYears(1);
            //}
            //if (!ExistInSqlServer(endDate))
            //{
            //    CreateDBOrTableIfNecessary(endDate);
            //}

            foreach (var date in tradedays)
            {
                var etf = stockRepo.GetStockTransactionWithRedis(underlying, date, date);
            }
               
        }



        private void SaveResultToMssql(DateTime date, DataTable dt)
        {
            var sql = string.Format(@"delete from [ivix{0}].[dbo].[{1}] where tdatetime>'{2}' and tdatetime<'{3}'", date.Year, date.ToString("yyyy"), date.ToString("yyyy-MM-dd"), date.AddDays(1).ToString("yyyy-MM-dd"));
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[ivix{0}].[dbo].[{1}]", date.Year, date.ToString("yyyy")));
        }

        private void CreateDBOrTableIfNecessary(DateTime date)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('ivix{0}') is null
begin
CREATE DATABASE [ivix{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'ivix{0}', FILENAME = N'{2}\ivix{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'ivix{0}_log', FILENAME = N'{2}\ivix{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [ivix{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [ivix{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[ivix{0}].dbo.[{1}]') is null
begin
CREATE TABLE [ivix{0}].[dbo].[{1}](
	[tdatetime] [datetime] NOT NULL,
    [expiredate1] [datetime] NULL,
    [expiredate2] [datetime] NULL,
	[duration1] [decimal](10, 6) NULL,
    [duration2] [decimal](10, 6) NULL,
    [sigma1Ask] [decimal](10, 4) NULL,
    [sigma1Bid] [decimal](10, 4) NULL,
    [sigma2Ask] [decimal](10, 4) NULL,
    [sigma2Bid] [decimal](10, 4) NULL,
    [sigmaAsk] [decimal](10, 4) NULL,
    [sigmaBid] [decimal](10, 4) NULL,
    [vegaTotal] [decimal](10, 4) NULL,
    [number] [decimal](10, 4) NULL,
    [percentAskMax] [decimal](10, 4) NULL,
    [percentAskMin] [decimal](10, 4) NULL,
    [percentBidMax] [decimal](10, 4) NULL,
    [percentBidMin] [decimal](10, 4) NULL,
	[LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [ivix{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [ivix{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", date.Year, date.ToString("yyyy"), fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(DateTime date)
        {
            var sqlScript = string.Format(@"use master
if db_id('ivix{0}') is not null
begin
	if object_id('[ivix{0}].dbo.[{1}]') is not null
	begin
		select 1 from [ivix{0}].dbo.[{1}] 
	end
end
else
begin
select 0
end ", date.Year, date.ToString("yyyy"));
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }

        private DataTable initializeDataTable(DataTable dt)
        {
            dt.Columns.Add("tdatetime");
            dt.Columns.Add("expiredate1");
            dt.Columns.Add("expiredate2");
            dt.Columns.Add("duration1");
            dt.Columns.Add("duration2");
            dt.Columns.Add("sigma1Ask");
            dt.Columns.Add("sigma1Bid");
            dt.Columns.Add("sigma2Ask");
            dt.Columns.Add("sigma2Bid");
            dt.Columns.Add("sigmaAsk");
            dt.Columns.Add("sigmaBid");
            dt.Columns.Add("vegaTotal");
            dt.Columns.Add("number");
            dt.Columns.Add("percentAskMax");
            dt.Columns.Add("percentAskMin");
            dt.Columns.Add("percentBidMax");
            dt.Columns.Add("percentBidMin");
            return dt;
        }

    }

}
