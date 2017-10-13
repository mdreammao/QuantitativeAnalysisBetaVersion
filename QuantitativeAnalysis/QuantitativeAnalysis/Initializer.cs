using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.IO;
using System.Configuration;
using Autofac;
namespace QuantitativeAnalysis
{
    public class Initializer
    {
        public static void Initialize(ConnectionType type)
        {
            CreateDBAndTableIfNecessary(type);
            InstanceFactory.Initialize();
        }

        private static void CreateDBAndTableIfNecessary(ConnectionType type)
        {
            var sqlWriter = new SqlServerWriter(type);
            var sqlLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            if (!Directory.Exists(sqlLocation))
                Directory.CreateDirectory(sqlLocation);
            CreateCommonDBAndTables(sqlLocation, sqlWriter);
            CreateDailyTransactionDBAndTables(sqlLocation, sqlWriter);
        }

        private static void CreateDailyTransactionDBAndTables(string sqlLocation, SqlServerWriter sqlWriter)
        {
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
if object_id('DailyTransaction.dbo.Stock') is null
begin
CREATE TABLE [DailyTransaction].[dbo].[Stock](
	[Code] [varchar](20) NOT NULL,
	[DateTime] [date] NOT NULL,
	[OPEN] [decimal](12, 4) NULL,
	[HIGH] [decimal](12, 4) NULL,
	[LOW] [decimal](12, 4) NULL,
	[CLOSE] [decimal](12, 4) NULL,
	[VOLUME] [decimal](20, 0) NULL,
	[AMT] [decimal](20, 3) NULL,
	[ADJFACTOR] [decimal](20, 6) NULL,
	[TRADE_STATUS] [nvarchar](50) NULL,
	[UpdatedDateTime] [datetime] NULL CONSTRAINT [DF_Stock_UpdatedDateTime]  DEFAULT (getdate()),
 CONSTRAINT [PK_Stock_1] PRIMARY KEY CLUSTERED 
(
	[Code] ASC,
	[DateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
end", sqlLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private static void CreateCommonDBAndTables(string filePath,SqlServerWriter sqlWriter)
        {
            string sqlStr = string.Format(@"use [master];
if db_id('common') is null
begin
 	CREATE DATABASE [Common]
	CONTAINMENT = NONE
	ON  PRIMARY 
	( NAME = N'Common', FILENAME = N'{0}\Common.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
	 LOG ON 
	( NAME = N'Common_log', FILENAME = N'{0}\Common_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
	ALTER DATABASE [Common] SET COMPATIBILITY_LEVEL = 120
	IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
	begin
	EXEC [Common].[dbo].[sp_fulltext_database] @action = 'enable'
	end
end
go
if object_id('Common.dbo.[TransactionDate]') is null
begin
	CREATE TABLE [Common].[dbo].[TransactionDate](
	[DateTime] [date] NOT NULL,
	CONSTRAINT [PK_TransactionDate] PRIMARY KEY CLUSTERED 
	([DateTime] ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
		) ON [PRIMARY] 
end
go
if object_id('Common.dbo.OptionInfo') is null
begin
CREATE TABLE [Common].[dbo].[OptionInfo](
	[wind_code] [varchar](20) NOT NULL,
	[sec_name] [nvarchar](30) NULL,
	[option_mark_code] [varchar](15) NOT NULL,
	[exercise_mode] [nvarchar](10) NULL,
	[exercise_price] [nvarchar](10) NULL,
	[call_or_put] [nvarchar](10) NULL,
	[contract_unit] [int] NULL,
	[listed_date] [date] NULL,
	[expire_date] [date] NULL,
	[update_date_time] [datetime] NULL,
 CONSTRAINT [PK_OptionInfo] PRIMARY KEY CLUSTERED 
(
	[wind_code] ASC,
	[option_mark_code] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [Common].[dbo].[OptionInfo] ADD  CONSTRAINT [DF_OptionInfo_update_date_time]  DEFAULT (getdate()) FOR [update_date_time]

end
go
if object_id('Common.dbo.StockInfo') is null
begin
    CREATE TABLE [Common].[dbo].[StockInfo](
	    [Code] [varchar](12) NOT NULL,
	    [SecName] [nvarchar](12) NULL,
	    [IPODate] [date] NULL,
	    [DelistDate] [date] NULL,
	    [UpdateDateTime] [datetime] NULL,
     CONSTRAINT [PK_StockInfo] PRIMARY KEY CLUSTERED 
    (
	    [Code] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY]
    ALTER TABLE [Common].[dbo].[StockInfo] ADD  CONSTRAINT [DF_StockInfo_UpdateDateTime]  DEFAULT (getdate()) FOR [UpdateDateTime]
end
GO", filePath);
            sqlWriter.ExecuteSqlScript(sqlStr);
        }


    }
}
