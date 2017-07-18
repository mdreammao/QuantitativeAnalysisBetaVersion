using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Configuration;
using QuantitativeAnalysis.Utilities;
using System.IO;

namespace QuantitativeAnalysis.DataAccess
{
    public class TransactionDateTimeRepository
    {
        public TransactionDateTimeRepository(ConnectionType type)
        {
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
        }
        private SqlServerReader sqlReader;
        private SqlServerWriter sqlWriter;
        private WindReader windReader = new WindReader();
        public List<DateTime> GetStockTransactionDate(DateTime start,DateTime end)
        {
            IdentifyOrCreateDBandDataTable();
            for(int year=start.Year; year <= end.Year;year++)
            {
                var existed = sqlReader.ExecuteScalar<int>(string.Format("select 1 from [Common].[dbo].[TransactionDate] where datetime >= '{0}-01-01' and datetime<='{0}-12-31'", year)) > 0;
                if(!existed)
                {
                    var res = windReader.GetTransactionDate(new DateTime(year,1,1), new DateTime(year,12,31)).ToDataTableWithSingleColum("DateTime");
                    sqlWriter.InsertBulk(res, "[Common].[dbo].[TransactionDate]");
                }

            }
            return FetchTransactionDateFromSql(start, end);
        }

        #region internal method
        private List<DateTime> FetchTransactionDateFromSql(DateTime start, DateTime end)
        {
            var sqlStr = string.Format("select DateTime from [Common].[dbo].[TransactionDate] where datetime>='{0}'and datetime<='{1}'", start.ToShortDateString(), end.ToShortDateString());
            var res = sqlReader.GetDataTable(sqlStr);
            return res.ToList<DateTime>();
        }

        private void IdentifyOrCreateDBandDataTable()
        {
            var sqlLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            if (!Directory.Exists(sqlLocation))
                Directory.CreateDirectory(sqlLocation);
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
end",sqlLocation);
            sqlWriter.ExecuteSqlScript(sqlStr);
        }
        #endregion

    }
}
