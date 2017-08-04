using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Configuration;
using System.IO;

namespace QuantitativeAnalysis.DataAccess.Option
{
    public class OptionInfoRepository
    {
        private WindReader windReader = new WindReader();
        private SqlServerWriter sqlWriter;
        public OptionInfoRepository(ConnectionType type)
        {
            sqlWriter = new SqlServerWriter(type);
        }
        public void UpdateOptionInfo(string underlyingCode)
        {
            underlyingCode = underlyingCode.ToUpper();
            var exchange = underlyingCode.EndsWith(".SH") ? "sse" : throw new Exception("暂不支持");
            var dt = windReader.GetDataSetTable("optioncontractbasicinfo", string.Format("exchange={0};windcode={1};status=all;field=wind_code,sec_name,option_mark_code,call_or_put,exercise_mode,exercise_price,contract_unit,listed_date,expire_date",
                exchange, underlyingCode));
            WriteToSqlServer(underlyingCode,dt);
        }

        private void WriteToSqlServer(string underlyingCode,DataTable dt)
        {
            IdentifyOrCreateDBAndTable();
            ClearExistedOptionInfo(underlyingCode);
            sqlWriter.InsertBulk(dt, "[Common].dbo.[OptionInfo]");
        }

        private void ClearExistedOptionInfo(string underlyingCode)
        {
            var sql = string.Format(@"delete [Common].dbo.[OptionInfo] where option_mark_code='{0}'",underlyingCode);
            sqlWriter.WriteChanges(sql);
        }

        private void IdentifyOrCreateDBAndTable()
        {
            var sqlLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            if (!Directory.Exists(sqlLocation))
                Directory.CreateDirectory(sqlLocation);
            var sql = string.Format(@"use [master];
if db_id('Common') is null
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
if object_id('Common.dbo.OptionInfo') is null
begin
CREATE TABLE [Common].[dbo].[OptionInfo](
	[wind_code] [varchar](20) NOT NULL,
	[sec_name] [nvarchar](30) NULL,
	[option_mark_code] [varchar](15) NOT NULL,
	[exercise_mode] [nvarchar](10) NULL,
	[exercise_price] [decimal](10, 3) NULL,
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

SET ANSI_PADDING OFF
ALTER TABLE [Common].[dbo].[OptionInfo] ADD  CONSTRAINT [DF_OptionInfo_update_date_time]  DEFAULT (getdate()) FOR [update_date_time]

end
go",sqlLocation);
            sqlWriter.ExecuteSqlScript(sql);
        }
    }
}
