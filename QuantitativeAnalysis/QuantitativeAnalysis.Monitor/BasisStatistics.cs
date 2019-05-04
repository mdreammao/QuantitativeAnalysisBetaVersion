using Autofac;
using NLog;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.Utilities;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static QuantitativeAnalysis.Utilities.DateTimeExtension;

namespace QuantitativeAnalysis.Monitor
{
    public class BasisStatistics
    {
        private Logger logger = LogManager.GetCurrentClassLogger();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private string code;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private StockMinuteRepository stockMinutelyRepo;
        private TransactionDateTimeRepository dateRepo;
        private string indexCode;


        public BasisStatistics(StockMinuteRepository stockMinutelyRepo, string code)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
            this.code = code;
            indexCode = code == "IF" ? "000300.SH" : "000905.SH";

        }

        public void compute(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var parameters = startDate.ToShortDateString() + '_' + endDate.ToShortDateString()+"frontNext";
            if (!ExistInSqlServer(code, parameters))
            {
                CreateDBOrTableIfNecessary(code, parameters);
            }
            DataTable dt = new DataTable();
            dt.Columns.Add("tdatetime", typeof(DateTime));
            dt.Columns.Add("future1");
            dt.Columns.Add("future2");
            dt.Columns.Add("expiredate1", typeof(DateTime));
            dt.Columns.Add("expiredate2", typeof(DateTime));
            dt.Columns.Add("duration1");
            dt.Columns.Add("duration2");
            dt.Columns.Add("indexPrice");
            dt.Columns.Add("price1");
            dt.Columns.Add("price2");
            dt.Columns.Add("basis1");
            dt.Columns.Add("basis2");
            dt.Columns.Add("basis12");

            foreach (var date in tradedays)
            {
                //var list = getSpecialFutureList(date);
                var list = getFutureList(date);
                var index = stockMinutelyRepo.GetStockTransactionWithRedis(indexCode, date, date);
                var dataList = new Dictionary<string, List<StockTransaction>>();
                var basisDataList = new List<specialBasis>();
                foreach (var item in list)
                {
                    var data = stockMinutelyRepo.GetStockTransactionWithRedis(item.Key, date, date);
                    dataList.Add(item.Key, data);
                }
                for (int i = 5; i <= 235; i=i+5)
                {
                    var data0 = new specialBasis();
                    var indexNow = index[i];
                    var future1 = dataList[list.Keys.First()][i];
                    var future2 = dataList[list.Keys.Last()][i];
                    data0.future1 = future1.Code;
                    data0.future2 = future2.Code;
                    data0.time = indexNow.DateTime;
                    data0.expireDate1 = list[list.Keys.First()].expireDate;
                    data0.expireDate2= list[list.Keys.Last()].expireDate;
                    data0.indexPrice = indexNow.Close;
                    data0.price1 =future1.Volume==0?future1.Close: (future1.Amount / future1.Volume)/200;
                    data0.price2 =future2.Volume==0?future2.Close: (future2.Amount/future2.Volume)/200;
                    data0.basis1 = data0.price1 - data0.indexPrice;
                    data0.basis2 = data0.price2 - data0.indexPrice;
                    data0.basis12 = data0.price2 - data0.price1;
                    data0.duration1= DateUtils.GetSpanOfTradeDays(date, data0.expireDate1) / 252.0;
                    data0.duration2 = DateUtils.GetSpanOfTradeDays(date, data0.expireDate2) / 252.0;
                    basisDataList.Add(data0);
                }
                foreach (var item in basisDataList)
                {
                    DataRow dr = dt.NewRow();
                    dr["tdatetime"] = item.time;
                    dr["future1"] = item.future1;
                    dr["future2"] = item.future2;
                    dr["expireDate1"] = item.expireDate1;
                    dr["expireDate2"] = item.expireDate2;
                    dr["indexPrice"] = item.indexPrice;
                    dr["price1"] = item.price1;
                    dr["price2"] = item.price2;
                    dr["basis1"] = item.basis1;
                    dr["basis2"] = item.basis2;
                    dr["basis12"] = item.basis12;
                    dr["duration1"] = item.duration1;
                    dr["duration2"] = item.duration2;
                    dt.Rows.Add(dr);
                }
            }
            SaveResultToMssql(dt, code, parameters);
        }


        /// <summary>
        /// 根据计算规则获取当日交易日的股指期货列表
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        private Dictionary<string, CFEFutures> getFutureList(DateTime date)
        {
            var number = 2; //3为选择3个合约，4为选择4个合约
            List<CFEFutures> list = new List<CFEFutures>();
            List<DateTime> dateList = new List<DateTime>();
            Dictionary<string, CFEFutures> dic = new Dictionary<string, CFEFutures>();
            var expireDateOfThisMonth = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
            if (date > expireDateOfThisMonth)
            {
                date = DateUtils.GetFirstDateOfNextMonth(date);
            }
            dateList.Add(date);
            var date2 = DateUtils.GetFirstDateOfNextMonth(date);
            dateList.Add(date2);
            var date3 = DateUtils.GetLastDateOfThisSeason(date2);
            if (date3.Month == date2.Month)
            {
                date3 = DateUtils.GetFirstDateOfNextMonth(date3);
                date3 = DateUtils.GetLastDateOfThisSeason(date3);
            }
            dateList.Add(date3);
            var date4 = DateUtils.GetLastDateOfThisSeason(DateUtils.GetFirstDateOfNextMonth(date3));
            dateList.Add(date4);
            for (int i = 0; i < number; i++)
            {
                date = dateList[i];
                var future = new CFEFutures();
                string year = date.Year.ToString();
                year = year.Substring(year.Length - 2, 2);
                string month = "0" + date.Month.ToString();
                month = month.Substring(month.Length - 2, 2);
                future.code = code + year + month + ".CFE";
                future.expireDate = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
                list.Add(future);
            }
            foreach (var item in list)
            {
                dic.Add(item.code, item);
            }
            return dic;
        }

        /// <summary>
        /// 根据计算规则获取当日交易日的当月和季月的股指期货列表
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        private Dictionary<string, CFEFutures> getSpecialFutureList(DateTime date)
        {
            List<CFEFutures> list = new List<CFEFutures>();
            List<DateTime> dateList = new List<DateTime>();
            Dictionary<string, CFEFutures> dic = new Dictionary<string, CFEFutures>();
            var expireDateOfThisMonth = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
            if (date > expireDateOfThisMonth)
            {
                date = DateUtils.GetFirstDateOfNextMonth(date);
            }
            dateList.Add(date);
            var date2 = DateUtils.GetLastDateOfThisSeason(date);
            if (date.Month==date2.Month)
            {
                date2 = DateUtils.GetLastDateOfThisSeason(DateUtils.GetFirstDateOfNextMonth(date));
            }
            dateList.Add(date2);
            for (int i = 0; i < 2; i++)
            {
                date = dateList[i];
                var future = new CFEFutures();
                string year = date.Year.ToString();
                year = year.Substring(year.Length - 2, 2);
                string month = "0" + date.Month.ToString();
                month = month.Substring(month.Length - 2, 2);
                future.code = code + year + month + ".CFE";
                future.expireDate = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
                list.Add(future);
            }
            foreach (var item in list)
            {
                dic.Add(item.code, item);
            }
            return dic;
        }



        private void SaveResultToMssql(DataTable dt, string indexCode, string parameters)
        {
            var sql = string.Format(@"delete from [BasisData{0}].[dbo].[{1}]", indexCode, parameters);
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[BasisData{0}].[dbo].[{1}]", indexCode, parameters));
        }

        private void CreateDBOrTableIfNecessary(string indexCode, string parameters)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('BasisData{0}') is null
begin
CREATE DATABASE [BasisData{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'BasisData{0}', FILENAME = N'{2}\BasisData{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'BasisData{0}_log', FILENAME = N'{2}\BasisData{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [BasisData{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [BasisData{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[BasisData{0}].dbo.[{1}]') is null
begin
CREATE TABLE [BasisData{0}].[dbo].[{1}](
	[tdatetime] [datetime] NOT NULL,
    [future1] varchar(20)  NOT NULL,
    [future2] varchar(20)  NOT NULL,
    [expiredate1] [datetime] NOT NULL,
    [expiredate2] [datetime] NOT NULL,
    [duration1] [decimal](12, 4) NULL,
    [duration2] [decimal](12, 4) NULL,
    [indexPrice] [decimal](12, 4) NULL,
    [price1] [decimal](12, 4) NULL,
    [price2] [decimal](12, 4) NULL,
    [basis1] [decimal](12, 4) NULL,
    [basis2] [decimal](12, 4) NULL,
    [basis12] [decimal](12, 4) NULL,
    [LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [BasisData{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [BasisData{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", indexCode, parameters, fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(string indexCode, string parameters)
        {
            var sqlScript = string.Format(@"use master
if db_id('BasisData{0}') is not null
begin
	if object_id('[BasisData{0}].dbo.[{1}]') is not null
	begin
		select 1 from [BasisData{0}].dbo.[{1}] 
	end
end
else
begin
select 0
end ", indexCode, parameters);
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }
    }
}
