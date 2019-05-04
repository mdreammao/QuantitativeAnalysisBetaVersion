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
    public class BasisChoice
    {
        private Logger logger = LogManager.GetCurrentClassLogger();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private string code;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private StockMinuteRepository stockMinutelyRepo;
        private TransactionDateTimeRepository dateRepo;
        private double slip = 10;
        private double treshold = 100;
        private string indexCode;
        

        public BasisChoice(StockMinuteRepository stockMinutelyRepo,string code)
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
            var choice = new Dictionary<DateTime, List<CFEFuturesChoice>>();
            string myHoldStr = "";
            var tradeRecord = new List<CFEFuturesTradeRecord>();
            var parameters = startDate.ToShortDateString() + '_' + endDate.ToShortDateString()+"_treshold"+treshold.ToString()+"_slip"+slip.ToString();
            if (!ExistInSqlServer(code,parameters))
            {
                CreateDBOrTableIfNecessary(code, parameters);
            }
            DataTable dt = new DataTable();
            dt.Columns.Add("code");
            dt.Columns.Add("tdatetime", typeof(DateTime));
            dt.Columns.Add("expiredate", typeof(DateTime));
            dt.Columns.Add("indexPrice");
            dt.Columns.Add("price");
            dt.Columns.Add("annulizedBasis");
            
            foreach (var date in tradedays)
            {
                var list = getFutureList(date);
                var index = stockMinutelyRepo.GetStockTransactionWithRedis(indexCode, date, date);
                var dataList = new Dictionary<string, List<StockTransaction>>();
                var holdNow = new CFEFutures();
                var choiceToday = new List<CFEFuturesChoice>();
                
                if (myHoldStr!="")
                {
                    holdNow = list[myHoldStr];
                }
                foreach (var item in list)
                {
                    var data = stockMinutelyRepo.GetStockTransactionWithRedis(item.Key, date, date);
                    dataList.Add(item.Key, data);
                }
                for (int i = 5; i < 235; i++)
                {
                    
                    DateTime expireDate=new DateTime();
                    CFEFuturesChoice record = new CFEFuturesChoice();
                    double basis = -10000;
                    if (myHoldStr=="")
                    {
                        var transaction = new CFEFuturesTradeRecord();
                        foreach (var item in list)
                        {
                            double basisNow = getAnuualizedBasis(item.Value, date, dataList[item.Value.code][i].Close, index[i].Close);
                            if (basisNow>basis)
                            {
                                holdNow = item.Value;
                                basis = basisNow;
                                expireDate = item.Value.expireDate;
                                myHoldStr = item.Key;
                                transaction.code = item.Key;
                                transaction.indexPrice = index[i].Close;
                                transaction.price = dataList[item.Value.code][i].Close;
                                transaction.time = index[i].DateTime;
                                transaction.direction = -1;
                                transaction.expireDate = item.Value.expireDate;
                            }
                        }
                        //第一次开仓
                        tradeRecord.Add(transaction);
                    }
                    else
                    {
                        double modify = 0;
                        var closeRecord = new CFEFuturesTradeRecord();
                        var transaction = new CFEFuturesTradeRecord();
                        var latestBasis = tradeRecord.Last().price - tradeRecord.Last().indexPrice;
                        var latestTime = tradeRecord.Last().time;
                        var latestExpireDate = tradeRecord.Last().expireDate;
                        var latestAnnulizedBasis = getAnuualizedBasis2(latestBasis, latestTime, latestExpireDate);
                        closeRecord.time = index[i].DateTime;
                        closeRecord.indexPrice = index[i].Close;
                        closeRecord.direction = 1;
                        closeRecord.code = holdNow.code;
                        closeRecord.price = dataList[holdNow.code][i].Close;
                        closeRecord.expireDate = holdNow.expireDate;
                        basis = getAnuualizedBasis(holdNow, date, dataList[holdNow.code][i].Close, index[i].Close);
                        foreach (var item in list)
                        {
                            if (item.Value!=holdNow)
                            {
                                modify = slip;
                            }
                            double basisNow = getAnuualizedBasis(item.Value, date, dataList[item.Value.code][i].Close-modify, index[i].Close);
                            double annulizedBasisNow = getAnuualizedBasis2(dataList[item.Value.code][i].Close  - modify+latestBasis-dataList[holdNow.code][i].Close, latestTime, item.Value.expireDate);
                            if ((annulizedBasisNow>latestAnnulizedBasis+treshold && DateUtils.GetSpanOfTradeDays(date,item.Value.expireDate)>10)|| DateUtils.GetSpanOfTradeDays(date, holdNow.expireDate) < 5)
                            //if (DateUtils.GetSpanOfTradeDays(date, holdNow.expireDate) < 5)
                            {
                                holdNow = item.Value;
                                basis = basisNow;
                                expireDate = item.Value.expireDate;
                                myHoldStr = item.Key;
                                transaction.code = item.Key;
                                transaction.indexPrice = index[i].Close;
                                transaction.price = dataList[item.Value.code][i].Close;
                                transaction.time = index[i].DateTime;
                                transaction.direction = -1;
                                transaction.expireDate = expireDate;
                            }
                        }
                        if (transaction.code!=null)
                        {
                            tradeRecord.Add(closeRecord);
                            tradeRecord.Add(transaction);
                        }
                    }
                    record.code = holdNow.code;
                    record.expireDate = holdNow.expireDate;
                    record.annulizedBasis = basis;
                    record.indexPrice = index[i].Close;
                    record.price = dataList[holdNow.code][i].Close;
                    choiceToday.Add(record);
                    DataRow dr = dt.NewRow();
                    dr["code"] = record.code;
                    dr["tdatetime"] = index[i].DateTime;//etf[i].TransactionDateTime;
                    dr["expiredate"] = record.expireDate;
                    dr["indexPrice"] = record.indexPrice;
                    dr["price"] = record.price;
                    dr["annulizedBasis"] = record.annulizedBasis;
                    dt.Rows.Add(dr);
                }
                choice.Add(date, choiceToday);
            }
            var pnl = getPnL(tradeRecord);
            var netValue = getNetValue(tradeRecord, tradedays);
            Console.WriteLine(pnl);
            SaveResultToMssql(dt, code, parameters);

            DataTable dt2 = new DataTable();
            dt2.Columns.Add("code");
            dt2.Columns.Add("tdatetime", typeof(DateTime));
            dt2.Columns.Add("expiredate", typeof(DateTime));
            dt2.Columns.Add("indexPrice");
            dt2.Columns.Add("price");
            dt2.Columns.Add("direction");
            for (int i = 0; i < tradeRecord.Count(); i++)
            {
                DataRow dr = dt2.NewRow();
                var record = tradeRecord[i];
                dr["code"] = record.code;
                dr["tdatetime"] = record.time;
                dr["expiredate"] = record.expireDate;
                dr["indexPrice"] = record.indexPrice;
                dr["price"] = record.price;
                dr["direction"] = record.direction;
                dt2.Rows.Add(dr);
            }
            if (!ExistInSqlServer2(code, parameters))
            {
                CreateDBOrTableIfNecessary2(code, parameters);
            }
            SaveResultToMssql2(dt2, code, parameters);

            DataTable dt3 = new DataTable();
            dt3.Columns.Add("code");
            dt3.Columns.Add("tdatetime", typeof(DateTime));
            dt3.Columns.Add("expiredate", typeof(DateTime));
            dt3.Columns.Add("basis");
            dt3.Columns.Add("annulizedBasis");
            dt3.Columns.Add("basisWithSlip");
            dt3.Columns.Add("annulizedBasisWithSlip");
            for (int i = 0; i < netValue.Count(); i++)
            {
                DataRow dr = dt3.NewRow();
                var record = netValue[i];
                dr["code"] = record.code;
                dr["tdatetime"] = record.time;
                dr["expiredate"] = record.expireDate;
                dr["basis"] = record.basis;
                dr["annulizedBasis"] = record.annulizedBasis;
                dr["basisWithSlip"] = record.basisWithSlip;
                dr["annulizedBasisWithSlip"] = record.annulizedBasisWithSlip;
                dt3.Rows.Add(dr);
            }
            if (!ExistInSqlServer3(code, parameters))
            {
                CreateDBOrTableIfNecessary3(code, parameters);
            }
            SaveResultToMssql3(dt3, code, parameters);


        }

        /// <summary>
        /// 计算年化基差
        /// </summary>
        /// <param name="future"></param>
        /// <param name="today"></param>
        /// <param name="futurePrice"></param>
        /// <param name="indexPrice"></param>
        /// <returns></returns>
        private double getAnuualizedBasis(CFEFutures future,DateTime today,double futurePrice,double indexPrice)
        {
            double term = DateUtils.GetSpanOfTradeDays(today, future.expireDate) / 252.0;
            return (futurePrice - indexPrice) / term;
        }

        private double getAnuualizedBasis2(double basis,DateTime startTime,DateTime endTime)
        {
            double term = DateUtils.GetSpanOfTradeDays(startTime, endTime)/252.0;
            return basis / term;
        }

        /// <summary>
        /// 计算损益
        /// </summary>
        /// <param name="list"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        private double getPnL(List<CFEFuturesTradeRecord> list)
        {
            if (list.Count() % 2 == 0)
            {
                Console.WriteLine("计算异常！交易记录偶数条！");
                throw new Exception("计算异常！交易记录偶数条！");
            }
            double pnl = 0;
            var lastRecord = list[list.Count() - 1];
            
            for (int i = 0; i < list.Count(); i++)
            {
                pnl += -(list[i].price-list[i].indexPrice) * list[i].direction;
            }
            pnl += -Math.Floor((list.Count() / 2.0)) * slip;
            var startDate = list[0].time;
            var endDate = lastRecord.expireDate;
            pnl = pnl/ DateUtils.GetSpanOfTradeDays(startDate,endDate) *252.0;
            return pnl;
        }

        /// <summary>
        /// 计算净值曲线
        /// </summary>
        /// <param name="list"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        private List<basisNetValue> getNetValue(List<CFEFuturesTradeRecord> list,List<DateTime> tradedays)
        {
            var netValue = new List<basisNetValue>();
            var netValueAnswer = new List<basisNetValue>();
            if (list.Count() % 2 == 0)
            {
                Console.WriteLine("计算异常！交易记录偶数条！");
                throw new Exception("计算异常！交易记录偶数条！");
            }
            double pnl = -(list[0].price - list[0].indexPrice) * list[0].direction;
            var netValueNow0 = new basisNetValue();
            netValueNow0.code = list[0].code;
            netValueNow0.time = list[0].time.Date;
            netValueNow0.basis = pnl;
            netValueNow0.basisWithSlip = pnl;
            netValueNow0.expireDate = list[0].expireDate;
            var startTime = list[0].time;
            var endTime = list[0].expireDate;
            netValueNow0.annulizedBasis = pnl / DateUtils.GetSpanOfTradeDays(startTime, endTime) * 252.0;
            netValueNow0.annulizedBasisWithSlip = netValueNow0.basisWithSlip / DateUtils.GetSpanOfTradeDays(startTime, endTime) * 252.0;
            netValue.Add(netValueNow0);
            for (int i = 1; i < list.Count(); i=i+2)
            {
                var netValueNow = new basisNetValue();
                pnl += -(list[i].price - list[i].indexPrice) * list[i].direction;
                pnl += -(list[i+1].price - list[i+1].indexPrice) * list[i+1].direction;
                netValueNow.code = list[i + 1].code;
                netValueNow.time = list[i + 1].time.Date;
                netValueNow.basis = pnl;
                netValueNow.basisWithSlip=pnl-Math.Floor(((i+1) / 2.0)) * slip;
                netValueNow.expireDate = list[i + 1].expireDate;
                startTime = list[0].time;
                endTime = list[i+1].expireDate;
                netValueNow.annulizedBasis = pnl / DateUtils.GetSpanOfTradeDays(startTime, endTime) * 252.0;
                netValueNow.annulizedBasisWithSlip = netValueNow.basisWithSlip / DateUtils.GetSpanOfTradeDays(startTime, endTime) * 252.0;
                netValue.Add(netValueNow);
            }
            int num = 0;
            for (int i = 0; i < tradedays.Count(); i++)
            {
                basisNetValue netValue0 = new basisNetValue();
                for (int j = num; j < netValue.Count(); j++)
                {
                    if (netValue[j].time.Date<=tradedays[i].Date)
                    {
                        netValue0.code = netValue[j].code;
                        netValue0.time = tradedays[i].Date;
                        netValue0.expireDate = netValue[j].expireDate;
                        netValue0.basis = netValue[j].basis;
                        netValue0.annulizedBasis = netValue[j].annulizedBasis;
                        netValue0.basisWithSlip = netValue[j].basisWithSlip;
                        netValue0.annulizedBasisWithSlip = netValue[j].annulizedBasisWithSlip;
                    }
                    else
                    {
                        num =Math.Max(j-1,0);
                        break;
                    }
                }
                netValueAnswer.Add(netValue0);
            }
            return netValueAnswer;
        }

        /// <summary>
        /// 根据计算规则获取当日交易日的股指期货列表
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        private Dictionary<string, CFEFutures> getFutureList(DateTime date)
        {
            var number = 3; //3为选择3个合约，4为选择4个合约
            List<CFEFutures> list = new List<CFEFutures>();
            List<DateTime> dateList = new List<DateTime>();
            Dictionary<string, CFEFutures> dic = new Dictionary<string, CFEFutures>();
            var expireDateOfThisMonth = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
            if (date>expireDateOfThisMonth)
            {
                date = DateUtils.GetFirstDateOfNextMonth(date);
            }
            dateList.Add(date);
            var date2 = DateUtils.GetFirstDateOfNextMonth(date);
            dateList.Add(date2);
            var date3 = DateUtils.GetLastDateOfThisSeason(date2);
            if (date3.Month==date2.Month)
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
                string month = "0"+date.Month.ToString();
                month = month.Substring(month.Length - 2, 2);
                future.code = code + year + month + ".CFE";
                future.expireDate= DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
                list.Add(future);
            }
            foreach (var item in list)
            {
                dic.Add(item.code, item);
            }         
            return dic;
        }

        private void SaveResultToMssql(DataTable dt, string indexCode,string parameters)
        {
            var sql = string.Format(@"delete from [Basis{0}].[dbo].[{1}]", indexCode,parameters);
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[Basis{0}].[dbo].[{1}]", indexCode,parameters));
        }

        private void SaveResultToMssql2(DataTable dt, string indexCode, string parameters)
        {
            var sql = string.Format(@"delete from [BasisResult{0}].[dbo].[{1}]", indexCode, parameters);
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[BasisResult{0}].[dbo].[{1}]", indexCode, parameters));
        }

        private void SaveResultToMssql3(DataTable dt, string indexCode, string parameters)
        {
            var sql = string.Format(@"delete from [BasisNetValue{0}].[dbo].[{1}]", indexCode, parameters);
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[BasisNetValue{0}].[dbo].[{1}]", indexCode, parameters));
        }

        private void CreateDBOrTableIfNecessary(string indexCode,string parameters)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('Basis{0}') is null
begin
CREATE DATABASE [Basis{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'Basis{0}', FILENAME = N'{2}\Basis{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'Basis{0}_log', FILENAME = N'{2}\Basis{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [Basis{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [Basis{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[Basis{0}].dbo.[{1}]') is null
begin
CREATE TABLE [Basis{0}].[dbo].[{1}](
    [code] varchar(20)  NOT NULL,
	[tdatetime] [datetime] NOT NULL,
    [expiredate] [datetime] NOT NULL,
    [indexPrice] [decimal](12, 4) NULL,
    [price] [decimal](12, 4) NULL,
	[annulizedBasis] [decimal](12, 4) NULL,
    [LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [Basis{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [Basis{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", indexCode, parameters, fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private void CreateDBOrTableIfNecessary2(string indexCode, string parameters)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('BasisResult{0}') is null
begin
CREATE DATABASE [BasisResult{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'BasisResult{0}', FILENAME = N'{2}\BasisResult{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'BasisResult{0}_log', FILENAME = N'{2}\BasisResult{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [BasisResult{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [BasisResult{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[BasisResult{0}].dbo.[{1}]') is null
begin
CREATE TABLE [BasisResult{0}].[dbo].[{1}](
    [code] varchar(20)  NOT NULL,
	[tdatetime] [datetime] NOT NULL,
    [expiredate] [datetime] NOT NULL,
    [indexPrice] [decimal](12, 4) NULL,
    [price] [decimal](12, 4) NULL,
	[direction] [decimal](4, 0) NULL,
    [LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [BasisResult{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [BasisResult{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", indexCode, parameters, fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }


        private void CreateDBOrTableIfNecessary3(string indexCode, string parameters)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('BasisNetValue{0}') is null
begin
CREATE DATABASE [BasisNetValue{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'BasisNetValue{0}', FILENAME = N'{2}\BasisNetValue{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'BasisNetValue{0}_log', FILENAME = N'{2}\BasisNetValue{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [BasisNetValue{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [BasisNetValue{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[BasisNetValue{0}].dbo.[{1}]') is null
begin
CREATE TABLE [BasisNetValue{0}].[dbo].[{1}](
    [code] varchar(20)  NOT NULL,
	[tdatetime] [datetime] NOT NULL,
    [expiredate] [datetime] NOT NULL,
    [basis] [decimal](12, 4) NULL,
    [annulizedBasis] [decimal](12, 4) NULL,
    [basisWithSlip] [decimal](12, 4) NULL,
    [annulizedBasisWithSlip] [decimal](12, 4) NULL,
    [LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [BasisNetValue{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [BasisNetValue{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", indexCode, parameters, fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(string indexCode,string parameters)
        {
            var sqlScript = string.Format(@"use master
if db_id('Basis{0}') is not null
begin
	if object_id('[Basis{0}].dbo.[{1}]') is not null
	begin
		select 1 from [Basis{0}].dbo.[{1}] 
	end
end
else
begin
select 0
end ", indexCode,parameters);
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }


        private bool ExistInSqlServer2(string indexCode, string parameters)
        {
            var sqlScript = string.Format(@"use master
if db_id('BasisResult{0}') is not null
begin
	if object_id('[BasisResult{0}].dbo.[{1}]') is not null
	begin
		select 1 from [BasisResult{0}].dbo.[{1}] 
	end
end
else
begin
select 0
end ", indexCode, parameters);
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }

        private bool ExistInSqlServer3(string indexCode, string parameters)
        {
            var sqlScript = string.Format(@"use master
if db_id('BasisNetValue{0}') is not null
begin
	if object_id('[BasisNetValue{0}].dbo.[{1}]') is not null
	begin
		select 1 from [BasisNetValue{0}].dbo.[{1}] 
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
