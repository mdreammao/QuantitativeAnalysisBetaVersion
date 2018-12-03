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
using QuantitativeAnalysis.DataAccess.ETF;

namespace QuantitativeAnalysis.Monitor
{
    public class TDstrategy
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData;
        private StockMinuteRepository stockMinutelyRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private string databaseName = "reversal";
        private string tableName = "TD";
        private string tableName2 = "TD_Transction";
        

        public TDstrategy(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, string code)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            this.code = code;
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
            CreateDBOrTableIfNecessary(databaseName, tableName);
            CreateDBOrTableIfNecessary2(databaseName, tableName2);
        }

        public void compute(DateTime startDate, DateTime endDate,int delaynum=5,int startnum=3,int calculatornum=6)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var etfall= stockMinutelyRepo.GetStockTransaction(code, tradedays.First(),tradedays.Last());
            modifyData(ref etfall);
            var netvalue = new double[etfall.Count()];
            var signal = new double[etfall.Count()];
            var buyStart = new double[etfall.Count()];
            var buyCalculator = new double[etfall.Count()];
            var longSignal = new double[etfall.Count()];
            var sellStart = new double[etfall.Count()];
            var sellCalculator = new double[etfall.Count()];
            var shortSignal = new double[etfall.Count()];
            int startnumNow = 0;
            int calculatornumNow = 0;
         

            //买入启动
            startnumNow = 0;
            for (int i = delaynum; i < etfall.Count() - delaynum; i++)
            {
                
                if (etfall[i].Close < etfall[i - delaynum].Close)
                {
                    startnumNow += 1;
                }
                else
                {
                    startnumNow = 0;
                }
                if (startnumNow>=startnum)
                {
                    buyStart[i] = 1;
                }
            }
            //买入计数
            calculatornumNow = 0;
            double buyCalculatorStartPrice = 0;
            bool buyCalculatorStart = false;
            for (int i = delaynum; i < etfall.Count() - delaynum; i++)
            {
                if (buyStart[i-1]==1)
                {
                    buyCalculatorStart = true;
                    buyCalculatorStartPrice = 0;
                    calculatornumNow = 0;
                }
                if (buyCalculatorStart==true && etfall[i].Close>=etfall[i-2].High && etfall[i].High>etfall[i-1].High)  
                {
                    if (calculatornumNow>0 && etfall[i].Close>buyCalculatorStartPrice)
                    {
                        calculatornumNow += 1;
                        buyCalculator[i] = calculatornumNow;
                        if (calculatornumNow == calculatornum)
                        {
                            longSignal[i+1] = 1;
                            buyCalculatorStart = false;
                        }
                        buyCalculatorStartPrice = etfall[i].Close;
                    }
                    if (calculatornumNow==0)
                    {
                        calculatornumNow += 1;
                        buyCalculatorStartPrice = etfall[i].Close;
                    }
                }
                if (calculatornumNow>0)
                {
                    buyCalculator[i] = calculatornumNow;
                }
            }

            //卖出启动
            startnumNow = 0;
            for (int i =delaynum; i < etfall.Count() - delaynum; i++)
            {

                if (etfall[i].Close > etfall[i - delaynum].Close)
                {
                    startnumNow += 1;
                }
                else
                {
                    startnumNow = 0;
                }
                if (startnumNow >= startnum)
                {
                    sellStart[i] = 1;
                }
            }
            //卖出计数
            calculatornumNow = 0;
            double sellCalculatorStartPrice = 99999999;
            bool sellCalculatorStart = false;
            for (int i = delaynum; i < etfall.Count() - delaynum; i++)
            {
                if (sellStart[i-1] == 1)
                {
                    sellCalculatorStart = true;
                    sellCalculatorStartPrice = 99999999;
                    calculatornumNow = 0;
                }
                if (sellCalculatorStart ==true && etfall[i].Close <= etfall[i - 2].Low && etfall[i].Low< etfall[i - 1].Low)
                {
                    if (calculatornumNow > 0 && etfall[i].Close < sellCalculatorStartPrice)
                    {
                        calculatornumNow += 1;
                        if (calculatornumNow == calculatornum)
                        {
                            shortSignal[i+1] = -1;
                            sellCalculatorStart = false;
                        }
                        sellCalculatorStartPrice = etfall[i].Close;
                    }
                    if (calculatornumNow == 0)
                    {
                        calculatornumNow += 1;
                        sellCalculatorStartPrice = etfall[i].Close;
                    }
                }
                if (calculatornumNow > 0)
                {
                    sellCalculator[i] = calculatornumNow;
                }
            }
           // storeSignal(etfall, longSignal, shortSignal,delaynum,startnum,calculatornum);
            netvalue = computeNetValue(etfall, longSignal, shortSignal,ref transactionData);
            var nv = getNetValueDaily(etfall, netvalue);
            double sharpe = Utilities.strategyPerformance.sharpeRatioByDailyNetValue(nv);
            storeTransctionData(transactionData, delaynum, startnum, calculatornum);
            statisticDataOfTransaction(transactionData,tradedays);
          }


        private void statisticDataOfTransaction(List<OneByOneTransaction> data,List<DateTime> tradeDays)
        {
            Dictionary<DateTime, double> dailyCount = new Dictionary<DateTime, double>();
            List<double> yield = new List<double>();
            List<double> maintain = new List<double>();
            foreach (var day in tradeDays)
            {
                dailyCount.Add(day.Date, 0);
            }
            foreach (var item in data)
            {
                var today = item.openTime.Date;
                if (dailyCount.ContainsKey(today))
                {
                    dailyCount[today] += 1;
                }
                else
                {
                    dailyCount.Add(today, 1);
                }
                double r = (item.closePrice-item.openPrice)/item.openPrice*item.position;
                yield.Add(r);
                double time = (item.closeTime - item.openTime).TotalMinutes;
                if (item.openTime.TimeOfDay<new TimeSpan(13,00,00) && item.closeTime.TimeOfDay >= new TimeSpan(13, 00, 00))
                {
                    time = time - 90;
                }
                maintain.Add(time);
            }
            var counts = dailyCount.Values.ToList();
            ListToCSV.SaveDataToCSVFile<double>(yield, ListToCSV.CreateFile("E:\\result\\td\\", "yield"), "yield");
            ListToCSV.SaveDataToCSVFile<double>(counts, ListToCSV.CreateFile("E:\\result\\td\\", "counts"), "counts");
            ListToCSV.SaveDataToCSVFile<double>(maintain, ListToCSV.CreateFile("E:\\result\\td\\", "maintain"), "maintain");
            var dt = DataTableExtension.ToDataTable<OneByOneTransaction>(data);
            DataTableExtension.SaveCSV(dt, "E:\\result\\td\\transaction.csv");
        }

        private void modifyData(ref List<StockTransaction> data)
        {
            foreach (var item in data)
            {
                item.Close = Math.Round(item.Close, 3);
                item.High = Math.Round(item.High, 3);
                item.Low = Math.Round(item.Low, 3);
                item.Open = Math.Round(item.Open, 3);
            }
        }

        private void storeTransctionData(List<OneByOneTransaction> transactionData, int delaynum, int startnum, int calculatornum)
        {
            DataTable dt = new DataTable();
            dt = initializeDataTable2(dt);
            for (int i = 0; i < transactionData.Count(); i++)
            {
                DataRow dr = dt.NewRow();
                dr["opentime"] = transactionData[i].openTime;
                dr["closetime"] = transactionData[i].closeTime;
                dr["openprice"] = transactionData[i].openPrice;
                dr["closeprice"] = transactionData[i].closePrice;
                dr["code"] = code;
                dr["position"] = transactionData[i].position;
                dr["delaynum"] = delaynum;
                dr["startnum"] = startnum;
                dr["calculatornum"] = calculatornum;
                dt.Rows.Add(dr);
            }
            DateTime startTime = transactionData[0].openTime;
            DateTime endTime = transactionData[transactionData.Count() - 1].closeTime;
            SaveResultToMssql2(databaseName, tableName2, dt, startTime, endTime, delaynum, startnum, calculatornum, code);
        }


        private void storeSignal(List<StockTransaction> etf,double[] longSignal,double[] shortSignal, int delaynum, int startnum, int calculatornum)
        {
            DataTable dt = new DataTable();
            dt = initializeDataTable(dt);
            for (int i = 0; i < etf.Count(); i++)
            {
                DataRow dr = dt.NewRow();
                dr["tdatetime"] = etf[i].DateTime;
                dr["code"] = etf[i].Code;
                dr["open"] = etf[i].Open;
                dr["high"] = etf[i].High;
                dr["low"] = etf[i].Low;
                dr["close"] = etf[i].Close;
                dr["delaynum"] = delaynum;
                dr["startnum"] = startnum;
                dr["calculatornum"] = calculatornum;
                dr["longsignal"] = longSignal[i];
                dr["shortsignal"] = shortSignal[i];
                dt.Rows.Add(dr);
            }
            DateTime startTime = etf[0].DateTime;
            DateTime endTime = etf[etf.Count() - 1].DateTime;
            SaveResultToMssql(databaseName,tableName,dt,startTime,endTime,delaynum,startnum,calculatornum,code);
        }


        private double[] computeNetValue(List<StockTransaction> etf, double[] longSignal, double[] shortSignal,ref List<OneByOneTransaction> data)
        {
            double[] netvalue = new double[etf.Count()];
            data = new List<OneByOneTransaction>();
            OneByOneTransaction transaction = new OneByOneTransaction();
            double nv = 0;
            double position = 0;
            double cash = 1;
            double stopPrice = 0;
            double shortStopPrice = 0;
            double stopRatio = 0.95;
            double shortStopRatio = 1.05;
            double slipRatio = 0.0002;
            double count = 0;
            for (int i = 0; i < etf.Count(); i++)
            {
                DateTime time = etf[i].DateTime;
                double stockPrice = etf[i].Close;
                if (time.TimeOfDay > new TimeSpan(14, 55, 00) && position!=0) //超过14点55分有仓位，强制平仓
                {
                    cash = cash + (stockPrice * position)-Math.Abs(stockPrice * position*slipRatio);
                    position = 0;
                    stopPrice = 0;
                    shortStopPrice = 0;
                    transaction.closeTime = time;
                    transaction.closePrice = stockPrice;
                    data.Add(transaction);
                    transaction = new OneByOneTransaction();
                }
                if (position==0  && time.TimeOfDay < new TimeSpan(14, 45, 00) && time.TimeOfDay > new TimeSpan(10, 00, 00)) //未开仓
                {
                    if (longSignal[i]==1) //开多头
                    {
                        position = cash*(1-slipRatio) / stockPrice;
                        cash = 0;
                        stopPrice = stockPrice * stopRatio;
                        count++;
                        transaction = new OneByOneTransaction();
                        transaction.openTime = time;
                        transaction.openPrice = stockPrice;
                        transaction.position = 1;
                    }
                    else if (shortSignal[i]==-1)//开空头
                    {
                        position = -cash*(1-slipRatio) / stockPrice;
                        cash = cash-position* stockPrice;
                        shortStopPrice = stockPrice * shortStopRatio;
                        count++;
                        transaction = new OneByOneTransaction();
                        transaction.openTime = time;
                        transaction.openPrice = stockPrice;
                        transaction.position = -1;
                    }

                }
                else //已开仓
                {
                    if (position>0) //已开多仓
                    {
                        //记录追踪止损的点位
                        if (stopPrice < stockPrice * stopRatio)
                        {
                            stopPrice = stockPrice * stopRatio;
                        }
                        if (shortSignal[i] == -1 || stockPrice < stopPrice) //平仓或者止损
                        {
                            cash =cash+ stockPrice * position-Math.Abs(stockPrice * position*slipRatio);
                            position = 0;
                            stopPrice = 0;
                            transaction.closeTime = time;
                            transaction.closePrice = stockPrice;
                            data.Add(transaction);
                            transaction = new OneByOneTransaction();
                        }
                    }
                    else if (position<0) //已开空仓
                    {
                        //记录追踪止损的点位
                        if (shortStopPrice > stockPrice * shortStopRatio)
                        {
                            shortStopPrice = stockPrice * shortStopRatio;
                            
                        }
                        if (longSignal[i] == 1 || stockPrice > shortStopPrice) //平仓或者止损
                        {
                            cash =cash + stockPrice * position-Math.Abs(stockPrice * position*slipRatio);
                            position = 0;
                            shortStopPrice = 0;
                            transaction.closeTime = time;
                            transaction.closePrice = stockPrice;
                            data.Add(transaction);
                            transaction = new OneByOneTransaction();
                        }
                    }
                }
                nv = cash + position * etf[i].Close;
                netvalue[i] = nv;
            }
             return netvalue;
        }

        private List<double> getNetValueDaily(List<StockTransaction> etf,double[] netvalue)
        {
            List<double> nv = new List<double>();
            List<netvalueDaily> nv2 = new List<netvalueDaily>();
            for (int i = 0; i < netvalue.Length; i++)
            {
                if (etf[i].DateTime.TimeOfDay==new TimeSpan(14,59,00))
                {
                    nv.Add(netvalue[i]);
                    var nvtoday = new netvalueDaily();
                    nvtoday.date = etf[i].DateTime.Date;
                    nvtoday.netvalue = netvalue[i];
                    nv2.Add(nvtoday);
                }
            }
            DataTableExtension.SaveCSV(DataTableExtension.ToDataTable<netvalueDaily>(nv2), "E:\\result\\td\\netvalue.csv");
            return nv;
        }

        //保存信号数据
        private DataTable initializeDataTable(DataTable dt)
        {
            dt.Columns.Add("tdatetime");
            dt.Columns.Add("code");
            dt.Columns.Add("open");
            dt.Columns.Add("high");
            dt.Columns.Add("low");
            dt.Columns.Add("close");
            dt.Columns.Add("delaynum");
            dt.Columns.Add("startnum");
            dt.Columns.Add("calculatornum");
            dt.Columns.Add("longsignal");
            dt.Columns.Add("shortsignal");
            return dt;
        }

        //保存逐笔数据
        private DataTable initializeDataTable2(DataTable dt)
        {
            dt.Columns.Add("opentime");
            dt.Columns.Add("closetime");
            dt.Columns.Add("code");
            dt.Columns.Add("position");
            dt.Columns.Add("openprice");
            dt.Columns.Add("closeprice");
            dt.Columns.Add("delaynum");
            dt.Columns.Add("startnum");
            dt.Columns.Add("calculatornum");
            return dt;
        }
        
        //保存逐笔交易数据
        private void SaveResultToMssql2(string databaseName, string tableName,DataTable dt,DateTime startTime,DateTime endTime, int delaynum, int startnum, int calculatornum,string code)
        {
            var sql = string.Format(@"delete from [{0}].[dbo].[{1}] where opentime>='{2}' and closetime<='{3}' and delaynum='{4}' and startnum='{5}' and calculatornum='{6}' and code='{7}'",databaseName,tableName,startTime.ToString("yyyy-MM-dd"), endTime.ToString("yyyy-MM-dd"),delaynum,startnum,calculatornum,code);
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[{0}].[dbo].[{1}]", databaseName,tableName));
        }

        //保存信号数据
        private void SaveResultToMssql(string databaseName, string tableName, DataTable dt, DateTime startTime, DateTime endTime, int delaynum, int startnum, int calculatornum, string code)
        {
            var sql = string.Format(@"delete from [{0}].[dbo].[{1}] where tdatetime>='{2}' and tdatetime<='{3}' and delaynum='{4}' and startnum='{5}' and calculatornum='{6}' and code='{7}'", databaseName, tableName, startTime.ToString("yyyy-MM-dd"), endTime.ToString("yyyy-MM-dd"), delaynum, startnum, calculatornum, code);
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[{0}].[dbo].[{1}]", databaseName, tableName));
        }

        private void CreateDBOrTableIfNecessary(string databaseName,string tableName)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('{0}') is null
begin
CREATE DATABASE [{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'{0}', FILENAME = N'{2}\{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'{0}_log', FILENAME = N'{2}\{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[{0}].dbo.[{1}]') is null
begin
CREATE TABLE [{0}].[dbo].[{1}](
	[tdatetime] [datetime] NOT NULL,
    [code] varchar(20)  NOT NULL,
    [open] [decimal](10, 4) NULL,
    [high] [decimal](10, 4) NULL,
    [low] [decimal](10, 4) NULL,
    [close] [decimal](10, 4) NULL,
    [delaynum] [decimal](4, 0) NULL,
    [startnum] [decimal](4, 0) NULL,
    [calculatornum] [decimal](4, 0) NULL,
    [longsignal] [decimal](4, 0) NULL,
    [shortsignal] [decimal](4, 0) NULL,
	[LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", databaseName, tableName, fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        //保存逐笔开平仓数据的代码
        private void CreateDBOrTableIfNecessary2(string databaseName, string tableName)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('{0}') is null
begin
CREATE DATABASE [{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'{0}', FILENAME = N'{2}\{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'{0}_log', FILENAME = N'{2}\{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[{0}].dbo.[{1}]') is null
begin
CREATE TABLE [{0}].[dbo].[{1}](
	[opentime] [datetime] NOT NULL,
    [closetime] [datetime] NOT NULL,
    [code] varchar(20)  NOT NULL,
    [openprice] [decimal](10, 4) NULL,
    [closeprice] [decimal](10, 4) NULL,
    [position] [decimal](4, 0) NULL,
    [delaynum] [decimal](4, 0) NULL,
    [startnum] [decimal](4, 0) NULL,
    [calculatornum] [decimal](4, 0) NULL,
	[LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [{0}].[dbo].[{1}]
(
	[opentime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", databaseName, tableName, fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(string databaseName, string tableName)
        {
            var sqlScript = string.Format(@"use master
if db_id('{0}') is not null
begin
	if object_id('[{0}].dbo.[{1}]') is not null
	begin
		select 1 from [{0}].dbo.[{1}] 
	end
end
else
begin
select 0
end ", databaseName,tableName);
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        } 

    }
}
