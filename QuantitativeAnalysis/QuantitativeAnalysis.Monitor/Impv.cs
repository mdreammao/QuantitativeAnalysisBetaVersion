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
    public class Impv
    {
        private double rate;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string underlying = "510050.SH";
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private OptionInfoRepository infoRepo;
        private StockOptionTickRepository optionRepo;
        private StockTickRepository stockRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private double volumeTarget = 10;
        private double emaCoeff = 0.03;
        private List<TimeSpan> timelist = DataTimeStampExtension.GetStockTickStamp();

        private double getOptionMidPrice(StockOptionTickTransaction tick, double volumeTarget = 10)
        {
            double mid = 0;
            if (tick == null || tick.AskV1 == 0 || tick.BidV1 == 0 || tick.Ask1 == 0 || tick.Bid1 == 0)
            {
                return mid;
            }
            var askv1 = Math.Min(volumeTarget, tick.AskV1);
            var askv2 =Math.Max(Math.Min(volumeTarget - askv1, tick.AskV2),0);
            var askv3 = Math.Max(Math.Min(volumeTarget - askv1-askv2, tick.AskV3), 0);
            var askv4 = Math.Max(Math.Min(volumeTarget - askv1 - askv2-askv3, tick.AskV4), 0);
            var askv5 = Math.Max(Math.Min(volumeTarget - askv1 - askv2 - askv3-askv4, tick.AskV5), 0);
            var bidv1 = Math.Min(volumeTarget, tick.BidV1);
            var bidv2 = Math.Max(Math.Min(volumeTarget - bidv1, tick.BidV2), 0);
            var bidv3 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2, tick.BidV3), 0);
            var bidv4 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2 - bidv3, tick.BidV4), 0);
            var bidv5 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2 - bidv3 - bidv4, tick.BidV5), 0);
            var askvtotal = askv1 + askv2 + askv3 + askv4 + askv5;
            var bidvtotal = bidv1 + bidv2 + bidv3 + bidv4 + bidv5;
            var position= (askv1 * tick.Ask1 + askv2 * tick.Ask2 + askv3 * tick.Ask3 + askv4 * tick.Ask4 + askv5 * tick.Ask5 + bidv1 * tick.Bid1 + bidv2 * tick.Bid2 + bidv3 * tick.Bid3 + bidv4 * tick.Bid4 + bidv5 * tick.Bid5) / (askvtotal + bidvtotal);
            var x = (position - (tick.Ask1 + tick.Bid1) / 2)/ ((tick.Ask1 - tick.Bid1) / 2);
            if (x>0)
            {
                mid = (x) / (0.1 + x) * ((tick.Ask1 - tick.Bid1) / 2) + (tick.Ask1 + tick.Bid1) / 2;
            }
            else
            {
                mid= (x) / (0.1 - x) * ((tick.Ask1 - tick.Bid1) / 2) + (tick.Ask1 + tick.Bid1) / 2;
            }
            return mid;

        }

        private double getOptionSpread(StockOptionTickTransaction tick, double volumeTarget = 10)
        {
            double mid = tick.Ask1-tick.Bid1;
            var askv1 = Math.Min(volumeTarget, tick.AskV1);
            var askv2 = Math.Max(Math.Min(volumeTarget - askv1, tick.AskV2), 0);
            var askv3 = Math.Max(Math.Min(volumeTarget - askv1 - askv2, tick.AskV3), 0);
            var askv4 = Math.Max(Math.Min(volumeTarget - askv1 - askv2 - askv3, tick.AskV4), 0);
            var askv5 = Math.Max(Math.Min(volumeTarget - askv1 - askv2 - askv3 - askv4, tick.AskV5), 0);
            var bidv1 = Math.Min(volumeTarget, tick.BidV1);
            var bidv2 = Math.Max(Math.Min(volumeTarget - bidv1, tick.BidV2), 0);
            var bidv3 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2, tick.BidV3), 0);
            var bidv4 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2 - bidv3, tick.BidV4), 0);
            var bidv5 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2 - bidv3 - bidv4, tick.BidV5), 0);
            var askvtotal = askv1 + askv2 + askv3 + askv4 + askv5;
            var bidvtotal = bidv1 + bidv2 + bidv3 + bidv4 + bidv5;
            var askMean = (askv1 * tick.Ask1 + askv2 * tick.Ask2 + askv3 * tick.Ask3 + askv4 * tick.Ask4 + askv5 * tick.Ask5) / askvtotal;
            var bidMean = (bidv1 * tick.Bid1 + bidv2 * tick.Bid2 + bidv3 * tick.Bid3 + bidv4 * tick.Bid4 + bidv5 * tick.Bid5) / bidvtotal;
            mid = askMean - bidMean;
            return mid;

        }
        private double getStockMidPrice(StockTickTransaction tick,double volumeTarget=1000)
        {
            double mid = 0;
            if (tick == null || tick.AskV1 == 0 || tick.BidV1 == 0 || tick.Ask1==0 || tick.Bid1==0)
            {
                return mid;
            }
            var askv1 = Math.Min(volumeTarget, tick.AskV1);
            var askv2 = Math.Max(Math.Min(volumeTarget - askv1, tick.AskV2), 0);
            var askv3 = Math.Max(Math.Min(volumeTarget - askv1 - askv2, tick.AskV3), 0);
            var askv4 = Math.Max(Math.Min(volumeTarget - askv1 - askv2 - askv3, tick.AskV4), 0);
            var askv5 = Math.Max(Math.Min(volumeTarget - askv1 - askv2 - askv3 - askv4, tick.AskV5), 0);
            var bidv1 = Math.Min(volumeTarget, tick.BidV1);
            var bidv2 = Math.Max(Math.Min(volumeTarget - bidv1, tick.BidV2), 0);
            var bidv3 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2, tick.BidV3), 0);
            var bidv4 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2 - bidv3, tick.BidV4), 0);
            var bidv5 = Math.Max(Math.Min(volumeTarget - bidv1 - bidv2 - bidv3 - bidv4, tick.BidV5), 0);
            var askvtotal = askv1 + askv2 + askv3 + askv4 + askv5;
            var bidvtotal = bidv1 + bidv2 + bidv3 + bidv4 + bidv5;
            mid = (askv1 * tick.Ask1 + askv2 * tick.Ask2 + askv3 * tick.Ask3 + askv4 * tick.Ask4 + askv5 * tick.Ask5 + bidv1 * tick.Bid1 + bidv2 * tick.Bid2 + bidv3 * tick.Bid3 + bidv4 * tick.Bid4 + bidv5 * tick.Bid5) / (askvtotal + bidvtotal);
            return mid;
        }

        public Impv(OptionInfoRepository infoRepo, StockOptionTickRepository optionRepo, StockTickRepository stockRepo, double rate = 0.04)
        {
            this.infoRepo = infoRepo;
            this.optionRepo = optionRepo;
            this.stockRepo = stockRepo;
            this.rate = rate;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
        }

        public void computeImpv(DateTime startDate, DateTime endDate)
        {
            var start = startDate;
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
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //逐日进行计算
            foreach (var date in tradedays)
            {
                if (!ExistInSqlServer(date))
                {
                    CreateDBOrTableIfNecessary(date);
                }
                double[,] myFuture = new double[4,28802];
                var tickdata = new Dictionary<string, List<StockOptionTickTransaction>>();
                var etf= DataTimeStampExtension.ModifyStockTickData(stockRepo.GetStockTransaction("510050.SH", date, date.AddHours(17)));
                var list = infoRepo.GetStockOptionInfo(underlying, date, date).Where(x=>x.unit==10000);
                Dictionary<StockOptionProperty, string> optionCode = new Dictionary<StockOptionProperty, string>();
                //给出所有的strike信息
                List<double> strikeList = new List<double>();
                foreach (var item in list)
                {
                    if (strikeList.Contains(item.strike)==false)
                    {
                        strikeList.Add(item.strike);
                    }
                }
                strikeList=strikeList.OrderBy(x => x).ToList();
                //给出所有的duration信息
                List<DateTime> expireDateList = new List<DateTime>();
                foreach (var item in list)
                {
                    if (expireDateList.Contains(item.expireDate)==false)
                    {
                        expireDateList.Add(item.expireDate);
                    }
                }
                expireDateList=expireDateList.OrderBy(x => x).ToList();
                foreach (var item in list)
                {
                    var option0 = optionRepo.GetStockTransaction(item.code, date, date.AddHours(17));
                    if (option0.Count == 0)
                    {
                        continue;
                    }
                    var option = DataTimeStampExtension.ModifyOptionTickData(option0);
                    StockOptionProperty property = new StockOptionProperty { strike = item.strike, call_or_put = item.type, expireDate = item.expireDate };
                    optionCode.Add(property, item.code);
                    tickdata.Add(item.code, option);
                }
                //计算合约的合成远期价格

                for (int k = 0; k < 3; k++) //k遍历了到期时间
                {
                    double[,] futures = new double[4,28802];//futures[选取的strike,时间下标]
                    double[,] weights = new double[4,28802];
                    for (int i = 0; i < 28802; i++)
                    {
                        var etfMid = getStockMidPrice(etf[i], volumeTarget * 100);
                        if (etfMid == 0)
                        {
                            continue;
                        }
                        var expireDate = expireDateList[k];
                        var strikeListNow = strikeList.OrderBy(x => Math.Abs(x - etfMid * Math.Exp(rate * dateRepo.GetDuration(date, expireDate)/252.0))).ToList();

                        for (int j = 0; j <= 3; j++)
                        {

                            StockOptionProperty call = new StockOptionProperty { strike = strikeListNow[j], call_or_put = "认购", expireDate = expireDate };
                            StockOptionProperty put = new StockOptionProperty { strike = strikeListNow[j], call_or_put = "认沽", expireDate = expireDate };
                            bool callExists = false, putExists = false;
                            foreach (var key in optionCode.Keys)
                            {
                                if (key.call_or_put == call.call_or_put && key.strike == call.strike && key.expireDate == call.expireDate)
                                {
                                    callExists = true;
                                    call = key;
                                }
                                if (key.call_or_put == put.call_or_put && key.strike == put.strike && key.expireDate == put.expireDate)
                                {
                                    putExists = true;
                                    put = key;
                                }
                            }
                            if (callExists && putExists)
                            {
                                var callTick = tickdata[optionCode[call]];
                                var putTick = tickdata[optionCode[put]];
                                var callMid = getOptionMidPrice(callTick[i], volumeTarget);
                                var putMid = getOptionMidPrice(putTick[i], volumeTarget);
                                if (callMid > 0 && putMid > 0)
                                {
                                    var callSpread = getOptionSpread(callTick[i], volumeTarget);
                                    var putSpread = getOptionSpread(putTick[i], volumeTarget);
                                    futures[j, i] = (callMid - putMid) * Math.Exp(rate * dateRepo.GetDuration(date, expireDate)/252.0) + strikeListNow[j];
                                    weights[j, i] = 1 / ((Math.Pow(callSpread, 2) + Math.Pow(putSpread, 2)) / 2);
                                }
                            }
                        }
                        myFuture[k, i] = 0;
                        double weightsAll = 0;
                        for (int j = 0; j < 3; j++)
                        {
                            myFuture[k, i] += futures[j, i] * weights[j, i];
                            weightsAll += weights[j, i];
                        }
                        if (weightsAll!=0)
                        {
                            myFuture[k, i] /= weightsAll;
                        }
                    }
                    int firstNonZero = 0;
                    for (int i = 0; i < 28802; i++)
                    {
                        if (myFuture[k,i]!=0)
                        {
                            firstNonZero = i;
                            break;
                        }
                    }
                    for (int i = firstNonZero+1; i < 28802; i++)
                    {
                        if (myFuture[k,i]==0)
                        {
                            myFuture[k, i] = myFuture[k, i - 1];
                        }
                    }
                    for (int i = firstNonZero+1; i < 28802; i++)
                    {
                       myFuture[k, i] = emaCoeff * myFuture[k, i] + (1 - emaCoeff) * myFuture[k, i - 1];
                    }
                    //计算隐含波动率

                    foreach (var item in list)
                    {
                        if (item.expireDate!= expireDateList[k])
                        {
                            continue;
                        }
                        DataTable dt = new DataTable();
                        dt.Columns.Add("code");
                        dt.Columns.Add("tdatetime",typeof(DateTime));
                        dt.Columns.Add("expiredate");
                        dt.Columns.Add("futurePrice");
                        dt.Columns.Add("futurePrice0");
                        dt.Columns.Add("duration");
                        dt.Columns.Add("maturitydate");
                        dt.Columns.Add("etfPrice");
                        dt.Columns.Add("strike");
                        dt.Columns.Add("call_or_put");
                        dt.Columns.Add("ask");
                        dt.Columns.Add("bid");
                        dt.Columns.Add("ask_impv");
                        dt.Columns.Add("bid_impv");
                        StockOptionProperty option = new StockOptionProperty { strike = item.strike, call_or_put = item.type, expireDate = item.expireDate };
                        foreach (var key in optionCode.Keys)
                        {
                            if (key.call_or_put == option.call_or_put && key.strike == option.strike && key.expireDate == option.expireDate)
                            {
                                option = key;
                            }
                        }
                        if (optionCode.ContainsKey(option) == true)
                        {
                            for (int i = 0; i < 28802; i++)
                            {
                                if (myFuture[k, i] == 0)
                                {
                                    continue;
                                }
                                var etfMid = getStockMidPrice(etf[i], volumeTarget * 100);
                                if (etfMid == 0)
                                {
                                    continue;
                                }
                                var strikeListNow = strikeList.OrderBy(x => Math.Abs(x - etfMid * Math.Exp(rate * dateRepo.GetDuration(date, item.expireDate) / 252.0))).ToList();
                                var optionTick = tickdata[optionCode[option]];
                                if (optionTick[i] == null)
                                {
                                    continue;
                                }
                                double etfprice = etf[i].LastPrice;
                                double ask = optionTick[i].Ask1;
                                double bid = optionTick[i].Bid1;
                                double duration = dateRepo.GetDuration(date, option.expireDate) / 252.0;
                                double strike = item.strike;
                                string callorput = item.type;
                                double askvol = Math.Round(ImpliedVolatilityExtension.sigmaByFuture(myFuture[k, i], ask, strike, duration, rate, callorput), 4);
                                double bidvol = Math.Round(ImpliedVolatilityExtension.sigmaByFuture(myFuture[k, i], bid, strike, duration, rate, callorput), 4);
                                double future0 = 0;
                                for (int m = 0; m <= 3; m++)
                                {
                                    if (strikeListNow[m] == item.strike)
                                    {
                                        future0 = futures[m, i];
                                        break;
                                    }
                                }
                                DataRow dr = dt.NewRow();
                                dr["code"] = item.code;
                                dr["tdatetime"] =Convert.ToDateTime(date + timelist[i]);//etf[i].TransactionDateTime;
                                dr["maturitydate"] = item.expireDate;
                                dr["futurePrice"] = Math.Round(myFuture[k, i], 4);
                                dr["futurePrice0"] = Math.Round(future0, 4);
                                dr["strike"] = Math.Round(strike, 4);
                                dr["expiredate"] = dateRepo.GetDuration(date, option.expireDate);
                                dr["duration"] = Math.Round(duration, 5);
                                dr["etfPrice"] = etfprice;
                                dr["call_or_put"] = item.type;
                                dr["ask"] = ask;
                                dr["bid"] = bid;
                                if (askvol > 0 && askvol < 3)
                                {
                                    dr["ask_impv"] = askvol;
                                }
                                else
                                {
                                    dr["ask_impv"] = null;
                                }
                                if (bidvol > 0 && bidvol < 3)
                                {
                                    dr["bid_impv"] = bidvol;
                                }
                                else
                                {
                                    dr["bid_impv"] = null;
                                }
                                if (optionTick[i].TransactionDateTime < date.Date + new TimeSpan(14, 57, 00))
                                {
                                    dt.Rows.Add(dr);
                                }
                            }
                        }
                            
                        SaveResultToMssql(date, dt, item.strike,dateRepo.GetDuration(date,item.expireDate), item.type,item.code);
                    }
                }
            }
        }

        private void SaveResultToMssql(DateTime date, DataTable dt, double strike, int expiredate,string type,string code)
        {
            //var sql = string.Format(@"delete from [Impv{0}].[dbo].[{1}] where tdatetime>'{2}' and tdatetime<'{3}' and strike='{4}' and expiredate='{5}' and call_or_put='{6}'", date.Year, date.ToString("yyyy-MM-dd"), date.ToString("yyyy-MM-dd"), date.AddDays(1).ToString("yyyy-MM-dd"), strike, expiredate,type);
            var sql = string.Format(@"delete from [Impv{0}].[dbo].[{1}] where tdatetime>'{2}' and tdatetime<'{3}' and code='{4}'", date.Year, date.ToString("yyyy-MM-dd"), date.ToString("yyyy-MM-dd"), date.AddDays(1).ToString("yyyy-MM-dd"),code );
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[Impv{0}].[dbo].[{1}]", date.Year, date.ToString("yyyy-MM-dd")));
        }

        private void CreateDBOrTableIfNecessary(DateTime date)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('Impv{0}') is null
begin
CREATE DATABASE [Impv{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'Impv{0}', FILENAME = N'{2}\Impv{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'Impv{0}_log', FILENAME = N'{2}\Impv{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [Impv{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [Impv{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[Impv{0}].dbo.[{1}]') is null
begin
CREATE TABLE [Impv{0}].[dbo].[{1}](
    [code] varchar(20)  NOT NULL,
	[tdatetime] [datetime] NOT NULL,
    [strike] [decimal](8, 4) NULL,
    [call_or_put] [nvarchar](10) NULL,
	[expiredate] [decimal](10, 0) NULL,
    [duration] [decimal] (10,6) NULL,
    [maturitydate] [datetime] NOT NULL,
	[etfPrice] [decimal](12, 4) NULL,
    [futurePrice] [decimal](12, 4) NULL,
    [futurePrice0] [decimal](12, 4) NULL,
    [ask] [decimal](12, 4) NULL,
    [bid] [decimal](12, 4) NULL,
    [ask_impv] [decimal](12, 4) NULL,
	[bid_impv] [decimal](12, 4) NULL,
	[LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [Impv{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}] ON [Impv{0}].[dbo].[{1}]
(
	[strike] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [Impv{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", date.Year, date.ToString("yyyy-MM-dd"), fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(DateTime date)
        {
            var sqlScript = string.Format(@"use master
if db_id('Impv{0}') is not null
begin
	if object_id('[Impv{0}].dbo.[{1}]') is not null
	begin
		select 1 from [Impv{0}].dbo.[{1}] 
	end
end
else
begin
select 0
end ", date.Year, date.ToString("yyyy-MM-dd"));
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }

    }
}
