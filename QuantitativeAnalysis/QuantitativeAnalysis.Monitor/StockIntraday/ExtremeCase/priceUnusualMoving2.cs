/* 
 * 股票大涨8%或者大跌8%之后，做反转
 */
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

namespace QuantitativeAnalysis.Monitor.StockIntraday.ExtremeCase
{
    public class priceUnusualMoving2
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private WindReader windReader;
        private List<DateTime> tradedays = new List<DateTime>();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<GrabCeilingTransaction> transactionData = new List<GrabCeilingTransaction>();
        private StockMinuteRepository stockMinutelyRepo;
        private StockTickRepository stockTickRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private string index;
        private StockInfoRepository stockInfoRepo;
        private List<StockTransaction> underlyingAll = new List<StockTransaction>();
        private double slipRatio = 0.001;
        private double feeRatioBuy = 0.0001;
        private double feeRatioSell = 0.0011;
        private double priceUnit = 0.01;
        private double loss = 0.015;
        //记录股票组合信息
        Dictionary<string, StockIPOInfo> allStockDic = new Dictionary<string, StockIPOInfo>();
        //分钟线数据
        Dictionary<string, Dictionary<DateTime, List<StockTransaction>>> minutelyKLine = new Dictionary<string, Dictionary<DateTime, List<StockTransaction>>>();
        //日线数据
        Dictionary<string, List<StockTransaction>> DailyKLine = new Dictionary<string, List<StockTransaction>>();
        //快照数据
        Dictionary<string, Dictionary<DateTime, List<StockTickTransaction>>> tick = new Dictionary<string, Dictionary<DateTime, List<StockTickTransaction>>>();


        //构造函数
        public priceUnusualMoving2(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, StockTickRepository stockTickRepo, StockInfoRepository stockInfoRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.stockTickRepo = stockTickRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
            this.windReader = new WindReader();
            this.stockInfoRepo = stockInfoRepo;
        }

        //获取指数成分股列表
        public void backtestByIndexCode(string index, DateTime startDate, DateTime endDate)
        {
            this.index = index;
            dataPrepareAll(index, startDate, endDate);
            allStockDic = getStockInfoList(index, endDate, endDate);
            int num = 0;
            foreach (var item in allStockDic)
            {
                var stock = item.Value;
                DateTime stockStart = startDate;
                DateTime stockEnd = endDate;
                if (stockStart < stock.IPODate)
                {
                    stockStart = stock.IPODate.AddDays(10);
                }
                //backtestOpposite(stock.code, stockStart, stockEnd);
                backtestOpposite2(stock.code, stockStart, stockEnd);
                num += 1;
                Console.WriteLine("完成 {0} of {1}!", num, allStockDic.Count());
            }
            //存入交易信息
            var dt = DataTableExtension.ToDataTable(transactionData);
            var codeStr = index.Split('.');
            var now = DateTime.Now.ToString("yyyyMMddhhmm");
            string name = string.Format("E:\\result\\grabCeiling\\{0}_{1}.csv", codeStr[0], now);
            DataTableExtension.SaveCSV(dt, name);
        }


        /// <summary>
        /// 大涨之后做反转
        /// </summary>
        /// <param name="underlyingCode"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="parameter"></param>
        public void backtestOpposite(string underlyingCode, DateTime startDate, DateTime endDate, double parameter = 0.08)
        {
            dataPrepare(underlyingCode, startDate, endDate, 0);
            //string index = "000905.SH";
            //dataPrepareAll(index, startDate, endDate);
            var daily = DailyKLine[underlyingCode];
            var indexDaily = DailyKLine[index];
            var minutely = minutelyKLine[underlyingCode];
            //var ticks = tick[underlyingCode];
            for (int i = 1; i < daily.Count() - 2; i++)
            {
                var today = daily[i];
                var yesterday = daily[i - 1];
                var tomorrow = daily[i + 1];
                var tomorrow2 = daily[i + 2];
                var indexToday = indexDaily[i];
                var indexYesterday = indexDaily[i - 1];
                if (yesterday == null || today == null || indexYesterday == null || today.TradeStatus != "交易")
                {
                    continue;
                }
                double preClose = yesterday.Close * today.AdjFactor / yesterday.AdjFactor;
                double limitPrice = Math.Round(preClose * 1.1, 2);
                if (today.High > preClose * (1 + parameter))
                {
                    var minuteToday = minutely[today.DateTime.Date];
                    foreach (var item in minuteToday)
                    {
                        if (item.Volume > 0 && item.Amount == 0)
                        {
                            item.Amount = item.Volume * item.Close;
                        }
                    }
                    var indexMinuteToday = minutelyKLine[index][today.DateTime.Date];
                    if (indexMinuteToday.Count == 0 || minuteToday.Count == 0)
                    {
                        continue;
                    }
                    double position = 0;
                    double openIndex = 0;
                    GrabCeilingTransaction trade = new GrabCeilingTransaction();
                    for (int j = 5; j < minuteToday.Count(); j++)
                    {
                        if (minuteToday[j].Open > preClose * (1 + parameter) && position == 0 && minuteToday[j].Open < preClose * (1 + parameter + 0.01) && j <= minuteToday.Count() - 10 && indexMinuteToday[j].Open > indexYesterday.Close * 0.9) //股票价格达到8%
                        {
                            double volume = minuteToday[j].Volume * 0.05;
                            if (volume > 0)
                            {
                                position = -1;
                                // double price = minuteToday[j].Amount / minuteToday[j].Volume;
                                double price = minuteToday[j].Open * (1 - feeRatioSell);
                                if (minuteToday[j].Amount > 0)
                                {
                                    price = minuteToday[j].Amount / minuteToday[j].Volume * (1 - feeRatioSell);
                                }
                                else
                                {
                                    price = minuteToday[j].Open * (1 - feeRatioSell);
                                }
                                trade = new GrabCeilingTransaction();
                                trade.code = underlyingCode;
                                trade.date = today.DateTime.Date;
                                trade.openTime = minuteToday[j].DateTime;
                                trade.openPrice = price;
                                trade.position = position;
                                trade.maxOpenAmount = Math.Min(volume * price, 100000);
                                trade.parameter = parameter;
                                openIndex = j;
                                trade.limitPrice = limitPrice;
                                trade.fiveMinutesIncreaseBefore = minuteToday[j].Open / minuteToday[j - 5].Open - 1;
                                trade.fiveMinutesIncreaseAfter = minuteToday[j + 5].Open / minuteToday[j].Open - 1;
                                trade.oneMinuteIncreaseAfter = minuteToday[j + 1].Open / minuteToday[j].Open - 1;
                            }
                        }
                        else if (position == -1)
                        {
                            //止损或者收盘强制平仓

                            if ((j >= minuteToday.Count - 5) || (j > openIndex + 60) || (j > openIndex + 20 && trade.openPrice < minuteToday[j].Open))
                            {
                                position = 0;
                                double volume = minuteToday[j].Volume * 0.05;
                                double price = minuteToday[j].Close;
                                if (volume > 0)
                                {
                                    if (minuteToday[j].Amount > 0)
                                    {
                                        price = minuteToday[j].Amount / minuteToday[j].Volume * (1 + feeRatioBuy);
                                    }
                                    else
                                    {
                                        price = minuteToday[j].Open * (1 + feeRatioBuy);
                                    }

                                }
                                trade.closePrice = price;
                                trade.closeTime = minuteToday[j].DateTime;
                                if (minuteToday[j].Open == limitPrice)
                                {
                                    if (tomorrow.Open > 1.099 * limitPrice)
                                    {
                                        trade.closePrice = tomorrow2.Open * (1 + feeRatioBuy);
                                        trade.closeTime = tomorrow2.DateTime.AddHours(9).AddMinutes(30);
                                    }
                                    else
                                    {
                                        trade.closePrice = tomorrow.Open * (1 + feeRatioBuy);
                                        trade.closeTime = tomorrow.DateTime.AddHours(9).AddMinutes(30);
                                    }

                                }
                                trade.maxCloseAmount = Math.Min(volume * price, 100000);
                                trade.yield = -(trade.closePrice - trade.openPrice) / trade.openPrice;
                                transactionData.Add(trade);
                                break;
                            }
                            if (minuteToday[j].Open > trade.openPrice && minuteToday[j].Volume > 0)
                            {
                                position = 0;
                                double volume = minuteToday[j].Volume * 0.05;
                                double price = minuteToday[j].Close;
                                if (volume > 0)
                                {
                                    if (minuteToday[j].Amount > 0)
                                    {
                                        price = minuteToday[j].Amount / minuteToday[j].Volume * (1 + feeRatioBuy);
                                    }
                                    else
                                    {
                                        price = minuteToday[j].Open * (1 + feeRatioBuy);
                                    }
                                }
                                trade.closePrice = price;
                                trade.closeTime = minuteToday[j].DateTime;
                                if (minuteToday[j].Open == limitPrice)
                                {
                                    trade.closePrice = tomorrow.Open * (1 + feeRatioBuy);
                                    trade.closeTime = tomorrow.DateTime.AddHours(9).AddMinutes(30);
                                }
                                trade.maxCloseAmount = Math.Min(volume * price, 100000);
                                trade.yield = -(trade.closePrice - trade.openPrice) / trade.openPrice;
                                transactionData.Add(trade);
                                break;
                            }
                        }

                    }
                }
            }


        }


        /// <summary>
        /// 大跌之后做反转
        /// </summary>
        /// <param name="underlyingCode"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="parameter"></param>
        public void backtestOpposite2(string underlyingCode, DateTime startDate, DateTime endDate, double parameter = 0.08)
        {
            dataPrepare(underlyingCode, startDate, endDate, 0);
            //string index = "000905.SH";
            //dataPrepareAll(index, startDate, endDate);
            var daily = DailyKLine[underlyingCode];
            var indexDaily = DailyKLine[index];
            var minutely = minutelyKLine[underlyingCode];
            //var ticks = tick[underlyingCode];
            for (int i = 1; i < daily.Count() - 2; i++)
            {
                var today = daily[i];
                var yesterday = daily[i - 1];
                var tomorrow = daily[i + 1];
                var tomorrow2 = daily[i + 2];
                var indexToday = indexDaily[i];
                var indexYesterday = indexDaily[i - 1];
                if (yesterday == null || today == null || indexYesterday == null || today.TradeStatus != "交易")
                {
                    continue;
                }
                double preClose = yesterday.Close * today.AdjFactor / yesterday.AdjFactor;
                double limitPrice = Math.Round(preClose * 1.1, 2);
                double limitPriceLower= Math.Round(preClose * 0.9, 2);
                if (today.Low < preClose * (1 - parameter))
                {
                    var minuteToday = minutely[today.DateTime.Date];
                    foreach (var item in minuteToday)
                    {
                        if (item.Volume > 0 && item.Amount == 0)
                        {
                            item.Amount = item.Volume * item.Close;
                        }
                    }
                    var indexMinuteToday = minutelyKLine[index][today.DateTime.Date];
                    if (indexMinuteToday.Count == 0 || minuteToday.Count == 0)
                    {
                        continue;
                    }
                    double position = 0;
                    double openIndex = 0;
                    GrabCeilingTransaction trade = new GrabCeilingTransaction();
                    for (int j = 5; j < minuteToday.Count(); j++)
                    {
                        if (minuteToday[j].Open < preClose * (1 - parameter) && position == 0 && minuteToday[j].Open > preClose * (1 - parameter - 0.01) && j <= minuteToday.Count() - 10 ) //股票价格跌幅达到8%
                        {
                            double volume = minuteToday[j].Volume * 0.05;
                            if (volume > 0)
                            {
                                position = 1;
                                // double price = minuteToday[j].Amount / minuteToday[j].Volume;
                                double price = minuteToday[j].Open * (1 + feeRatioBuy);
                                if (minuteToday[j].Amount > 0)
                                {
                                    price = minuteToday[j].Amount / minuteToday[j].Volume * (1 + feeRatioBuy);
                                }
                                else
                                {
                                    price = minuteToday[j].Open * (1 + feeRatioBuy);
                                }
                                trade = new GrabCeilingTransaction();
                                trade.code = underlyingCode;
                                trade.date = today.DateTime.Date;
                                trade.openTime = minuteToday[j].DateTime;
                                trade.openPrice = price;
                                trade.position = position;
                                trade.maxOpenAmount = Math.Min(volume * price, 100000);
                                trade.parameter = parameter;
                                openIndex = j;
                                trade.limitPrice = limitPrice;
                                trade.fiveMinutesIncreaseBefore = minuteToday[j].Open / minuteToday[j - 5].Open - 1;
                                trade.fiveMinutesIncreaseAfter = minuteToday[j + 5].Open / minuteToday[j].Open - 1;
                                trade.oneMinuteIncreaseAfter = minuteToday[j + 1].Open / minuteToday[j].Open - 1;
                            }
                        }
                        else if (position == 1)
                        {
                            //止损或者收盘强制平仓

                            if ((j >= minuteToday.Count - 5) || (j > openIndex + 60) || (j > openIndex + 20 && trade.openPrice > minuteToday[j].Open))
                            {
                                position = 0;
                                double volume = minuteToday[j].Volume * 0.05;
                                double price = minuteToday[j].Close;
                                if (volume > 0)
                                {
                                    if (minuteToday[j].Amount > 0)
                                    {
                                        price = minuteToday[j].Amount / minuteToday[j].Volume * (1 - feeRatioSell);
                                    }
                                    else
                                    {
                                        price = minuteToday[j].Open * (1 - feeRatioSell);
                                    }

                                }
                                trade.closePrice = price;
                                trade.closeTime = minuteToday[j].DateTime;
                                if (minuteToday[j].Open == limitPriceLower)
                                {
                                    if (tomorrow.Open < 0.905 * limitPriceLower)
                                    {
                                        trade.closePrice = tomorrow2.Open * (1 - feeRatioSell);
                                        trade.closeTime = tomorrow2.DateTime.AddHours(9).AddMinutes(30);
                                    }
                                    else
                                    {
                                        trade.closePrice = tomorrow.Open * (1 - feeRatioSell);
                                        trade.closeTime = tomorrow.DateTime.AddHours(9).AddMinutes(30);
                                    }

                                }
                                trade.maxCloseAmount = Math.Min(volume * price, 100000);
                                trade.yield = (trade.closePrice - trade.openPrice) / trade.openPrice;
                                transactionData.Add(trade);
                                break;
                            }
                            if (minuteToday[j].Open > trade.openPrice && minuteToday[j].Volume > 0)
                            {
                                position = 0;
                                double volume = minuteToday[j].Volume * 0.05;
                                double price = minuteToday[j].Close;
                                if (volume > 0)
                                {
                                    if (minuteToday[j].Amount > 0)
                                    {
                                        price = minuteToday[j].Amount / minuteToday[j].Volume * (1 + feeRatioBuy);
                                    }
                                    else
                                    {
                                        price = minuteToday[j].Open * (1 + feeRatioBuy);
                                    }
                                }
                                trade.closePrice = price;
                                trade.closeTime = minuteToday[j].DateTime;
                                if (minuteToday[j].Open == limitPrice)
                                {
                                    trade.closePrice = tomorrow.Open * (1 + feeRatioBuy);
                                    trade.closeTime = tomorrow.DateTime.AddHours(9).AddMinutes(30);
                                }
                                trade.maxCloseAmount = Math.Min(volume * price, 100000);
                                trade.yield = (trade.closePrice - trade.openPrice) / trade.openPrice;
                                transactionData.Add(trade);
                                break;
                            }
                        }

                    }
                }
            }


        }




        public void backtest(string underlyingCode, DateTime startDate, DateTime endDate, double parameter = 0.08)
        {
            dataPrepare(underlyingCode, startDate, endDate, 0);
            //string index = "000905.SH";
            //dataPrepareAll(index, startDate, endDate);
            var daily = DailyKLine[underlyingCode];
            var indexDaily = DailyKLine[index];
            var minutely = minutelyKLine[underlyingCode];
            //var ticks = tick[underlyingCode];
            for (int i = 1; i < daily.Count(); i++)
            {
                var today = daily[i];
                var yesterday = daily[i - 1];
                var indexToday = indexDaily[i];
                var indexYesterday = indexDaily[i - 1];
                if (yesterday == null || today == null || indexYesterday == null || today.TradeStatus != "交易")
                {
                    continue;
                }
                double preClose = yesterday.Close * today.AdjFactor / yesterday.AdjFactor;
                double limitPrice = Math.Round(preClose * 1.1, 2);
                if (today.High > preClose * (1 + parameter))
                {
                    var minuteToday = minutely[today.DateTime.Date];
                    foreach (var item in minuteToday)
                    {
                        if (item.Volume > 0 && item.Amount == 0)
                        {
                            item.Amount = item.Volume * item.Close;
                        }
                    }
                    var indexMinuteToday = minutelyKLine[index][today.DateTime.Date];
                    if (indexMinuteToday.Count == 0 || minuteToday.Count == 0)
                    {
                        continue;
                    }
                    double position = 0;
                    double openIndex = 0;
                    GrabCeilingTransaction trade = new GrabCeilingTransaction();
                    for (int j = 5; j < minuteToday.Count(); j++)
                    {
                        if (minuteToday[j].Open > preClose * (1 + parameter) && position == 0 && minuteToday[j].Open < limitPrice * 0.995 && j <= minuteToday.Count() - 10 && indexMinuteToday[j].Open > indexYesterday.Close) //股票价格达到8%且五分钟涨幅不超过4%时候买入
                        {
                            double volume = minuteToday[j].Volume * 0.05;
                            if (volume > 0)
                            {
                                position = 1;
                                // double price = minuteToday[j].Amount / minuteToday[j].Volume;
                                double price = minuteToday[j].Open * (1 + feeRatioBuy);
                                trade = new GrabCeilingTransaction();
                                trade.code = underlyingCode;
                                trade.date = today.DateTime.Date;
                                trade.openTime = minuteToday[j].DateTime;
                                trade.openPrice = price;
                                trade.position = position;
                                trade.maxOpenAmount = Math.Min(volume * price, 100000);
                                trade.parameter = parameter;
                                openIndex = j;
                                trade.fiveMinutesIncreaseBefore = minuteToday[j].Open / minuteToday[j - 5].Open - 1;
                                trade.fiveMinutesIncreaseAfter = minuteToday[j + 5].Open / minuteToday[j].Open - 1;
                                trade.oneMinuteIncreaseAfter = minuteToday[j + 1].Open / minuteToday[j].Open - 1;
                            }
                        }
                        else if (position == 1)
                        {
                            //止损或者收盘强制平仓

                            if ((minuteToday[j].Open < trade.openPrice * 0.9 && minuteToday[j].Volume > 0) || (j >= minuteToday.Count - 5) || (j > openIndex + 20))
                            {
                                position = 0;
                                double volume = minuteToday[j].Volume * 0.05;
                                double price = minuteToday[j].Close;
                                if (volume > 0)
                                {
                                    //price= minuteToday[j].Amount / minuteToday[j].Volume;
                                    price = minuteToday[j].Open * (1 - feeRatioSell);
                                }
                                trade.closePrice = price;
                                trade.closeTime = minuteToday[j].DateTime;
                                trade.maxCloseAmount = Math.Min(volume * price, 100000);
                                trade.yield = (trade.closePrice - trade.openPrice) / trade.openPrice * trade.maxOpenAmount;
                                transactionData.Add(trade);
                                break;
                            }
                            if (minuteToday[j].Low < trade.openPrice * 0.995 && minuteToday[j].Volume > 0)
                            {
                                position = 0;
                                double volume = minuteToday[j].Volume * 0.05;
                                double price = minuteToday[j].Close;
                                if (volume > 0)
                                {
                                    //price= minuteToday[j].Amount / minuteToday[j].Volume;
                                    price = minuteToday[j].Low * (1 - feeRatioSell);
                                }
                                trade.closePrice = price;
                                trade.closeTime = minuteToday[j].DateTime;
                                trade.maxCloseAmount = Math.Min(volume * price, 100000);
                                trade.yield = (trade.closePrice - trade.openPrice) / trade.openPrice * trade.maxOpenAmount;
                                transactionData.Add(trade);
                                break;
                            }
                        }

                    }
                }
            }


        }

        private void dataPrepareAll(string underlyingCode, DateTime startDate, DateTime endDate)
        {
            //获取交易日信息
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取日线数据
            var dayNow = stockDailyRepo.GetStockTransaction(underlyingCode, startDate, endDate);
            if (this.DailyKLine.ContainsKey(underlyingCode))
            {
                var data = DailyKLine[underlyingCode];
                data = dayNow;
            }
            else
            {
                DailyKLine.Add(underlyingCode, dayNow);
            }
            //选取需要获取数据的交易日
            List<DateTime> myTradedays = dateRepo.GetStockTransactionDate(startDate, endDate);


            getMinuteData(underlyingCode, myTradedays);

            //getTickData(underlyingCode, myTradedays);
        }



        //将计算用的数据准备好
        private void dataPrepare(string underlyingCode, DateTime startDate, DateTime endDate, int pushForwardDays = 0)
        {
            //获取交易日信息
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取日线数据
            var dayNow = stockDailyRepo.GetStockTransaction(underlyingCode, startDate.AddDays(-pushForwardDays), endDate);
            if (this.DailyKLine.ContainsKey(underlyingCode))
            {
                var data = DailyKLine[underlyingCode];
                data = dayNow;
            }
            else
            {
                DailyKLine.Add(underlyingCode, dayNow);
            }
            //选取需要获取数据的交易日
            List<DateTime> myTradedays = new List<DateTime>();
            //从日线上观察波动剧烈的日期，并记录数据
            for (int i = 1; i < DailyKLine[underlyingCode].Count; i++)
            {

                var yesterday = DailyKLine[underlyingCode][i - 1];
                var item = DailyKLine[underlyingCode][i];
                if (yesterday == null || item == null)
                {
                    continue;
                }
                double priceClose = yesterday.Close * item.AdjFactor / yesterday.AdjFactor;
                if (item.High > priceClose * 1.05 || item.Low<priceClose*0.95)
                {
                    if (myTradedays.Contains(item.DateTime.Date) == false)
                    {
                        myTradedays.Add(item.DateTime.Date);
                    }
                    myTradedays.Add(DateTimeExtension.DateUtils.NextTradeDay(item.DateTime.Date));
                }
            }

            getMinuteData(underlyingCode, myTradedays);

            //getTickData(underlyingCode, myTradedays);
        }

        //获取分钟线数据
        private void getMinuteData(string underlyingCode, List<DateTime> tradedays)
        {
            foreach (var date in tradedays)
            {
                if (minutelyKLine.ContainsKey(underlyingCode) == false)
                {
                    try
                    {
                        // var minuteNow = stockMinutelyRepo.GetStockTransactionFromLocalSqlByCode(underlyingCode, date, date);
                        var minuteNow = stockMinutelyRepo.GetStockTransactionFromLocalSqlByCodeWithRedis(underlyingCode, date, date);
                        Dictionary<DateTime, List<StockTransaction>> data = new Dictionary<DateTime, List<StockTransaction>>();
                        data.Add(date, minuteNow);
                        minutelyKLine.Add(underlyingCode, data);
                    }
                    catch (Exception e)
                    {

                        Console.WriteLine(e.Message);
                    }
                }
                else if (minutelyKLine[underlyingCode].ContainsKey(date) == false)
                {
                    try
                    {
                        // var minuteNow = stockMinutelyRepo.GetStockTransactionFromLocalSqlByCode(underlyingCode, date, date);
                        var minuteNow = stockMinutelyRepo.GetStockTransactionFromLocalSqlByCodeWithRedis(underlyingCode, date, date);
                        var data = minutelyKLine[underlyingCode];
                        data.Add(date, minuteNow);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
            }
        }

        //获取tick数据
        private void getTickData(string underlyingCode, List<DateTime> tradedays)
        {
            foreach (var date in tradedays)
            {
                DateTime dateStart = date.Date + new TimeSpan(9, 30, 0);
                DateTime dateEnd = date.Date + new TimeSpan(15, 0, 0);
                if (tick.ContainsKey(underlyingCode) == false)
                {
                    try
                    {
                        var tickNow = stockTickRepo.GetStockTransaction(underlyingCode, dateStart, dateEnd);
                        Dictionary<DateTime, List<StockTickTransaction>> data = new Dictionary<DateTime, List<StockTickTransaction>>();
                        data.Add(date, tickNow);
                        tick.Add(underlyingCode, data);
                    }
                    catch (Exception e)
                    {

                        Console.WriteLine(e.Message);
                    }
                }
                else if (tick[underlyingCode].ContainsKey(date) == false)
                {
                    try
                    {
                        var tickNow = stockTickRepo.GetStockTransaction(underlyingCode, dateStart, dateEnd);
                        var data = tick[underlyingCode];
                        data.Add(date, tickNow);
                    }
                    catch (Exception e)
                    {

                        Console.WriteLine(e.Message);
                    }
                }
            }
        }

        private List<double> getYieldList(DateTime startDate, DateTime endDate, int N, double K1, double K2, double lossStopRatio = 0.008)
        {
            List<double> yieldList = new List<double>();
            return yieldList;
        }

        private double getParametersSharpe(DateTime startDate, DateTime endDate, int N, double K1, double K2, double lossStopRatio = 0.02)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<double> yieldList = new List<double>();
            foreach (var date in tradedays)
            {
                double yield = computeDaily(date, N, K1, K2, lossStopRatio);
                yieldList.Add(yield);
            }
            double sharpe = evaluateSharpe(yieldList);
            double returnAVG = yieldList.Sum() / yieldList.Count() * 252;
            return sharpe + returnAVG * 10;
        }

        private List<GrabCeilingTransaction> getTransactionData(DateTime startDate, DateTime endDate, int N, double K1, double K2, double lossStopRatio = 0.02)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<GrabCeilingTransaction> list = new List<GrabCeilingTransaction>();
            foreach (var date in tradedays)
            {
                var transaction = computeDailyWithRecord(date, N, K1, K2, lossStopRatio);
                list.Add(transaction);
            }
            return list;
        }


        //按照给定参数回测
        private double computeDaily(DateTime date, int N, double K1, double K2, double lossStopRatio)
        {
            var record = computeDailyWithRecord(date, N, K1, K2, lossStopRatio);
            return record.yield;
        }

        private double getVolmueScore(List<StockTransaction> data, int index)
        {
            double score = 0;
            double volumeMean = 0;
            int num = 0;
            for (int i = Math.Max(index - 30, 0); i < index; i++)
            {
                num += 1;
                volumeMean += data[i].Volume;
            }
            if (num > 0)
            {
                volumeMean /= num;
                score = data[index].Volume / volumeMean;
            }
            return score;
        }


        //按照给定参数回测
        private GrabCeilingTransaction computeDailyWithRecord(DateTime date, int N, double K1, double K2, double lossStopRatio)
        {
            GrabCeilingTransaction result = new GrabCeilingTransaction();



            return result;
        }


        //挑选最佳参数
        private double evaluateSharpe(List<double> yieldList)
        {
            double sharpe = 0;
            double std = MathUtility.std(yieldList);
            double mean = yieldList.Average();
            // if (mean*252>0.1)
            {
                sharpe = mean / std * Math.Sqrt(252);
            }
            return sharpe;
        }

        public Dictionary<string, StockIPOInfo> getStockInfoList(string index, DateTime startDate, DateTime endDate)
        {
            Dictionary<string, StockIPOInfo> allDic = new Dictionary<string, StockIPOInfo>();
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<DateTime> lastDate = new List<DateTime>();
            lastDate.Add(tradedays.Last());
            var dic = getStockList(lastDate, index);
            foreach (var stock in dic)
            {
                if (stock.Value.IPODate > startDate)
                {
                    startDate = stock.Value.IPODate;
                }
                if (stock.Value.DelistDate < endDate)
                {
                    endDate = stock.Value.DelistDate;
                }
                if (allDic.ContainsKey(stock.Value.code) == false)
                {
                    allDic.Add(stock.Value.code, stock.Value);
                }
                //Console.WriteLine("code:{0} complete!", stock.Key);
            }
            return allDic;
        }

        private Dictionary<string, StockIPOInfo> getStockList(List<DateTime> days, string index)
        {
            Dictionary<string, StockIPOInfo> stockDic = new Dictionary<string, StockIPOInfo>();
            var stockInfo = stockInfoRepo.GetStockListInfoFromSql();
            foreach (var date in days)
            {
                var list = getIndexStocks(date, index);
                foreach (var item in list)
                {
                    if (stockDic.ContainsKey(item) == false)
                    {
                        foreach (var stock in stockInfo)
                        {
                            if (stock.code == item)
                            {
                                stockDic.Add(item, stock);
                            }
                        }
                    }
                }

            }
            return stockDic;
        }

        private List<string> getIndexStocks(DateTime date, string index)
        {
            var rawData = windReader.GetDataSetTable("sectorconstituent", string.Format("date={0};windcode={1}", date.Date, index));
            List<string> codeList = new List<string>();
            foreach (DataRow dr in rawData.Rows)
            {
                codeList.Add(Convert.ToString(dr[1]));
            }
            return codeList;
        }
        //回测结果的记录

    }
}
