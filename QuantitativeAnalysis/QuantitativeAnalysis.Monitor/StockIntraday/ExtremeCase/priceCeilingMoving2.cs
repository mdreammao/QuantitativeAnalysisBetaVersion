/* 
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
    public class priceCeilingMoving2
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private WindReader windReader;
        private List<DateTime> tradedays = new List<DateTime>();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransactionDaily> transactionData;
        private StockMinuteRepository stockMinutelyRepo;
        private StockTickRepository stockTickRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
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
        public priceCeilingMoving2(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, StockTickRepository stockTickRepo, StockInfoRepository stockInfoRepo)
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
            allStockDic = getStockInfoList(index, endDate, endDate);
            foreach (var item in allStockDic)
            {
                var stock = item.Value;
                DateTime stockStart = startDate;
                DateTime stockEnd = endDate;
                if (stockStart < stock.IPODate)
                {
                    stockStart = stock.IPODate.AddDays(10);
                }
                if (stockEnd > stock.DelistDate)
                {
                    stockEnd = stock.DelistDate;
                }
                //dataPrepare(stock.code, stockStart, stockEnd);
                backtest(stock.code, stockStart, stockEnd);
            }
            var dt = DataTableExtension.ToDataTable(transactionData);
            var codeStr = index.Split('.');
            string name = string.Format("E:\\result\\grabCeiling\\{0}.csv", codeStr[0]);
            DataTableExtension.SaveCSV(dt, name);

        }

        public void backtestAllStock(DateTime startDate, DateTime endDate)
        {
            allStockDic = getAllstockInfo(endDate, endDate);
            foreach (var item in allStockDic)
            {
                var stock = item.Value;
                DateTime stockStart = startDate;
                DateTime stockEnd = endDate;
                if (stockStart < stock.IPODate)
                {
                    stockStart = stock.IPODate;
                }
                if (stockEnd > stock.DelistDate)
                {
                    stockEnd = stock.DelistDate;
                }
                //dataPrepare(stock.code, stockStart, stockEnd);
                backtest(stock.code, stockStart, stockEnd);
            }
            var dt = DataTableExtension.ToDataTable(transactionData);
            var codeStr = "all";
            string name = string.Format("E:\\result\\grabCeiling\\{0}.csv", codeStr);
            DataTableExtension.SaveCSV(dt, name);

        }


        public void backtest(string underlyingCode, DateTime startDate, DateTime endDate, double ceilRatio = 0.08)
        {
            dataPrepare(underlyingCode, startDate, endDate);
            OneByOneTransactionDaily trade = new OneByOneTransactionDaily();
            double ratio = 1 + ceilRatio;
            double position = 0;
            var data = DailyKLine[underlyingCode];
            for (int i = 1; i < data.Count(); i++)
            {
                var yesterday = data[i - 1];
                var today = data[i];
                double preClose = yesterday.Close * yesterday.AdjFactor / today.AdjFactor;
                if (today.High > preClose * ratio && today.Volume > 0 && position == 0)
                {
                    var tickToday = tick[underlyingCode][today.DateTime.Date];
                    foreach (var tickNow in tickToday)
                    {
                        if (tickNow.LastPrice >= preClose * ratio && position == 0)
                        {
                            trade = new OneByOneTransactionDaily();
                            trade.openTime = tickNow.TransactionDateTime;
                            trade.openPrice = tickNow.Ask1;
                            trade.maxOpenAmount = tickNow.AskV1 * tickNow.Ask1;
                            trade.position = 1;
                            position = 1;
                            trade.openAdjust = today.AdjFactor;
                            break;
                        }

                    }

                }
                if (position == 1 && trade.openTime < today.DateTime.Date && today.Open < preClose * 1.09) //买入之后卖出
                {
                    trade.closeTime = today.DateTime.Date + new TimeSpan(9, 30, 0);
                    trade.closeStatus = "股票未涨停平仓";
                    trade.closePrice = today.Open;
                    trade.closeAdjust = today.AdjFactor;
                    trade.yield = (trade.closePrice * trade.closeAdjust - trade.openPrice * trade.openAdjust) / (trade.openPrice * trade.openAdjust) * trade.maxOpenAmount;
                    transactionData.Add(trade);
                }

            }


        }

        public void backtestByDailyData(string underlyingCode, DateTime startDate, DateTime endDate, double ceilRatio = 0.08)
        {
            dataPrepare(underlyingCode, startDate, endDate);
            OneByOneTransactionDaily trade = new OneByOneTransactionDaily();
            double ratio = 1 + ceilRatio;
            double position = 0;
            var data = DailyKLine[underlyingCode];
            for (int i = 1; i < data.Count(); i++)
            {
                var yesterday = data[i - 1];
                var today = data[i];
                double preClose = yesterday.Close * yesterday.AdjFactor / today.AdjFactor;
                if (today.High > preClose * ratio && today.Volume > 0 && position == 0)
                {
                    var tickToday = tick[underlyingCode][today.DateTime.Date];
                    foreach (var tickNow in tickToday)
                    {
                        if (tickNow.LastPrice >= preClose * ratio && position == 0)
                        {
                            trade = new OneByOneTransactionDaily();
                            trade.openTime = tickNow.TransactionDateTime;
                            trade.openPrice = tickNow.Ask1;
                            trade.maxOpenAmount = tickNow.AskV1 * tickNow.Ask1;
                            trade.position = 1;
                            position = 1;
                            trade.openAdjust = today.AdjFactor;
                            break;
                        }

                    }

                }
                if (position == 1 && trade.openTime < today.DateTime.Date && today.Open < preClose * 1.09) //买入之后卖出
                {
                    trade.closeTime = today.DateTime.Date + new TimeSpan(9, 30, 0);
                    trade.closeStatus = "股票未涨停平仓";
                    trade.closePrice = today.Open;
                    trade.closeAdjust = today.AdjFactor;
                    trade.yield = (trade.closePrice * trade.closeAdjust - trade.openPrice * trade.openAdjust) / (trade.openPrice * trade.openAdjust) * trade.maxOpenAmount;
                    transactionData.Add(trade);
                }

            }


        }

        //将计算用的数据准备好
        private void dataPrepare(string underlyingCode, DateTime startDate, DateTime endDate, int pushForwardDays = 30)
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
            List<DateTime> myTradedays = new List<DateTime>();
            //从日线上观察波动剧烈的日期，并记录数据
            foreach (var item in DailyKLine[underlyingCode])
            {
                if (item==null)
                {
                    continue;
                }
                if (item.High / item.Low - 1 > 0.05)
                {
                    var today = item.DateTime.Date;
                    if (myTradedays.Contains(today) == false)
                    {
                        myTradedays.Add(item.DateTime.Date);
                    }
                    var nextDay = DateTimeExtension.DateUtils.NextTradeDay(item.DateTime.Date).Date;
                    if (myTradedays.Contains(nextDay) == false)
                    {
                        myTradedays.Add(nextDay);
                    }
                }
            }

            //getMinuteData(underlyingCode, myTradedays);

            // getTickData(underlyingCode, myTradedays);
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
                        var minuteNow = stockMinutelyRepo.GetStockTransaction(underlyingCode, date, date);
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
                        var minuteNow = stockMinutelyRepo.GetStockTransaction(underlyingCode, date, date);
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



        private Dictionary<string,StockIPOInfo> getAllstockInfo(DateTime startDate, DateTime endDate)
        {
            Dictionary<string, StockIPOInfo> myInfo = new Dictionary<string, StockIPOInfo>();
            var stockInfo = stockInfoRepo.GetStockListInfoFromSql();
            foreach (var item in stockInfo)
            {
                DateTime startTime = startDate;
                DateTime endTime = endDate;
                if (item.IPODate>startTime)
                {
                    startTime = item.IPODate;
                }
                if (item.DelistDate<endTime)
                {
                    endTime = item.DelistDate;
                }
                if (startTime<=endTime)
                {
                    myInfo.Add(item.code, item);
                }
            }
            return myInfo;
        }





        private Dictionary<string, StockIPOInfo> getStockInfoList(string index, DateTime startDate, DateTime endDate)
        {
            Dictionary<string, StockIPOInfo> allDic = new Dictionary<string, StockIPOInfo>();
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var dic = getStockList(tradedays, index);
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
