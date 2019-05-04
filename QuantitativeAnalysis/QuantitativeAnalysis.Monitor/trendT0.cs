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
    //利用趋势的方法来计算日内交易
    public class trendT0
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string stockBoard;
        private double slipRatio = 0.0002;
        private double multiple = 300;
        private List<DateTime> tradedays;
        private DateTime startDate, endDate;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private StockMinuteRepository stockMinutelyRepo;
        private List<OneByOneTransaction> transactionData;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private Dictionary<string, Dictionary<DateTime, List<StockTransaction>>> allStocks = new Dictionary<string, Dictionary<DateTime, List<StockTransaction>>>();
        public trendT0(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, string stockBoard, DateTime startDate, DateTime endDate)
        {
            this.stockDailyRepo = stockDailyRepo;
            this.stockMinutelyRepo = stockMinutelyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
            this.stockBoard = stockBoard;
            this.startDate = startDate;
            this.endDate = endDate;
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var list = searchAllStocks(stockBoard, startDate,endDate);
            list = new List<stockInfo>();
            stockInfo stock = new stockInfo();
            stock.code = "IF.CFE";
            stock.startDate = startDate;
            stock.endDate = endDate;
            list.Add(stock);
            getAllStocks(list);
            computeOnAllStocks(list);
        }

        //获取股票近三年列表信息
        private List<stockInfo> searchAllStocks(string stockBorad,DateTime startDate,DateTime endDate)
        {
            List<stockInfo> info = new List<stockInfo>();
            WindReader windReader = new WindReader();
            var dt=windReader.GetSectorconstituentByDate(stockBorad, startDate);
            foreach (DataRow dr in dt.Rows)
            {
                stockInfo stock = new stockInfo();
                stock.code = dr[1].ToString();
                stock.startDate = startDate;
                stock.endDate = endDate;
                info.Add(stock);
                break;
            }
            return info;
        }

        private void getAllStocks(List<stockInfo> info)
        {
            foreach (var stock in info)
            {
                DateTime startDate = stock.startDate;
                DateTime endDate = stock.endDate;
                string code = stock.code;
                var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
                Dictionary<DateTime, List<StockTransaction>> thisStock = new Dictionary<DateTime, List<StockTransaction>>();
                foreach (var date in tradedays)
                {
                    var stockMinuteData0 = stockMinutelyRepo.GetStockTransactionWithRedis(code, date, date);
                    thisStock.Add(date, stockMinuteData0);
                }
                allStocks.Add(code, thisStock);
            }
        }

        private void computeOnAllStocks(List<stockInfo> stockInfoList)
        {
            foreach (var stockInfo in stockInfoList)
            {
                rollingByTime(stockInfo);
            }
        }

        //算出一年的最优参数，给后面N天使用
        private double rollingByTime(stockInfo stock, int trainingSetDuration=120,int testSetDuration=10)
        {

            stockInfo stockRollInfo = new stockInfo();
            Dictionary<DateTime, parameterPair> parameters = new Dictionary<DateTime, parameterPair>();
            DateTime startDate = stock.startDate;
            DateTime endDate = stock.endDate;
            var myTradeDays = dateRepo.GetStockTransactionDate(stock.startDate, stock.endDate);
            int length = tradedays.Count();
            for (int i = 0; i < length-trainingSetDuration; i=i+testSetDuration)
            {
                DateTime trainStart = tradedays[i];
                DateTime trainEnd = tradedays[i + trainingSetDuration];
                int testStartIndex = i + trainingSetDuration + 1;
                int testEndIndex = i + trainingSetDuration + testSetDuration;
                if (i==0)
                {
                    testStartIndex = 0;
                }
                if (testEndIndex>=length)
                {
                    testEndIndex = length - 1;
                }
                stockRollInfo.code = stock.code;
                stockRollInfo.startDate = trainStart;
                stockRollInfo.endDate = trainEnd;
                var bestPara = getBestParameters(stockRollInfo);
                Console.WriteLine("start:{0}, end:{1}, bestSharpe:{2}, para1:{3}, para2:{4}, para3:{5}", trainStart, trainEnd,bestPara.bestSharpe,bestPara.parameter1,bestPara.parameter2,bestPara.parameter3);
                for (int j = testStartIndex; j <= testEndIndex; j++)
                {
                    parameterPair para = new parameterPair();
                    para.code = stock.code;
                    para.date = tradedays[j];
                    para.parameter1 = bestPara.parameter1;
                    para.parameter2 = bestPara.parameter2;
                    para.parameter3 = bestPara.parameter3;
                    para.strategy = bestPara.strategy;
                    para.existGoodParameter = bestPara.existGoodParameter;
                    parameters.Add(para.date, para);
                }
            }
            List<netvalueDaily> nv = new List<netvalueDaily>();
            List<OneByOneTransaction> transactionData = new List<OneByOneTransaction>();
            dualTrust(stock, parameters, ref nv, ref transactionData);
            double sharpe = Utilities.strategyPerformance.sharpeRatioByDailyNetValue(nv.Select(s => s.netvalue).ToList());
            return sharpe;
        }

        private parameterPair getBestParameters(stockInfo stock)
        {
            double max = -10;
            parameterPair bestPara = new parameterPair();
            double k1Max = 0;
            double k2Max = 0;
            double trailingParameterMax = 0;
            var myTradeDays = dateRepo.GetStockTransactionDate(stock.startDate, stock.endDate);
            Dictionary<DateTime, parameterPair> parameters = new Dictionary<DateTime, parameterPair>();
            for (double k1 =1; k1 <= 3; k1 = k1 + 0.2)
            {
                for (double k2 = 1; k2 <= 3; k2 = k2 + 0.2)
                {
                    for (double trailingParameter = 0.004; trailingParameter <= 0.03; trailingParameter = trailingParameter + 0.002)
                    {
                        parameters = new Dictionary<DateTime, parameterPair>();
                        foreach (var date in myTradeDays)
                        {
                            parameterPair para = new parameterPair();
                            para.parameter1 = k1;
                            para.parameter2 = k2;
                            para.parameter3 = trailingParameter;
                            para.date = date;
                            para.strategy = "DualTrust";
                            para.code = stock.code;
                            para.existGoodParameter = true;
                            parameters.Add(date,para);
                        }
                        List<netvalueDaily> nv = new List<netvalueDaily>();
                        List<OneByOneTransaction> transactionData = new List<OneByOneTransaction>();
                        dualTrust(stock, parameters, ref nv, ref transactionData);
                        double sharpe = Utilities.strategyPerformance.sharpeRatioByDailyNetValue(nv.Select(s => s.netvalue).ToList());
                        if (sharpe > max)
                        {
                            max = sharpe;
                            k1Max = k1;
                            k2Max = k2;
                            trailingParameterMax = trailingParameter;
                            //Console.WriteLine("sharpe:{0}, k1:{1}, k2:{2}, trailing:{3}", max, k1Max, k2Max, trailingParameterMax);
                        }
                    }
                }
            }
            bestPara.parameter1 = k1Max;
            bestPara.parameter2 = k2Max;
            bestPara.parameter3 = trailingParameterMax;
            bestPara.strategy = "DualTrust";
            bestPara.code = stock.code;
            bestPara.bestSharpe = max;
            if (max>0)
            {
                bestPara.existGoodParameter = true;
            }
            else
            {
                bestPara.existGoodParameter = false;
            }
            return bestPara;
        }


        private bool dualTrust(stockInfo stock,Dictionary<DateTime,parameterPair> parameters,ref List<netvalueDaily> nv,ref List<OneByOneTransaction> transactionData)
        {

            if (allStocks.ContainsKey(stock.code)==false)
            {
                return false;
            }
            var data = allStocks[stock.code];
            double cash = 10000;
            double position = 0;
            var myTradeDays=dateRepo.GetStockTransactionDate(stock.startDate, stock.endDate);
            OneByOneTransaction transaction = new OneByOneTransaction();
            //观察期限30分钟
            int duration = 30;
            int minutes = 240;
            foreach (var date in myTradeDays)
            {
                if (data.ContainsKey(date)==false || parameters.ContainsKey(date)==false)
                {
                    return false;
                }
                var stockToday = data[date];
                var para = parameters[date];
                double k1 = para.parameter1;
                double k2 = para.parameter2;
                double trailing = para.parameter3;
                double maxProfit = 0;
                bool tradable = true;
                if (para.existGoodParameter==false)
                {
                    tradable = false;
                }
                //通过每日前30分钟计算指标
                double HH = 0;//最高价的最高价
                double HC = 0; //收盘价的最高价
                double LC = 99999; //收盘价的最低价
                double LL = 99999; //最低价的最低价
                double Range = 0;
                double longPoint = -1;
                double shortPoint = -1;
                for (int j = 0; j < duration; j++)
                {
                    var dataNow = stockToday[j];
                    if (dataNow.High > HH)
                    {
                        HH = dataNow.High;
                    }
                    if (dataNow.Close > HC)
                    {
                        HC = dataNow.Close;
                    }
                    if (dataNow.Close < LC)
                    {
                        LC = dataNow.Close;
                    }
                    if (dataNow.Low < LL)
                    {
                        LL = dataNow.Low;
                    }
                }
                Range = Math.Max(HH - LC, HC - LL);
                longPoint =stockToday[duration].Open + k1 * Range;
                shortPoint = stockToday[duration].Open - k2 * Range;
                for (int j = duration; j < minutes; j++)
                {
                    var dataNow = stockToday[j];
                    var dataLast = stockToday[j - 1];
                    double avgPrice = dataNow.Close;
                    if (dataNow.Volume==0)
                    {
                        tradable = false;
                    }
                    else
                    {
                        avgPrice = dataNow.Amount / dataNow.Volume/multiple;
                        if (para.existGoodParameter==true)
                        {
                            tradable = true;
                        }
                    }
                    if (position==0 && j<=minutes-10 && tradable)
                    {
                        //多头信号
                        if (dataNow.Open > longPoint && dataLast.Open < longPoint)
                        {
                            position = 0.9*Math.Floor(cash*1000/avgPrice)/1000;
                            cash = cash - avgPrice*position - avgPrice*position * slipRatio;
                            transaction = new OneByOneTransaction();
                            transaction.position = position;
                            transaction.openTime = dataNow.DateTime;
                            transaction.openPrice = avgPrice;
                            maxProfit = 0;

                        }
                        //空头信号
                        else if (dataNow.Open < shortPoint && dataLast.Open > shortPoint)
                        {
                            position = -0.9 * Math.Floor(cash * 1000 / avgPrice) / 1000;
                            cash = cash - avgPrice*position + avgPrice*position * slipRatio;
                            transaction = new OneByOneTransaction();
                            transaction.position =position;
                            transaction.openTime = dataNow.DateTime;
                            transaction.openPrice = avgPrice;
                            maxProfit = 0;
                        }
                    }
                    else if (position!=0 && j<=minutes-5 && tradable) //非收盘前5分钟，按追踪止损平仓
                    {
                        if (position > 0)
                        {
                            if ((dataNow.Open / transaction.openPrice - 1) < maxProfit - trailing)
                            {
                                cash = cash + avgPrice*position - avgPrice*position * slipRatio;
                                position = 0;
                                transaction.closePrice = avgPrice;
                                transaction.closeTime = dataNow.DateTime;
                                transaction.closeStatus = "追踪止损";
                                transactionData.Add(transaction);
                                transaction = new OneByOneTransaction();
                            }
                        }
                        if (position < 0)
                        {
                            if ((transaction.openPrice / dataNow.Open - 1) < maxProfit - trailing)
                            {
                                cash = cash + avgPrice*position + avgPrice*position * slipRatio;
                                position = 0;
                                transaction.closePrice = avgPrice;
                                transaction.closeTime = dataNow.DateTime;
                                transaction.closeStatus = "追踪止损";
                                transactionData.Add(transaction);
                                transaction = new OneByOneTransaction();
                            }
                        }
                    }
                    else if (position!=0 && j>minutes-5 && tradable)//收盘前5分钟强制平仓
                    {
                        if (position >0)
                        {
                            cash = cash + avgPrice * position - avgPrice * position * slipRatio;
                            position = 0;
                            transaction.closePrice = avgPrice;
                            transaction.closeTime = dataNow.DateTime;
                            transaction.closeStatus = "收盘强平";
                            transactionData.Add(transaction);
                            transaction = new OneByOneTransaction();
                        }
                        if (position <0 )
                        {
                            cash = cash + avgPrice * position + avgPrice * position * slipRatio;
                            position = 0;
                            transaction.closePrice = avgPrice;
                            transaction.closeTime = dataNow.DateTime;
                            transaction.closeStatus = "收盘强平";
                            transactionData.Add(transaction);
                            transaction = new OneByOneTransaction();
                        }
                    }

                    //计算追踪止损的参数
                    if (position >0)
                    {
                        if ((dataNow.Close / transaction.openPrice - 1) > maxProfit)
                        {
                            maxProfit = (dataNow.Close / transaction.openPrice - 1);
                        }
                    }
                    if (position <0)
                    {
                        if ((transaction.openPrice / dataNow.Close - 1) > maxProfit)
                        {
                            maxProfit = (transaction.openPrice / dataNow.Close - 1);
                        }
                    }
                    if (j==minutes-1)
                    {
                        dataNow = stockToday[j];
                        netvalueDaily nvToday = new netvalueDaily();
                        nvToday.date = dataNow.DateTime.Date;
                        nvToday.netvalue = cash + position * dataNow.Close;
                        nv.Add(nvToday);
                    }
                }   
                
            }
            return true;
        }
        
    }
}
