/*传统的双均线策略
 * 作为其他趋势策略的比较基准
 * 注意该策略中的价格，是用复权价格计算的
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

namespace QuantitativeAnalysis.Monitor.StockIntraday.MovingAverage
{
    public class MA1
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private List<DateTime> tradedays = new List<DateTime>();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData;
        private StockMinuteRepository stockMinutelyRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private double slipRatio = 0.001;
        private double feeRatioBuy = 0.0002;
        private double feeRatioSell = 0.0012;
        private Dictionary<DateTime, List<StockTransaction>> underlyingKLine = new Dictionary<DateTime, List<StockTransaction>>();
        //记录ma的信息，根据每分钟k线的收盘复权数据计算
        private Dictionary<int, Dictionary<DateTime, List<double>>> MADic = new Dictionary<int, Dictionary<DateTime, List<double>>>();

        public MA1(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void backtest(string code, DateTime startDate, DateTime endDate)
        {
            prepare(code, startDate, endDate);
            //训练集训练参数
            double maxn1 = 0;
            double maxn2 = 0;
            double maxSharpe = 0;
            for (int n1 = 3; n1 < 15; n1++)
            {
                for (int n2 = n1+1; n2 < 30; n2++)
                {
                    double sharpe = getParametersSharpe(startDate, endDate, n1,n2);
                    if (sharpe>maxSharpe)
                    {
                        maxSharpe = sharpe;
                        maxn1 = n1;
                        maxn2 = n2;
                        Console.WriteLine("sharpe:{0}, n1:{1}, n2:{2}", sharpe, n1, n2);
                    }
                }
            }
            Console.WriteLine("sharpe:{0}, n1:{1}, n2:{2}", maxSharpe, maxn1, maxn2);
        }


        private void prepare(string code, DateTime startDate, DateTime endDate)
        {
            //获取交易日信息
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取基本信息
            this.code = code;
            //获取日线数据
            var dailyData = stockDailyRepo.GetStockTransaction(code, startDate, endDate);
            //获取分钟线数据
            foreach (var date in tradedays)
            {
                var minuteKLine = stockMinutelyRepo.GetStockTransaction(code, date, date);
                underlyingKLine.Add(date, minuteKLine);
            }
            foreach (var item in dailyData)
            {
                var KLines = underlyingKLine[item.DateTime];
                foreach (var KLine in KLines)
                {
                    KLine.AdjFactor = item.AdjFactor;
                }
            }
            for (int n = 3; n < 50; n++)
            {
                var dic = getMA(underlyingKLine, n);
                MADic.Add(n, dic);
            }
        }


        private double getParametersSharpe(DateTime startDate, DateTime endDate,int n1,int n2,double lossStopRatio = 0.02)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<double> yieldList = new List<double>();
            foreach (var date in tradedays)
            {
                double yield = computeDaily(date,n1,n2, lossStopRatio);
                yieldList.Add(yield);
            }
            double sharpe = evaluateSharpe(yieldList);
            return sharpe;
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

        private Dictionary<DateTime, List<double>> getMA(Dictionary<DateTime, List<StockTransaction>> underlyingKLine,int n)
        {
            Dictionary<DateTime, List<double>> dic = new Dictionary<DateTime, List<double>>();
            List<StockTransaction> KLines = new List<StockTransaction>();
            foreach (var item in underlyingKLine)
            {
                KLines.AddRange(item.Value);
            }
            for (int i = 0; i < KLines.Count(); i++)
            {
                double ma = 0;
                int num = 0;
                DateTime today = KLines[i].DateTime.Date;
                for (int j =Math.Max(0, i-n); j < i; j++)
                {
                    if (KLines[j].Volume>0)
                    {
                        num += 1;
                        ma += KLines[j].Close * KLines[j].AdjFactor;
                    }
                }
                if (num>n*0.95)
                {
                    ma /= num;
                }
                if (dic.ContainsKey(today))
                {
                    dic[today].Add(ma);
                }
                else
                {
                    List<double> list = new List<double>();
                    list.Add(ma);
                    dic.Add(today, list);
                }
            }
            return dic;
        }

        //按照给定参数回测
        private double computeDaily(DateTime date, int n1, int n2, double lossStopRatio)
        {
            double yield = 0;
            if (underlyingKLine.ContainsKey(date) == false || MADic.ContainsKey(n1)==false || MADic.ContainsKey(n2)==false || MADic[n1].ContainsKey(date)==false ||MADic[n2].ContainsKey(date)==false)
            {
                return yield;
            }
            var ma1 = MADic[n1][date];
            var ma2 = MADic[n2][date];
            var underlying = underlyingKLine[date];
            double position = 0;
            double positionflag = 0;
            double openPrice = 0;
            double closePrice = 0;
            double longMaxPrice = 0;
            double shortMinPrice = 99999;
            double lossStopPoints = lossStopRatio * underlying[0].Open;
            double slipBuy = 0;
            double slipSell = 0;
            for (int i = 0; i < underlying.Count(); i++)
            {
                slipBuy = Math.Max(slipRatio * underlying[i].Close, 0.01)+feeRatioBuy*underlying[i].Close;
                slipSell = Math.Max(slipRatio * underlying[i].Close, 0.01) + feeRatioSell * underlying[i].Close;
                if (position==0 && ma1[i]>ma2[i] && i<underlying.Count()-30) //ma1上穿ma2
                {
                    openPrice = underlying[i+1].Open + slipBuy;
                    longMaxPrice = openPrice;
                    positionflag = 1;
                    position = 1;
                }
                else if (position==0 && ma1[i]<ma2[i] && i < underlying.Count() - 30) //ma1下穿ma2
                {
                    openPrice = underlying[i+1].Open - slipSell;
                    shortMinPrice = openPrice;
                    positionflag = -1;
                    position = -1;
                }
                if (position == 1 && underlying[i].Close > longMaxPrice)
                {
                    longMaxPrice = underlying[i].Close;
                }
                if (position == -1 && underlying[i].Close < shortMinPrice)
                {
                    shortMinPrice = underlying[i].Close;
                }
                if (position == 1 && underlying[i].Close < longMaxPrice - lossStopPoints)
                {
                    closePrice = underlying[i+1].Open - slipSell;
                    position = 0;
                    break;
                }
                if (position == -1 && underlying[i].Close > shortMinPrice + lossStopPoints)
                {
                    closePrice = underlying[i+1].Open + slipBuy;
                    position = 0;
                    break;
                }
            }
            slipBuy = Math.Max(slipRatio * underlying[underlying.Count() - 5].Close, 0.01) + feeRatioBuy * underlying[underlying.Count() - 5].Close;
            slipSell = Math.Max(slipRatio * underlying[underlying.Count() - 5].Close, 0.01) + feeRatioSell * underlying[underlying.Count() - 5].Close;
            if (position != 0)
            {
                if (position == 1)
                {
                    closePrice = underlying[underlying.Count() - 5].Close - slipSell;
                    position = 0;
                }
                else if (position == -1)
                {
                    closePrice = underlying[underlying.Count() - 5].Close + slipBuy;
                    position = 0;
                }
            }
            if (positionflag != 0)
            {
                yield = (closePrice / openPrice - 1) * positionflag;
            }
            return yield;
        }

    }
}
