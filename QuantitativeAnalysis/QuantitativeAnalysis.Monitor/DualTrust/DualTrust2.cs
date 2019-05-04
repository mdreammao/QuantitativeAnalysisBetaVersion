/* Dual Thrust是一个趋势跟踪系统，由Michael Chalek在20世纪80年代开发。
 * Dual Thrust系统具有简单易用、适用度广的特点，其思路简单、参数很少，配合不同的参数、
 * 止盈止损和仓位管理，可以为投资者带来长期稳定的收益，
 * 被投资者广泛应用于股票、货币、贵金属、债券、能源及股指期货市场等。
 * 在Dual Thrust交易系统中，对于震荡区间的定义非常关键，这也是该交易系统的核心和精髓。
 * Dual Thrust系统使用Range = Max(HH-LC,HC-LL)来描述震荡区间的大小。
 * 其中HH是N日High的最高价，LC是N日Close的最低价，HC是N日Close的最高价，LL是N日Low的最低价。
 * 信号出发的指标为指数，交易当月股指期货。
 * 根据指数前N日的信息，计算出对应的指标，指导股指期货日内交易。
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

namespace QuantitativeAnalysis.Monitor.DualTrust
{
    public class DualTrust2
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string underlyingCode;
        private string indexCode;
        private List<DateTime> tradedays = new List<DateTime>();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData;
        private StockMinuteRepository stockMinutelyRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private int minutes = 240;
        private Dictionary<DateTime, List<StockTransaction>> underlying = new Dictionary<DateTime, List<StockTransaction>>();
        private List<StockTransaction> underlyingAll = new List<StockTransaction>();
        private double slipPoint = 1;
        //预处理计算N天的range值
        Dictionary<int, Dictionary<DateTime, double>> RangeDic = new Dictionary<int, Dictionary<DateTime, double>>();
        //预处理分钟线数据
        Dictionary<DateTime, List<StockTransaction>> underlyingKLine = new Dictionary<DateTime, List<StockTransaction>>();
        Dictionary<DateTime, List<StockTransaction>> indexKLine = new Dictionary<DateTime, List<StockTransaction>>();

        public DualTrust2(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void backtest(string underlyingCode, string indexCode,DateTime startDate,DateTime endDate)
        {
            dataPrepare(underlyingCode, indexCode, startDate, endDate);
            //训练集训练参数
            int maxN = 0;
            double maxK1 = 0;
            double maxK2 = 0;
            double maxSharpe = 0;
            for (int n = 2; n <= 4; n++)
            {
                for (double k1 = 0.2; k1 < 1; k1 = k1 + 0.2)
                {
                    for (double k2 = 0.2; k2 < 1; k2 = k2 + 0.2)
                    {
                        double sharpe = getParametersSharpe(startDate, endDate, n, k1, k2);
                        if (sharpe>maxSharpe)
                        {
                            maxN = n;
                            maxK1 = k1;
                            maxK2 = k2;
                            maxSharpe = sharpe;
                            //Console.WriteLine("sharpe:{0}, n:{1}, k1:{2}, k2:{3}", maxSharpe, maxN, maxK1, maxK2);
                        }
                        Console.WriteLine("sharpe:{0}, n:{1}, k1:{2}, k2:{3}", sharpe, n, k1, k2);
                    }
                }
            }
            Console.WriteLine("sharpe:{0}, n:{1}, k1:{2}, k2:{3}", maxSharpe, maxN, maxK1, maxK2);
        }

        //将计算用的数据准备好
        private void dataPrepare(string underlyingCode, string indexCode,DateTime startDate, DateTime endDate,int N=20)
        {
            //获取交易日信息
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取基本信息
            this.indexCode = indexCode;
            this.underlyingCode = underlyingCode;
            //获取日线数据
            var indexData = stockDailyRepo.GetStockTransactionWithRedis(indexCode, startDate, endDate);
            for (int n = 1; n <=N; n++)
            {
                Dictionary<DateTime, double> range = new Dictionary<DateTime, double>();
                for (int i = n; i < indexData.Count() - 1; i++)
                {
                    double HH = 0;//最高价的最高价
                    double HC = 0; //收盘价的最高价
                    double LC = 99999; //收盘价的最低价
                    double LL = 99999; //最低价的最低价
                    for (int k = i-n; k <= i-1; k++)
                    {
                        if (indexData[k].High>HH)
                        {
                            HH = indexData[k].High;
                        }
                        if (indexData[k].Close > HC)
                        {
                            HC = indexData[k].Close;
                        }
                        if (indexData[k].Close < LC)
                        {
                            LC = indexData[k].Close;
                        }
                        if (indexData[k].Low < LL)
                        {
                            LL = indexData[k].Low;
                        }
                    }
                    double rangeNow = Math.Max(HH - LC, HC - LL);
                    range.Add(indexData[i].DateTime, rangeNow);

                }
                RangeDic.Add(n, range);
            }
            //获取分钟线数据
            foreach (var date in tradedays)
            {
                var minuteKLine = stockMinutelyRepo.GetStockTransactionWithRedis(underlyingCode, date, date);
                underlyingKLine.Add(date, minuteKLine);
                var minuteKLine2= stockMinutelyRepo.GetStockTransactionWithRedis(indexCode, date, date);
                indexKLine.Add(date, minuteKLine2);
            }
            
        }

        private double getParametersSharpe(DateTime startDate,DateTime endDate, int N, double K1, double K2, double lossStopRatio=0.008)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<double> yieldList = new List<double>();
            foreach (var date in tradedays)
            {
                double yield = computeDaily(date, N, K1, K2, lossStopRatio);
                yieldList.Add(yield);
            }
            double sharpe = evaluateSharpe(yieldList);
            return sharpe;
        }

        //按照给定参数回测
        private double computeDaily(DateTime date,int N,double K1,double K2,double lossStopRatio)
        {
            double yield = 0;
            if ((RangeDic.ContainsKey(N) && RangeDic[N].ContainsKey(date) && underlyingKLine.ContainsKey(date) && indexKLine.ContainsKey(date))==false)
            {
                return yield;
            }
            double range = RangeDic[N][date];
            var underlying = underlyingKLine[date];
            var index = indexKLine[date];
            double position = 0;
            double positionflag = 0;
            double openPrice = 0;
            double closePrice = 0;
            double longMaxPrice = 0;
            double shortMinPrice = 99999;
            double open = underlying[0].Open;
            double lossStopPoints =Math.Round(lossStopRatio * open/5)*5;
            for (int i = 0; i < index.Count()-30; i++)
            {
                if (position==0 && underlying[i].Open>open+K1*range)
                {
                    openPrice = underlying[i].Open + slipPoint;
                    longMaxPrice = openPrice;
                    positionflag = 1;
                    position = 1;
                }
                if (position==0 && underlying[i].Open<open-K2*range)
                {
                    openPrice = underlying[i].Open - slipPoint;
                    shortMinPrice = openPrice;
                    positionflag = -1;
                    position = -1;
                }
                if (position==1 && underlying[i].Open>longMaxPrice)
                {
                    longMaxPrice = underlying[i].Open;
                }
                if (position==-1 && underlying[i].Open<shortMinPrice)
                {
                    shortMinPrice = underlying[i].Open;
                }
                if (position==1 && underlying[i].Open<longMaxPrice-lossStopPoints)
                {
                    closePrice = underlying[i].Open - slipPoint;
                    position = 0;
                    break;
                }
                if (position==-1 && underlying[i].Open>shortMinPrice+lossStopPoints)
                {
                    closePrice = underlying[i].Open + slipPoint;
                    position = 0;
                    break;
                }
            }
            if (position!=0)
            {
                if (position==1)
                {
                    closePrice= underlying[index.Count() - 3].Open - slipPoint;
                    position = 0;
                }
                else if (position==-1)
                {
                    closePrice = underlying[index.Count() - 3].Open + slipPoint;
                    position = 0;
                }
            }
            if (positionflag!=0)
            {
                yield = (closePrice / openPrice - 1) * positionflag;
            }
            return yield;
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



        //回测结果的记录

    }
}
