/* Dual Thrust是一个趋势跟踪系统，由Michael Chalek在20世纪80年代开发。
 * Dual Thrust系统具有简单易用、适用度广的特点，其思路简单、参数很少，配合不同的参数、
 * 止盈止损和仓位管理，可以为投资者带来长期稳定的收益，
 * 被投资者广泛应用于股票、货币、贵金属、债券、能源及股指期货市场等。
 * 在Dual Thrust交易系统中，对于震荡区间的定义非常关键，这也是该交易系统的核心和精髓。
 * Dual Thrust系统使用Range = Max(HH-LC,HC-LL)来描述震荡区间的大小。
 * 其中HH是开盘前N分钟High的最高价，LC是开盘前N分钟Close的最低价，HC是开盘前N分钟N日Close的最高价，LL是开盘前N分钟N日Low的最低价。
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

namespace QuantitativeAnalysis.Monitor.StockIntraday.DualTrust
{
    public class DualTrust
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
        private Dictionary<DateTime, List<StockTransaction>> underlying = new Dictionary<DateTime, List<StockTransaction>>();
        private List<StockTransaction> underlyingAll = new List<StockTransaction>();
        private double slipRatio = 0.0006;
        private double feeRatioBuy = 0.0001;
        private double feeRatioSell = 0.0011;
        private double priceUnit = 0.01;
        private double loss = 0.03;
        //预处理计算N天的range值
        Dictionary<int, Dictionary<DateTime, double>> RangeDic = new Dictionary<int, Dictionary<DateTime, double>>();
        //预处理分钟线数据
        Dictionary<DateTime, List<StockTransaction>> underlyingKLine = new Dictionary<DateTime, List<StockTransaction>>();

        public DualTrust(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);

        }

        public void backtest(string underlyingCode,DateTime startDate, DateTime endDate)
        {
            dataPrepare(underlyingCode, startDate, endDate);
            //训练集训练参数
            int maxN = 0;
            double maxK1 = 0;
            double maxK2 = 0;
            double maxSharpe = -10;

            for (int n = 2; n <= 4; n++)
            {
                for (double k1 = 0.2; k1 < 1; k1 = k1 + 0.2)
                {
                    for (double k2 = 0.2; k2 < 1; k2 = k2 + 0.2)
                    {
                        double sharpe = getParametersSharpe(startDate, endDate, n, k1, k2,loss);
                        if (sharpe > maxSharpe)
                        {
                            maxN = n;
                            maxK1 = k1;
                            maxK2 = k2;
                            maxSharpe = sharpe;
                            //Console.WriteLine("sharpe:{0}, n:{1}, k1:{2}, k2:{3}", maxSharpe, maxN, maxK1, maxK2);
                        }
                        Console.WriteLine("rating:{0}, n:{1}, k1:{2}, k2:{3}", sharpe, n, k1, k2);
                    }
                }
            }
            Console.WriteLine("rating:{0}, n:{1}, k1:{2}, k2:{3}", maxSharpe, maxN, maxK1, maxK2);
            var yield = getYieldList(startDate, endDate, maxN, maxK1, maxK2,loss);
            yield = getYieldList(startDate, endDate, maxN, maxK1, maxK2,loss);
            double nv = 1;
            for (int i = 0; i < yield.Count(); i++)
            {
                nv = nv * (yield[i] + 1);
            }
            Console.WriteLine(nv);
            this.transactionData = getTransactionData(startDate, endDate, maxN, maxK1, maxK2,loss);
            var dt = DataTableExtension.ToDataTable(transactionData);
            var codeStr = underlyingCode.Split('.');
            string name = string.Format("E:\\result\\DualTrust\\{0}.csv", codeStr[0]);
            DataTableExtension.SaveCSV(dt, name);
         }

        //将计算用的数据准备好
        private void dataPrepare(string underlyingCode,DateTime startDate, DateTime endDate, int N = 20)
        {
            //获取交易日信息
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取基本信息
            this.underlyingCode = underlyingCode;
            //获取日线数据
            var underlyingData = stockDailyRepo.GetStockTransactionWithRedis(underlyingCode, startDate, endDate);
            for (int n = 1; n <= N; n++)
            {
                Dictionary<DateTime, double> range = new Dictionary<DateTime, double>();
                for (int i = n; i < underlyingData.Count() - 1; i++)
                {
                    double HH = 0;//最高价的最高价
                    double HC = 0; //收盘价的最高价
                    double LC = 99999; //收盘价的最低价
                    double LL = 99999; //最低价的最低价
                    for (int k = i - n; k <= i - 1; k++)
                    {
                        if (underlyingData[k].High > HH)
                        {
                            HH = underlyingData[k].High;
                        }
                        if (underlyingData[k].Close > HC)
                        {
                            HC = underlyingData[k].Close;
                        }
                        if (underlyingData[k].Close < LC)
                        {
                            LC = underlyingData[k].Close;
                        }
                        if (underlyingData[k].Low < LL)
                        {
                            LL = underlyingData[k].Low;
                        }
                    }
                    double rangeNow = Math.Max(HH - LC, HC - LL);
                    range.Add(underlyingData[i].DateTime, rangeNow);

                }
                RangeDic.Add(n, range);
            }

            //获取分钟线数据
            foreach (var date in tradedays)
            {
                var minuteKLine = stockMinutelyRepo.GetStockTransactionWithRedis(underlyingCode, date, date);
                underlyingKLine.Add(date, minuteKLine);
            }
        }

        private List<double> getYieldList(DateTime startDate, DateTime endDate, int N, double K1, double K2, double lossStopRatio = 0.008)
        {
            List<double> yieldList = new List<double>();
            int num = 0;
            int right = 0;
            int bigWin = 0;
            int total = 0;
            double range = 0;
            double range0 = 0;
            double range1 = 0;
            double range2 = 0;
            foreach (var date in tradedays)
            {
                var yield = computeDaily(date, N, K1, K2, lossStopRatio);
                yieldList.Add(yield);
                if (RangeDic[N].ContainsKey(date))
                {
                    total++;
                    range += RangeDic[N][date];
                    if (yield != 0)
                    {
                        num++;
                        range0 += RangeDic[N][date];
                        if (yield > 0)
                        {
                            right++;
                            range1 += RangeDic[N][date];
                        }
                        if (yield > 0.01)
                        {
                            range2 += RangeDic[N][date];
                            bigWin++;
                        }
                    }
                }
            }
            range /= total;
            range0 /= num;
            range1 /= right;
            range2 /= bigWin;
            double rate = right / (double)num;
            Console.WriteLine("num:{0}, winrate:{1}, range: total-{2}, open-{3}, right-{4}, bigwin-{5}", num, rate,range,range0,range1,range2);
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
            return sharpe+returnAVG*3;
        }

        private List<OneByOneTransaction> getTransactionData(DateTime startDate, DateTime endDate, int N, double K1, double K2, double lossStopRatio = 0.02)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<OneByOneTransaction> list = new List<OneByOneTransaction>();
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
            double yield = 0;
            if ((RangeDic.ContainsKey(N) && RangeDic[N].ContainsKey(date) && underlyingKLine.ContainsKey(date) ) == false)
            {
                return yield;
            }
            double range = RangeDic[N][date];
            var underlying = underlyingKLine[date];
            double position = 0;
            double positionflag = 0;
            double openPrice = 0;
            double closePrice = 0;
            double longMaxPrice = 0;
            double shortMinPrice = 99999;
            double open = underlying[0].Open;
            double lossStopPoints = lossStopRatio * open;
            double slipBuy = 0;
            double slipSell = 0;
            for (int i = 1; i < underlying.Count() - 30; i++)
            {
                slipBuy = Math.Max(slipRatio * underlying[i].Open, priceUnit) + feeRatioBuy * underlying[i].Open;
                slipSell = Math.Max(slipRatio * underlying[i].Open, priceUnit) + feeRatioSell * underlying[i].Open;
                if (position == 0 && underlying[i].Open > open + K1 * range && underlying[i].Amount>0)
                {
                    openPrice = underlying[i].Open + slipBuy;
                    longMaxPrice = openPrice;
                    positionflag = 1;
                    position = 1;
                }
                if (position == 0 && underlying[i].Open < open - K2 * range && underlying[i].Amount > 0)
                {
                    openPrice = underlying[i].Open - slipSell;
                    shortMinPrice = openPrice;
                    positionflag = -1;
                    position = -1;
                }
                if (position == 1 && underlying[i].Open > longMaxPrice && underlying[i].Amount > 0)
                {
                    longMaxPrice = underlying[i].Open;
                }
                if (position == -1 && underlying[i].Open < shortMinPrice && underlying[i].Amount > 0)
                {
                    shortMinPrice = underlying[i].Open;
                }
                if (position == 1 && underlying[i].Open < longMaxPrice - lossStopPoints && underlying[i].Amount > 0)
                {
                    closePrice = underlying[i].Open - slipSell;
                    position = 0;
                    break;
                }
                if (position == -1 && underlying[i].Open > shortMinPrice + lossStopPoints && underlying[i].Amount > 0)
                {
                    closePrice = underlying[i].Open + slipBuy;
                    position = 0;
                    break;
                }
            }
            if (position != 0)
            {
                if (position == 1)
                {
                    closePrice = underlying[underlying.Count() - 3].Open - slipSell;
                    position = 0;
                }
                else if (position == -1)
                {
                    closePrice = underlying[underlying.Count() - 3].Open + slipBuy;
                    position = 0;
                }
            }
            if (positionflag != 0)
            {
                yield = (closePrice / openPrice - 1) * positionflag;
            }
            return yield;
        }


        //按照给定参数回测
        private OneByOneTransaction computeDailyWithRecord(DateTime date, int N, double K1, double K2, double lossStopRatio)
        {
            OneByOneTransaction result = new OneByOneTransaction();
            if ((RangeDic.ContainsKey(N) && RangeDic[N].ContainsKey(date) && underlyingKLine.ContainsKey(date)) == false)
            {
                return result;
            }
            result.date = date;
            double yield = 0;
            double range = RangeDic[N][date];
            result.parameter = range;
            var underlying = underlyingKLine[date];
            double position = 0;
            double positionflag = 0;
            double openPrice = 0;
            double closePrice = 0;
            double longMaxPrice = 0;
            double shortMinPrice = 99999;
            double open = underlying[0].Open;
            double lossStopPoints = lossStopRatio * open;
            double slipBuy = 0;
            double slipSell = 0;
            for (int i = 1; i < underlying.Count() - 30; i++)
            {
                slipBuy = Math.Max(slipRatio * underlying[i].Open, priceUnit) + feeRatioBuy * underlying[i].Open;
                slipSell = Math.Max(slipRatio * underlying[i].Open, priceUnit) + feeRatioSell * underlying[i].Open;
                if (position == 0 && underlying[i].Open > open + K1 * range && underlying[i].Amount > 0)
                {
                    openPrice = underlying[i].Open + slipBuy;
                    longMaxPrice = openPrice;
                    positionflag = 1;
                    position = 1;
                    result.openTime = underlying[i].DateTime;
                    result.openPrice = openPrice;
                    result.position = 1;
                }
                if (position == 0 && underlying[i].Open < open - K2 * range && underlying[i].Amount > 0)
                {
                    openPrice = underlying[i].Open - slipSell;
                    shortMinPrice = openPrice;
                    positionflag = -1;
                    position = -1;
                    result.openTime = underlying[i].DateTime;
                    result.openPrice = openPrice;
                    result.position = -1;
                }
                if (position == 1 && underlying[i].Open > longMaxPrice && underlying[i].Amount > 0)
                {
                    longMaxPrice = underlying[i].Open;
                }
                if (position == -1 && underlying[i].Open < shortMinPrice && underlying[i].Amount > 0)
                {
                    shortMinPrice = underlying[i].Open;
                }
                if (position == 1 && underlying[i].Open < longMaxPrice - lossStopPoints && underlying[i].Amount > 0)
                {
                    closePrice = underlying[i].Open - slipSell;
                    position = 0;
                    result.closeTime = underlying[i].DateTime;
                    result.closePrice = closePrice;
                    result.closeStatus = "追踪止损";
                    break;
                }
                if (position == -1 && underlying[i].Open > shortMinPrice + lossStopPoints && underlying[i].Amount > 0)
                {
                    closePrice = underlying[i].Open + slipBuy;
                    position = 0;
                    result.closeTime = underlying[i].DateTime;
                    result.closePrice = closePrice;
                    result.closeStatus = "追踪止损";
                    break;
                }
            }
            if (position != 0)
            {
                if (position == 1)
                {
                    closePrice = underlying[underlying.Count() - 3].Open - slipSell;
                    position = 0;
                    result.closeTime = underlying[underlying.Count() - 3].DateTime;
                    result.closePrice = closePrice;
                    result.closeStatus = "收盘强平";
                }
                else if (position == -1)
                {
                    closePrice = underlying[underlying.Count() - 3].Open + slipBuy;
                    position = 0;
                    result.closeTime = underlying[underlying.Count() - 3].DateTime;
                    result.closePrice = closePrice;
                    result.closeStatus = "收盘强平";
                }
            }
            if (positionflag != 0)
            {
                yield = (closePrice / openPrice - 1) * positionflag;
            }
            result.yield = yield;
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

        //回测结果的记录

    }
}
