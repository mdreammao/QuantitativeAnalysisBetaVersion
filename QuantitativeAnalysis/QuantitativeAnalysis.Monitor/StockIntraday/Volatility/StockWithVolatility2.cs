/* stockWithVolatility策略是根据股票波动率来进行交易的股票日内策略
 * 先计算股票日间波动率
 * 该策略基本思路是，当标的价格突破日间波动率若干倍标准差的时候开仓，
 * 向上突破出多头信号，向下突破出空头信号
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

namespace QuantitativeAnalysis.Monitor.StockIntraday.Volatility
{
    public class StockWithVolatility2
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
        private double slipRatio = 0.0001;
        private double feeRatioBuy = 0.0001;
        private double feeRatioSell = 0.0001;
        private double priceUnit = 0.001;

        //预处理计算N天的vol值
        Dictionary<int, Dictionary<DateTime, double>> volDic = new Dictionary<int, Dictionary<DateTime, double>>();
        //预处理分钟线数据
        Dictionary<DateTime, List<StockTransaction>> underlyingKLine = new Dictionary<DateTime, List<StockTransaction>>();

        public StockWithVolatility2(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo,TransactionDateTimeRepository dateRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.dateRepo = dateRepo;
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void backtest(string underlyingCode, DateTime startDate, DateTime endDate)
        {
            dataPrepare(underlyingCode, startDate, endDate);
            //训练集训练参数
            int maxN = 0;
            double maxK1 = 0;
            double maxK2 = 0;
            double maxSharpe = -1000;
            for (int n = 2; n <= 10; n++)
            {
                for (double k1 = 0.2; k1 < 1; k1 = k1 + 0.2)
                {
                    for (double k2 = 0.2; k2 < 1; k2 = k2 + 0.2)
                    {
                        double sharpe = getParametersPerformance(startDate, endDate, n, k1, k2,0.01);
                        if (sharpe > maxSharpe)
                        {
                            maxN = n;
                            maxK1 = k1;
                            maxK2 = k2;
                            maxSharpe = sharpe;
                            Console.WriteLine("score:{0}, n:{1}, k1:{2}, k2:{3}", maxSharpe, maxN, maxK1, maxK2);
                        }
                    }
                }
            }
            Console.WriteLine("score:{0}, n:{1}, k1:{2}, k2:{3}", maxSharpe, maxN, maxK1, maxK2,0.01);
            var yield = getYieldList(startDate, endDate, maxN, maxK1, maxK2);
            yield = getYieldList(startDate, endDate, maxN, maxK1, maxK2);
            double nv = 1;
            for (int i = 0; i < yield.Count(); i++)
            {
                nv = nv * (yield[i] + 1);
            }
            Console.WriteLine(nv);
            this.transactionData = getTransactionData(startDate, endDate, maxN, maxK1, maxK2,0.01);
            var dt = DataTableExtension.ToDataTable(transactionData);
            var codeStr = underlyingCode.Split('.');
            string name = string.Format("E:\\result\\stockVol2\\{0}.csv", codeStr[0]);
            DataTableExtension.SaveCSV(dt, name);
        }

        //将计算用的数据准备好
        private void dataPrepare(string underlyingCode, DateTime startDate, DateTime endDate, int N = 20)
        {
            //获取交易日信息
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取基本信息
            this.underlyingCode = underlyingCode;
            //获取日线数据
            var underlyingData = stockDailyRepo.GetStockTransaction(underlyingCode, startDate, endDate);
            for (int n = 1; n <= N; n++)
            {
                Dictionary<DateTime, double> vol = new Dictionary<DateTime, double>();
                for (int i = n; i < underlyingData.Count() - 1; i++)
                {
                    List<double> openToClose = new List<double>();
                    for (int j = Math.Max(0,i-n); j < i; j++)
                    {
                        if (underlyingData[j].Volume>0)
                        {
                            openToClose.Add(underlyingData[j].Close / underlyingData[j].Open - 1);
                        }
                    }
                    if (openToClose.Count()>n/2)
                    {
                        double volNow = getVol(openToClose);
                        if (volNow>0)
                        {
                            vol.Add(underlyingData[i].DateTime, volNow);
                        }
                    }
                }
                volDic.Add(n, vol);
            }

            //获取分钟线数据
            foreach (var date in tradedays)
            {
                var minuteKLine = stockMinutelyRepo.GetStockTransaction(underlyingCode, date, date);
                underlyingKLine.Add(date, minuteKLine);
            }
        }

        private double getVol(List<double> list)
        {
            double vol = 0;
            vol = list.Average();
            return vol;
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
                if (volDic[N].ContainsKey(date))
                {
                    total++;
                    range += volDic[N][date];
                    if (yield != 0)
                    {
                        num++;
                        range0 += volDic[N][date];
                        if (yield > 0)
                        {
                            right++;
                            range1 += volDic[N][date];
                        }
                        if (yield > 0.01)
                        {
                            range2 += volDic[N][date];
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
            Console.WriteLine("num:{0}, winrate:{1}, range: total-{2}, open-{3}, right-{4}, bigwin-{5}", num, rate, range, range0, range1, range2);
            return yieldList;
        }


        private double getParametersPerformance(DateTime startDate, DateTime endDate, int N, double K1, double K2, double lossStopRatio = 0.008)
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
            return sharpe + returnAVG * 5;
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

        //按照给定参数回测
        private OneByOneTransaction computeDailyWithRecord(DateTime date, int N, double K1, double K2, double lossStopRatio)
        {
            OneByOneTransaction result = new OneByOneTransaction();
            if ((volDic.ContainsKey(N) && volDic[N].ContainsKey(date) && underlyingKLine.ContainsKey(date)) == false)
            {
                return result;
            }
            result.date = date;
            double yield = 0;
            double vol = volDic[N][date];
            var underlying = underlyingKLine[date];
            
            double position = 0;
            double positionflag = 0;
            double openPrice = 0;
            double closePrice = 0;
            double longMaxPrice = 0;
            double shortMinPrice = 99999;
            double open = underlying[N].Open;
            double range = open * vol;
            result.parameter = vol;
            double lossStopPoints = lossStopRatio * open;
            double slipBuy = 0;
            double slipSell = 0;
            for (int i = 1; i < 60; i++)
            {
                if (vol<0.005)
                {
                    continue;
                }
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
                slipBuy = Math.Max(slipRatio * underlying[underlying.Count() - 3].Open, priceUnit) + feeRatioBuy * underlying[underlying.Count() - 3].Open;
                slipSell = Math.Max(slipRatio * underlying[underlying.Count() - 3].Open, priceUnit) + feeRatioSell * underlying[underlying.Count() - 3].Open;
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

        //按照给定参数回测
        private double computeDaily(DateTime date, int N, double K1, double K2, double lossStopRatio)
        {
            double yield = 0;
            var transaction = computeDailyWithRecord(date, N, K1, K2, lossStopRatio);
            yield = transaction.yield;
            return yield;
        }


        private List<OneByOneTransaction> getTransactionData(DateTime startDate, DateTime endDate, int N, double K1, double K2, double lossStopRatio = 0.008)
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
    }
}
