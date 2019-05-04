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
    public class RBreakStrategy
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
        private int minutes = 240;
        private Dictionary<DateTime, List<StockTransaction>> underlying = new Dictionary<DateTime, List<StockTransaction>>();
        private List<StockTransaction> underlyingAll = new List<StockTransaction>();

        public RBreakStrategy(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, string code)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            this.code = code;
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void compute(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取日线数据
            var underlyingDaily = stockDailyRepo.GetStockTransactionWithRedis(code, tradedays.First(), tradedays.Last());
            //获取分钟线数据
            foreach (var date in tradedays)
            {
                var underlyingToday = stockMinutelyRepo.GetStockTransactionWithRedis(code, date, date);
                underlying.Add(date, underlyingToday);
                minutes = underlyingToday.Count();
                underlyingAll.AddRange(underlyingToday);
            }
            double bestSharpe = 0;
            double bestf1 = 0.64;
            double bestf2 = 0.48;
            double bestf3=0.08;
            double step = 0.04;

            //for (int i = 1; i <= 1/step; i=i+1)
            //{
            //    for (int j = 1; j <= 1 / step; j = j + 1)
            //    {
            //        for (int k = 1; k <= 1 / step; k = k + 1)
            //        {
            //            double f1 = i * step;
            //            double f2 = j * step;
            //            double f3 = k * step;
            //            double[] netvalue0 = getPerformance(startDate, endDate, tradedays, underlyingDaily, f1, f2, f3);
            //            var nv = getNetValueCurveDaily(getNetValueDaily(underlyingAll, netvalue0));
            //            double sharpe = Utilities.strategyPerformance.sharpeRatioByDailyNetValue(nv);
            //            if (sharpe>bestSharpe)
            //            {
            //                bestf1 = f1;
            //                bestf2 = f2;
            //                bestf3 = f3;
            //                bestSharpe = sharpe;
            //                Console.WriteLine("Best parameters:f1={0}, f2={1}, f3={2}, sharpe={3}", f1, f2, f3, sharpe);
            //            }
            //        }
            //    }
            //}

            double[] netvalue = getPerformance(startDate, endDate, tradedays, underlyingDaily, bestf1, bestf2, bestf3);
            var nvDaily = getNetValueDaily(underlyingAll, netvalue);
            DataTableExtension.SaveCSV(DataTableExtension.ToDataTable<netvalueDaily>(nvDaily), "E:\\result\\break\\netvalue.csv");
            statisticDataOfTransaction(transactionData, tradedays);
            double mean = 0;
            double num = 0;
            for (int i = -5; i <= 5; i = i + 1)
            {
                for (int j = -5; j <= 5; j = j + 1)
                {
                    for (int k = -5; k <= 5; k = k + 1)
                    {
                        double f1 =bestf1+i*step*0.25;
                        double f2 =bestf2+j * step*0.25;
                        double f3 =bestf3+k * step*0.25;
                        if (f1<=0 || f2<=0 || f3<=0)
                        {
                            continue;
                        }
                        double[] netvalue0 = getPerformance(startDate, endDate, tradedays, underlyingDaily, f1, f2, f3);
                        var nv = getNetValueCurveDaily(getNetValueDaily(underlyingAll, netvalue0));
                        double sharpe = Utilities.strategyPerformance.sharpeRatioByDailyNetValue(nv);
                        mean += sharpe;
                        num += 1;
                        Console.WriteLine("parameters around best:f1={0}, f2={1}, f3={2}, sharpe={3}", f1, f2, f3, sharpe);
                    }
                }
            }
            Console.WriteLine("mean:{0}", mean / num);

        }

        public double[] getPerformance(DateTime startDate, DateTime endDate,List<DateTime> tradedays,List<StockTransaction> underlyingDaily,double f1,double f2,double f3)
        {
            var longSignal = new double[underlyingAll.Count()];
            var shortSignal = new double[underlyingAll.Count()];

            for (int i = 1; i < tradedays.Count(); i++)
            {

                var yesterdayData = underlyingDaily[i - 1];
                double buySetup = yesterdayData.Low - f1 * (yesterdayData.High - yesterdayData.Close);
                double sellSetup = yesterdayData.High - f1 * (yesterdayData.Close - yesterdayData.Low);
                double sellEnter = (1 + f2) / 2 * (yesterdayData.High + yesterdayData.Low) - f2 * yesterdayData.Low;
                double buyEnter = (1 + f2) / 2 * (yesterdayData.High + yesterdayData.Low) - f2 * yesterdayData.High;
                double buyBreak = sellSetup + f3 * (sellSetup - buySetup);
                double sellBreak = buySetup + f3 * (sellSetup - buySetup);
                for (int j = 1; j < minutes; j++)
                {
                    int index = i * minutes + j;
                    //多头信号
                    //多头-突破
                    if (underlyingAll[index].Close > buyBreak && underlyingAll[index - 1].Close < buyBreak)
                    {
                        longSignal[index] = 1;
                    }
                    //多头-反转
                    if (underlyingAll[index].Close > buyEnter)
                    {
                        for (int k = index - 1; k >= 0; k--)
                        {
                            if (underlyingAll[k].Close > buyEnter)
                            {
                                break;
                            }
                            if (underlyingAll[k].Close < buySetup)
                            {
                                longSignal[index] = 2;
                                break;
                            }
                        }
                    }
                    //空头信号
                    //空头-突破
                    if (underlyingAll[index].Close < sellBreak && underlyingAll[index - 1].Close > sellBreak)
                    {
                        shortSignal[index] = -1;
                    }
                    //空头-反转
                    if (underlyingAll[index].Close < sellEnter)
                    {
                        for (int k = index - 1; k >= 0; k--)
                        {
                            if (underlyingAll[k].Close < sellEnter)
                            {
                                break;
                            }
                            if (underlyingAll[k].Close > sellSetup)
                            {
                                shortSignal[index] = -2;
                                break;
                            }
                        }
                    }
                }
            }
            var netvalue = computeNetValue(underlyingAll, longSignal, shortSignal, ref transactionData);
            return netvalue;
        }

        private double[] computeNetValue(List<StockTransaction> etf, double[] longSignal, double[] shortSignal, ref List<OneByOneTransaction> data)
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
                if (time.TimeOfDay > new TimeSpan(14, 55, 00) && position != 0) //超过14点55分有仓位，强制平仓
                {
                    cash = cash + (stockPrice * position) - Math.Abs(stockPrice * position * slipRatio);
                    position = 0;
                    stopPrice = 0;
                    shortStopPrice = 0;
                    transaction.closeTime = time;
                    transaction.closePrice = stockPrice;
                    data.Add(transaction);
                    transaction = new OneByOneTransaction();
                }
                if (position == 0 && time.TimeOfDay < new TimeSpan(14, 45, 00) && time.TimeOfDay > new TimeSpan(9, 35, 00)) //未开仓
                {
                    if (longSignal[i] >0) //开多头
                    {
                        position = cash * (1 - slipRatio) / stockPrice;
                        cash = 0;
                        stopPrice = stockPrice * stopRatio;
                        count++;
                        transaction = new OneByOneTransaction();
                        transaction.openTime = time;
                        transaction.openPrice = stockPrice;
                        transaction.position = 1;
                    }
                    else if (shortSignal[i] <0)//开空头
                    {
                        position = -cash * (1 - slipRatio) / stockPrice;
                        cash = cash - position * stockPrice;
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
                    if (position > 0) //已开多仓
                    {
                        //记录追踪止损的点位
                        if (stopPrice < stockPrice * stopRatio)
                        {
                            stopPrice = stockPrice * stopRatio;
                        }
                        if ( stockPrice < stopPrice || shortSignal[i]<0) //平仓或者止损
                        {
                            cash = cash + stockPrice * position - Math.Abs(stockPrice * position * slipRatio);
                            position = 0;
                            stopPrice = 0;
                            transaction.closeTime = time;
                            transaction.closePrice = stockPrice;
                            data.Add(transaction);
                            transaction = new OneByOneTransaction();
                        }
                        if (position==0 && shortSignal[i]==-2 && time.TimeOfDay < new TimeSpan(14, 45, 00)) //平仓之后反手
                        {
                            position = -cash * (1 - slipRatio) / stockPrice;
                            cash = cash - position * stockPrice;
                            shortStopPrice = stockPrice * shortStopRatio;
                            count++;
                            transaction = new OneByOneTransaction();
                            transaction.openTime = time;
                            transaction.openPrice = stockPrice;
                            transaction.position = -1;
                        }
                    }
                    else if (position < 0) //已开空仓
                    {
                        //记录追踪止损的点位
                        if (shortStopPrice > stockPrice * shortStopRatio)
                        {
                            shortStopPrice = stockPrice * shortStopRatio;

                        }
                        if (stockPrice > shortStopPrice || longSignal[i]>0) //平仓或者止损
                        {
                            cash = cash + stockPrice * position - Math.Abs(stockPrice * position * slipRatio);
                            position = 0;
                            shortStopPrice = 0;
                            transaction.closeTime = time;
                            transaction.closePrice = stockPrice;
                            data.Add(transaction);
                            transaction = new OneByOneTransaction();
                        }
                        if (position==0 && longSignal[i]==2 && time.TimeOfDay < new TimeSpan(14, 45, 00)) //平仓之后反手
                        {
                            position = cash * (1 - slipRatio) / stockPrice;
                            cash = 0;
                            stopPrice = stockPrice * stopRatio;
                            count++;
                            transaction = new OneByOneTransaction();
                            transaction.openTime = time;
                            transaction.openPrice = stockPrice;
                            transaction.position = 1;
                        }
                    }
                }
                nv = cash + position * etf[i].Close;
                netvalue[i] = nv;
            }
            return netvalue;
        }

        private void statisticDataOfTransaction(List<OneByOneTransaction> data, List<DateTime> tradeDays)
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
                double r = (item.closePrice - item.openPrice) / item.openPrice * item.position;
                yield.Add(r);
                double time = (item.closeTime - item.openTime).TotalMinutes;
                if (item.openTime.TimeOfDay < new TimeSpan(13, 00, 00) && item.closeTime.TimeOfDay >= new TimeSpan(13, 00, 00))
                {
                    time = time - 90;
                }
                maintain.Add(time);
            }
            var counts = dailyCount.Values.ToList();
            ListToCSV.SaveDataToCSVFile<double>(yield, ListToCSV.CreateFile("E:\\result\\break\\", "yield"), "yield");
            ListToCSV.SaveDataToCSVFile<double>(counts, ListToCSV.CreateFile("E:\\result\\break\\", "counts"), "counts");
            ListToCSV.SaveDataToCSVFile<double>(maintain, ListToCSV.CreateFile("E:\\result\\break\\", "maintain"), "maintain");
            var dt = DataTableExtension.ToDataTable<OneByOneTransaction>(data);
            DataTableExtension.SaveCSV(dt, "E:\\result\\break\\transaction.csv");
        }

        private List<netvalueDaily> getNetValueDaily(List<StockTransaction> etf, double[] netvalue)
        {
            List<netvalueDaily> nv = new List<netvalueDaily>();
            for (int i = 0; i < netvalue.Length; i++)
            {
                if (etf[i].DateTime.TimeOfDay == new TimeSpan(14, 59, 00))
                {
                    var nvtoday = new netvalueDaily();
                    nvtoday.date = etf[i].DateTime.Date;
                    nvtoday.netvalue = netvalue[i];
                    nv.Add(nvtoday);
                }
            }
            return nv;
        }

        private List<double> getNetValueCurveDaily(List<netvalueDaily> nv)
        {
            return nv.Select(s => s.netvalue).ToList();
        }



    }
}
