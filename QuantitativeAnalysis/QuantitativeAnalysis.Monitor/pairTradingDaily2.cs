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
    //利用突破的方法来计算配对交易
    public class pairtradingDaily2
    {

        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code1, code2;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private Dictionary<DateTime, List<StockTransaction>> underlying = new Dictionary<DateTime, List<StockTransaction>>();
        private List<StockTransaction> underlyingAll = new List<StockTransaction>();

        public pairtradingDaily2(StockDailyRepository stockDailyRepo, string code1, string code2)
        {
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            this.code1 = code1;
            this.code2 = code2;
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void compute(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取日线数据
            var underlyingDaily1 = stockDailyRepo.GetStockTransactionWithRedis(code1, tradedays.First(), tradedays.Last());
            var underlyingDaily2 = stockDailyRepo.GetStockTransactionWithRedis(code2, tradedays.First(), tradedays.Last());
            int duration=15;
            double lambda1=2.4;
            double trailingParameter=0.025;
            getParameter(tradedays, underlyingDaily1, underlyingDaily2, ref duration, ref lambda1, ref trailingParameter);
            int length = underlyingDaily1.Count();
            double[] y = new double[length];
            double[] x = new double[length];
            double[] z = new double[length];
            double[] alphaList = new double[length];
            double[] betaList = new double[length];
            double[] cointegrationSequence = new double[length];
            //向上穿越下轨1为1，向上穿越中轨为2，向上穿越上轨1为3，
            //向下穿越下轨1位-1,向下穿越中轨为-2，向下穿越上轨1为-3，
            //向上穿越上轨2为4，向下穿越下轨2为-4，其他为0
            double[] signal = new double[length];
            List<signalWithTime> signalList = new List<signalWithTime>();
            double[] longsignal = new double[length];
            double[] shortsignal = new double[length];
            int duration1 = 1000000;
            double lambda2 = 2.1;
            double y0 = underlyingDaily1[0].Close * underlyingDaily1[0].AdjFactor;
            double x0 = underlyingDaily2[0].Close * underlyingDaily2[0].AdjFactor;
            for (int i = 0; i < tradedays.Count(); i++)
            {
                if ((i - duration1) >= 0)
                {
                    y[i] = underlyingDaily1[i].Close * underlyingDaily1[i].AdjFactor / (underlyingDaily1[i - duration1].Close * underlyingDaily1[i - duration1].AdjFactor);
                    x[i] = underlyingDaily2[i].Close * underlyingDaily2[i].AdjFactor / (underlyingDaily2[i - duration1].Close * underlyingDaily2[i - duration1].AdjFactor);
                }
                else
                {
                    y[i] = underlyingDaily1[i].Close * underlyingDaily1[i].AdjFactor / y0;
                    x[i] = underlyingDaily2[i].Close * underlyingDaily2[i].AdjFactor / x0;
                }
                z[i] = Math.Log(y[i] / x[i]);
            }

            //计算x,y收益率的相关性
            double[] yy = new double[length-1];
            double[] xx = new double[length-1];
            for (int i = 1; i < length; i++)
            {
                yy[i - 1] = y[i] / y[i - 1] - 1;
                xx[i - 1] = x[i] / x[i - 1] - 1;
            }
            double corr = MathUtility.correlation(y, x);
            Console.WriteLine("corr:{0}", corr);
            var myboll = getBollingerBand(z, duration);
            double scale = 0.1;
            List<BollingerBandwithPrice> myboll2 = new List<BollingerBandwithPrice>();
            for (int i = 0; i < myboll.Length; i++)
            {
                if (myboll[i] != null)
                {
                    BollingerBandwithPrice boll0 = new BollingerBandwithPrice();
                    boll0.mean = myboll[i].mean;
                    boll0.std = myboll[i].std;
                    boll0.price = z[i];
                    boll0.time = tradedays[i].Date;
                    boll0.low2 = boll0.mean - lambda2 * boll0.std;
                    boll0.low1 = boll0.mean - lambda1 * boll0.std;
                    boll0.up2 = boll0.mean + lambda2 * boll0.std;
                    boll0.up1 = boll0.mean + lambda1 * boll0.std;
                    if (Math.Abs(boll0.price - boll0.mean) <= scale * boll0.std)
                    {
                        boll0.area = 0;
                    }
                    else if (boll0.price - boll0.mean > scale * boll0.std && boll0.price - boll0.up1 < -scale * boll0.std)
                    {
                        boll0.area = 1;
                    }
                    else if (Math.Abs(boll0.price - boll0.up1) <= scale * boll0.std)
                    {
                        boll0.area = 2;
                    }
                    else if (boll0.price - boll0.up1 > scale * boll0.std && boll0.price - boll0.up2 < -scale * boll0.std)
                    {
                        boll0.area = 3;
                    }
                    else if (Math.Abs(boll0.price - boll0.up2) <= scale * boll0.std)
                    {
                        boll0.area = 4;
                    }
                    else if (boll0.price - boll0.up2 > scale * boll0.std)
                    {
                        boll0.area = 5;
                    }
                    else if (boll0.price - boll0.mean < -scale * boll0.std && boll0.price - boll0.low1 > scale * boll0.std)
                    {
                        boll0.area = -1;
                    }
                    else if (Math.Abs(boll0.price - boll0.low1) <= scale * boll0.std)
                    {
                        boll0.area = -2;
                    }
                    else if (boll0.price - boll0.low1 < -scale * boll0.std && boll0.price - boll0.low2 > scale * boll0.std)
                    {
                        boll0.area = -3;
                    }
                    else if (Math.Abs(boll0.price - boll0.low2) <= scale * boll0.std)
                    {
                        boll0.area = -4;
                    }
                    else if (boll0.price - boll0.low2 < -scale * boll0.std)
                    {
                        boll0.area = -5;
                    }
                    myboll2.Add(boll0);
                }
            }
            var dt = DataTableExtension.ToDataTable(myboll2);
            DataTableExtension.SaveCSV(dt, "E:\\result\\bollinger\\boll.csv");
            for (int i = 1; i < length; i++)
            {
                if (myboll[i - 1] == null || myboll[i - 1].std == 0)
                {
                    signal[i - 1] = 0;
                    continue;
                }
                if (underlyingDaily1[i].TradeStatus != "交易" || underlyingDaily2[i].TradeStatus != "交易")
                {
                    signal[i] = 0;
                    continue;
                }
                double upper1 = myboll[i].mean + lambda1 * myboll[i].std;
                double lower1 = myboll[i].mean - lambda1 * myboll[i].std;
                double middle = myboll[i].mean;
                double upper2 = myboll[i].mean + lambda2 * myboll[i].std;
                double lower2 = myboll[i].mean - lambda2 * myboll[i].std;
                double upper1Previous = myboll[i - 1].mean + lambda1 * myboll[i - 1].std;
                double lower1Previous = myboll[i - 1].mean - lambda1 * myboll[i - 1].std;
                double middlePrevious = myboll[i - 1].mean;
                double upper2Previous = myboll[i - 1].mean + lambda2 * myboll[i - 1].std;
                double lower2Previous = myboll[i - 1].mean - lambda2 * myboll[i - 1].std;
                if (z[i] > lower1 && z[i - 1] <= lower1Previous && z[i] < middle)
                {
                    signal[i] = 1;
                }
                else if (z[i] > middle && z[i - 1] <= middlePrevious && z[i] < upper1)
                {
                    signal[i] = 2;
                }
                else if (z[i] > upper1 && z[i - 1] <= upper1Previous && z[i] < upper2)
                {
                    signal[i] = 3;
                }
                else if (z[i] < lower1 && z[i - 1] >= lower1Previous && z[i] > lower2)
                {
                    signal[i] = -1;
                }
                else if (z[i] < middle && z[i - 1] >= middlePrevious && z[i] > lower1)
                {
                    signal[i] = -2;
                }
                else if (z[i] < upper1 && z[i - 1] >= upper1Previous && z[i] > middle)
                {
                    signal[i] = -3;
                }
                else if (z[i] > upper2 && z[i - 1] <= upper2Previous)
                {
                    signal[i] = 4;
                }
                else if (z[i] < lower2 && z[i - 1] >= lower2Previous)
                {
                    signal[i] = -4;
                }
                else
                {
                    signal[i] = 0;
                }
            }

            for (int i = 0; i < length; i++)
            {
                var signal0 = new signalWithTime();
                signal0.signal = signal[i];
                signal0.time = tradedays[i].Date;
                signalList.Add(signal0);
            }
            List<OneByOneTransaction> data = new List<OneByOneTransaction>();
            List<netvalueDaily> netvalueList = new List<netvalueDaily>();

            bollingerBrand1(underlyingDaily1, underlyingDaily2, signal, duration, myboll2, trailingParameter,ref data, ref netvalueList);
            double sharpe = Utilities.strategyPerformance.sharpeRatioByDailyNetValue(netvalueList.Select(s => s.netvalue).ToList());
            List<OneByOneTransaction> orderedData = data.OrderByDescending(s => Math.Abs(s.closePrice - s.openPrice)).ToList();
            var dt2 = DataTableExtension.ToDataTable(netvalueList);
            DataTableExtension.SaveCSV(dt2, "E:\\result\\bollinger\\nv.csv");
            var dt3 = DataTableExtension.ToDataTable(data);
            DataTableExtension.SaveCSV(dt3, "E:\\result\\bollinger\\transaction.csv");
        }

        private void getParameter(List<DateTime> tradedays, List<StockTransaction> underlyingDaily1, List<StockTransaction> underlyingDaily2,ref int durationMax,ref double lambda1Max,ref double trailingParameterMax)
        {
            double max = 0;
            for (int duration =5;  duration <= 25; duration=duration+5)
            {
                for (double  lambda1 = 1; lambda1 <= 3; lambda1=lambda1+0.2)
                {
                    for (double  trailingParameter = 0.01; trailingParameter <= 0.05; trailingParameter=trailingParameter+0.005)
                    {
                        double sharpe = computeChoiceParameter(tradedays, underlyingDaily1, underlyingDaily2, duration, lambda1, trailingParameter);
                        if (sharpe>max)
                        {
                            max = sharpe;
                            durationMax = duration;
                            lambda1Max = lambda1;
                            trailingParameterMax = trailingParameter;
                            Console.WriteLine("sharpe:{0}, duration:{1}, lambda1:{2}, trailing:{3}", max, durationMax, lambda1Max, trailingParameterMax);
                        }
                    }
                }
            }
        }

        private double computeChoiceParameter(List<DateTime> tradedays,List<StockTransaction> underlyingDaily1,List<StockTransaction> underlyingDaily2,int duration,double lambda1,double trailingParameter)
        {
            int duration1 = 1000000;
            double lambda2 = lambda1+1;
            int length = underlyingDaily1.Count();
            double[] y = new double[length];
            double[] x = new double[length];
            double[] z = new double[length];
            //向上穿越下轨1为1，向上穿越中轨为2，向上穿越上轨1为3，
            //向下穿越下轨1位-1,向下穿越中轨为-2，向下穿越上轨1为-3，
            //向上穿越上轨2为4，向下穿越下轨2为-4，其他为0
            double[] signal = new double[length];
            List<signalWithTime> signalList = new List<signalWithTime>();
            double[] longsignal = new double[length];
            double[] shortsignal = new double[length];
            
            double y0 = underlyingDaily1[0].Close * underlyingDaily1[0].AdjFactor;
            double x0 = underlyingDaily2[0].Close * underlyingDaily2[0].AdjFactor;
            for (int i = 0; i < tradedays.Count(); i++)
            {
                if ((i - duration1) >= 0)
                {
                    y[i] = underlyingDaily1[i].Close * underlyingDaily1[i].AdjFactor / (underlyingDaily1[i - duration1].Close * underlyingDaily1[i - duration1].AdjFactor);
                    x[i] = underlyingDaily2[i].Close * underlyingDaily2[i].AdjFactor / (underlyingDaily2[i - duration1].Close * underlyingDaily2[i - duration1].AdjFactor);
                }
                else
                {
                    y[i] = underlyingDaily1[i].Close * underlyingDaily1[i].AdjFactor / y0;
                    x[i] = underlyingDaily2[i].Close * underlyingDaily2[i].AdjFactor / x0;
                }
                z[i] = Math.Log(y[i] / x[i]);
            }
            var myboll = getBollingerBand(z, duration);
            double scale = 0.1;
            List<BollingerBandwithPrice> myboll2 = new List<BollingerBandwithPrice>();
            for (int i = 0; i < myboll.Length; i++)
            {
                if (myboll[i] != null)
                {
                    BollingerBandwithPrice boll0 = new BollingerBandwithPrice();
                    boll0.mean = myboll[i].mean;
                    boll0.std = myboll[i].std;
                    boll0.price = z[i];
                    boll0.time = tradedays[i].Date;
                    boll0.low2 = boll0.mean - lambda2 * boll0.std;
                    boll0.low1 = boll0.mean - lambda1 * boll0.std;
                    boll0.up2 = boll0.mean + lambda2 * boll0.std;
                    boll0.up1 = boll0.mean + lambda1 * boll0.std;
                    if (Math.Abs(boll0.price - boll0.mean) <= scale * boll0.std)
                    {
                        boll0.area = 0;
                    }
                    else if (boll0.price - boll0.mean > scale * boll0.std && boll0.price - boll0.up1 < -scale * boll0.std)
                    {
                        boll0.area = 1;
                    }
                    else if (Math.Abs(boll0.price - boll0.up1) <= scale * boll0.std)
                    {
                        boll0.area = 2;
                    }
                    else if (boll0.price - boll0.up1 > scale * boll0.std && boll0.price - boll0.up2 < -scale * boll0.std)
                    {
                        boll0.area = 3;
                    }
                    else if (Math.Abs(boll0.price - boll0.up2) <= scale * boll0.std)
                    {
                        boll0.area = 4;
                    }
                    else if (boll0.price - boll0.up2 > scale * boll0.std)
                    {
                        boll0.area = 5;
                    }
                    else if (boll0.price - boll0.mean < -scale * boll0.std && boll0.price - boll0.low1 > scale * boll0.std)
                    {
                        boll0.area = -1;
                    }
                    else if (Math.Abs(boll0.price - boll0.low1) <= scale * boll0.std)
                    {
                        boll0.area = -2;
                    }
                    else if (boll0.price - boll0.low1 < -scale * boll0.std && boll0.price - boll0.low2 > scale * boll0.std)
                    {
                        boll0.area = -3;
                    }
                    else if (Math.Abs(boll0.price - boll0.low2) <= scale * boll0.std)
                    {
                        boll0.area = -4;
                    }
                    else if (boll0.price - boll0.low2 < -scale * boll0.std)
                    {
                        boll0.area = -5;
                    }
                    myboll2.Add(boll0);
                }
            }
            for (int i = 1; i < length; i++)
            {
                if (myboll[i - 1] == null || myboll[i - 1].std == 0)
                {
                    signal[i - 1] = 0;
                    continue;
                }
                if (underlyingDaily1[i].TradeStatus != "交易" || underlyingDaily2[i].TradeStatus != "交易")
                {
                    signal[i] = signal[i - 1];
                    continue;
                }
                double upper1 = myboll[i].mean + lambda1 * myboll[i].std;
                double lower1 = myboll[i].mean - lambda1 * myboll[i].std;
                double middle = myboll[i].mean;
                double upper2 = myboll[i].mean + lambda2 * myboll[i].std;
                double lower2 = myboll[i].mean - lambda2 * myboll[i].std;
                double upper1Previous = myboll[i - 1].mean + lambda1 * myboll[i - 1].std;
                double lower1Previous = myboll[i - 1].mean - lambda1 * myboll[i - 1].std;
                double middlePrevious = myboll[i - 1].mean;
                double upper2Previous = myboll[i - 1].mean + lambda2 * myboll[i - 1].std;
                double lower2Previous = myboll[i - 1].mean - lambda2 * myboll[i - 1].std;
                if (z[i] > lower1 && z[i - 1] <= lower1Previous && z[i] < middle)
                {
                    signal[i] = 1;
                }
                else if (z[i] > middle && z[i - 1] <= middlePrevious && z[i] < upper1)
                {
                    signal[i] = 2;
                }
                else if (z[i] > upper1 && z[i - 1] <= upper1Previous && z[i] < upper2)
                {
                    signal[i] = 3;
                }
                else if (z[i] < lower1 && z[i - 1] >= lower1Previous && z[i] > lower2)
                {
                    signal[i] = -1;
                }
                else if (z[i] < middle && z[i - 1] >= middlePrevious && z[i] > lower1)
                {
                    signal[i] = -2;
                }
                else if (z[i] < upper1 && z[i - 1] >= upper1Previous && z[i] > middle)
                {
                    signal[i] = -3;
                }
                else if (z[i] > upper2 && z[i - 1] <= upper2Previous)
                {
                    signal[i] = 4;
                }
                else if (z[i] < lower2 && z[i - 1] >= lower2Previous)
                {
                    signal[i] = -4;
                }
                else
                {
                    signal[i] = 0;
                }
            }

            for (int i = 0; i < length; i++)
            {
                var signal0 = new signalWithTime();
                signal0.signal = signal[i];
                signal0.time = tradedays[i].Date;
                signalList.Add(signal0);
            }
            List<OneByOneTransaction> data = new List<OneByOneTransaction>();
            List<netvalueDaily> netvalueList = new List<netvalueDaily>();
            bollingerBrand1(underlyingDaily1, underlyingDaily2, signal, duration, myboll2, trailingParameter,ref data, ref netvalueList);
            double sharpe = Utilities.strategyPerformance.sharpeRatioByDailyNetValue(netvalueList.Select(s => s.netvalue).ToList());
            //var dt = DataTableExtension.ToDataTable(myboll2);
            //DataTableExtension.SaveCSV(dt, "E:\\result\\bollinger\\boll.csv");
            //List<OneByOneTransaction> orderedData = data.OrderByDescending(s => Math.Abs(s.closePrice - s.openPrice)).ToList();
            //var dt2 = DataTableExtension.ToDataTable(netvalueList);
            //DataTableExtension.SaveCSV(dt2, "E:\\result\\bollinger\\nv.csv");
            //var dt3 = DataTableExtension.ToDataTable(data);
            //DataTableExtension.SaveCSV(dt3, "E:\\result\\bollinger\\transaction.csv");
            return sharpe;
            
        }

        private BollingerBand[] getBollingerBand(double[] data, int duration)
        {
            BollingerBand[] boll = new BollingerBand[data.Length];
            for (int i = duration; i < data.Length; i++)
            {
                List<double> list = new List<double>();
                for (int j = 0; j < duration; j++)
                {
                    list.Add(data[i - duration + j]);
                }
                boll[i] = new BollingerBand();
                boll[i].mean = list.Average();
                boll[i].std = MathUtility.std(list);
            }
            return boll;
        }
        //第一类布林带的使用，当价格上穿下轨1时候买入，当价格上穿中轨的时候止盈，当价格下穿下轨2的时候止损
        //当价格下穿上轨1时候卖出，当价格下穿中轨的时候止盈，当价格上穿上轨2的时候止损
        private bool bollingerBrand1(List<StockTransaction> underlying1, List<StockTransaction> underlying2, double[] signal, int duration, List<BollingerBandwithPrice> myboll, double trailingParameter,ref List<OneByOneTransaction> data, ref List<netvalueDaily> netvalueList)
        {
            netvalueList = new List<netvalueDaily>();
            data = new List<OneByOneTransaction>();
            OneByOneTransaction transaction = new OneByOneTransaction();
            double position1 = 0;
            double position2 = 0;
            double cash = 1;
            double slipRatio = 0.002;
            double nv = 1;
            double trailingProfit = 0;
            bool trailingStop = false;
            netvalueDaily nvToday = new netvalueDaily();
            nvToday.netvalue = nv;
            nvToday.date = underlying1[0].DateTime;
            bool trade = true;
            int length = signal.Length;
            netvalueList.Add(nvToday);
            for (int i = 1; i < length; i++)
            {
                trade = true;
                if (underlying1[i].TradeStatus != "交易" || underlying2[i].TradeStatus != "交易")
                {
                    trade = false;
                }
                double underlying1AvgPrice = underlying1[i].AdjFactor * underlying1[i].Amount / underlying1[i].Volume;
                double underlying2AvgPrice = underlying2[i].AdjFactor * underlying2[i].Amount / underlying2[i].Volume;
                double underlying1ClosePrice = underlying1[i].AdjFactor * underlying1[i].Close;
                double underlying2ClosePrice = underlying2[i].AdjFactor * underlying2[i].Close;
                double pairPriceYesterday = Math.Log(underlying1[i - 1].AdjFactor * underlying1[i - 1].Close / (underlying2[i - 1].AdjFactor * underlying2[i - 1].Close));
                var today = underlying1[i].DateTime;
                ////计算布林带数据
                //double upRatio = (myboll[i-duration].up1 - myboll[i - duration - 1].up1) / (myboll[i - duration].mean - myboll[i - duration - 1].mean);
                //double lowRatio= (myboll[i - duration].low1 - myboll[i - duration - 1].low1) / (myboll[i - duration].mean - myboll[i - duration - 1].mean);
                //空仓情况,看情况开仓
                if (position1 == 0 && position2 == 0 && trade == true)
                {
                    //上穿下轨1
                    if (signal[i - 1] == 1)
                    {
                        cash = cash * (1 - slipRatio);
                        position1 = cash / 2 / underlying1AvgPrice;
                        position2 = -cash / 2 / underlying2AvgPrice;
                        transaction.openTime = today;
                        transaction.position = 1;
                        transaction.openPrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                        trailingProfit = 0;
                        trailingStop = false;
                    }
                    //下穿上轨1
                    else if (signal[i - 1] == -3)
                    {
                        cash = cash * (1 - slipRatio);
                        position1 = -cash / 2 / underlying1AvgPrice;
                        position2 = cash / 2 / underlying2AvgPrice;
                        transaction.openTime = today;
                        transaction.position = -1;
                        transaction.openPrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                        trailingProfit = 0;
                        trailingStop = false;
                    }
                }
                //多头情况
                else if (position1 > 0 && position2 < 0 && trade == true)
                {
                    //上穿中轨，止盈
                    //if (signal[i-1]==2)
                    //{
                    //    cash += position1 * underlying1AvgPrice * (1 - slipRatio) + position2 * underlying2AvgPrice * (1 + slipRatio);
                    //    position1 = 0;
                    //    position2 = 0;
                    //    transaction.closeTime = today;
                    //    transaction.closePrice=Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                    //    transaction.closeStatus = "上穿中轨，止盈";
                    //    data.Add(transaction);
                    //    transaction = new OneByOneTransaction();
                    //}
                    ////上穿上轨1，止盈
                    //if (signal[i - 1] == 3 || signal[i - 1] == 4)
                    //{
                    //    cash += position1 * underlying1AvgPrice * (1 - slipRatio) + position2 * underlying2AvgPrice * (1 + slipRatio);
                    //    position1 = 0;
                    //    position2 = 0;
                    //    transaction.closeTime = today;
                    //    transaction.closePrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                    //    transaction.closeStatus = "上穿上轨1，止盈";
                    //    data.Add(transaction);
                    //    transaction = new OneByOneTransaction();
                    //}
                    ////下穿下轨2，止损
                    //else if (signal[i - 1] == -4)
                    //{
                    //    cash += position1 * underlying1AvgPrice * (1 - slipRatio) + position2 * underlying2AvgPrice * (1 + slipRatio);
                    //    position1 = 0;
                    //    position2 = 0;
                    //    transaction.closeTime = today;
                    //    transaction.closePrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                    //    transaction.closeStatus = "下穿下轨2，止损";
                    //    data.Add(transaction);
                    //    transaction = new OneByOneTransaction();
                    //}
                    //追踪止损
                    if (trailingStop == true)
                    {
                        cash += position1 * underlying1AvgPrice * (1 - slipRatio) + position2 * underlying2AvgPrice * (1 + slipRatio);
                        position1 = 0;
                        position2 = 0;
                        transaction.closeTime = today;
                        transaction.closePrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                        transaction.closeStatus = "追踪止损";
                        data.Add(transaction);
                        transaction = new OneByOneTransaction();
                        trailingStop = false;
                        trailingProfit = 0;
                    }
                }
                //空头情况
                else if (position1 < 0 && position2 > 0 && trade == true)
                {
                    //下穿中轨，止盈
                    //if (signal[i-1]==-2)
                    //{
                    //    cash += position1 * underlying1AvgPrice * (1 + slipRatio) + position2 * underlying2AvgPrice * (1 - slipRatio);
                    //    position1 = 0;
                    //    position2 = 0;
                    //    transaction.closeTime = today;
                    //    transaction.closePrice =Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                    //    transaction.closeStatus = "下穿中轨，止盈";
                    //    data.Add(transaction);
                    //    transaction = new OneByOneTransaction();
                    //}
                    ////下穿下轨1，止盈
                    //if (signal[i - 1] == -1 || signal[i - 1] == -4)
                    //{
                    //    cash += position1 * underlying1AvgPrice * (1 + slipRatio) + position2 * underlying2AvgPrice * (1 - slipRatio);
                    //    position1 = 0;
                    //    position2 = 0;
                    //    transaction.closeTime = today;
                    //    transaction.closePrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                    //    transaction.closeStatus = "下穿下轨1，止盈";
                    //    data.Add(transaction);
                    //    transaction = new OneByOneTransaction();
                    //}
                    //////上穿上轨2，止损
                    //else if (signal[i - 1] == 4)
                    //{
                    //    cash += position1 * underlying1AvgPrice * (1 + slipRatio) + position2 * underlying2AvgPrice * (1 - slipRatio);
                    //    position1 = 0;
                    //    position2 = 0;
                    //    transaction.closeTime = today;
                    //    transaction.closePrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                    //    transaction.closeStatus = "上穿上轨2，止损";
                    //    data.Add(transaction);
                    //    transaction = new OneByOneTransaction();
                    //}
                    //追踪止损
                    if (trailingStop == true)
                    {
                        cash += position1 * underlying1AvgPrice * (1 + slipRatio) + position2 * underlying2AvgPrice * (1 - slipRatio);
                        position1 = 0;
                        position2 = 0;
                        transaction.closeTime = today;
                        transaction.closePrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                        transaction.closeStatus = "追踪止损";
                        data.Add(transaction);
                        transaction = new OneByOneTransaction();
                        trailingStop = false;
                        trailingProfit = 0;
                    }
                }
                //计算追踪止损点位
                double closePrice = Math.Log(underlying1ClosePrice / underlying2ClosePrice);
                if (position1 > 0 && position2 < 0) //多头
                {
                    if (closePrice > trailingProfit + transaction.openPrice)
                    {
                        trailingProfit = closePrice - transaction.openPrice;
                    }
                    else if (trailingProfit - (closePrice - transaction.openPrice) > trailingParameter)
                    {
                        trailingStop = true;
                    }
                }
                else if (position1 < 0 && position2 > 0) //空头
                {
                    if (closePrice < -trailingProfit + transaction.openPrice)
                    {
                        trailingProfit = -closePrice + transaction.openPrice;
                    }
                    else if (trailingProfit - (-closePrice + transaction.openPrice) > trailingParameter)
                    {
                        trailingStop = true;
                    }
                }
                nv = cash + position1 * underlying1ClosePrice + position2 * underlying2ClosePrice;
                nvToday = new netvalueDaily();
                nvToday.netvalue = nv;
                nvToday.date = today;
                netvalueList.Add(nvToday);
            }
            return true;
        }

        //第二类布林带的使用，当价格上穿上轨1的时候买入，当下轨下穿下轨1的时候卖出
        private bool bollingerBrand2(List<StockTransaction> underlying1, List<StockTransaction> underlying2, double[] signal, int duration, List<BollingerBandwithPrice> myboll, ref List<OneByOneTransaction> data, ref List<netvalueDaily> netvalueList)
        {
            netvalueList = new List<netvalueDaily>();
            data = new List<OneByOneTransaction>();
            OneByOneTransaction transaction = new OneByOneTransaction();
            double position1 = 0;
            double position2 = 0;
            double cash = 1;
            double slipRatio = 0.003;
            double nv = 1;
            double trailingProfit = 0;
            bool trailingStop = false;
            double trailingParameter = 0.02;
            netvalueDaily nvToday = new netvalueDaily();
            nvToday.netvalue = nv;
            nvToday.date = underlying1[0].DateTime;
            bool trade = true;
            int length = signal.Length;
            netvalueList.Add(nvToday);
            for (int i = 1; i < length; i++)
            {
                trade = true;
                if (underlying1[i].TradeStatus != "交易" || underlying2[i].TradeStatus != "交易")
                {
                    trade = false;
                }
                double underlying1AvgPrice = underlying1[i].AdjFactor * underlying1[i].Amount / underlying1[i].Volume;
                double underlying2AvgPrice = underlying2[i].AdjFactor * underlying2[i].Amount / underlying2[i].Volume;
                double underlying1ClosePrice = underlying1[i].AdjFactor * underlying1[i].Close;
                double underlying2ClosePrice = underlying2[i].AdjFactor * underlying2[i].Close;
                double pairPriceYesterday = Math.Log(underlying1[i - 1].AdjFactor * underlying1[i - 1].Close / (underlying2[i - 1].AdjFactor * underlying2[i - 1].Close));
                var today = underlying1[i].DateTime;
                //空仓情况,看情况开仓
                if (position1 == 0 && position2 == 0 && trade == true)
                {
                    //上穿上轨1
                    if (signal[i - 1] == 3)
                    {
                        cash = cash * (1 - slipRatio);
                        position1 = cash / 2 / underlying1AvgPrice;
                        position2 = -cash / 2 / underlying2AvgPrice;
                        transaction.openTime = today;
                        transaction.position = 1;
                        transaction.openPrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                        trailingProfit = 0;
                        trailingStop = false;
                    }
                    //下穿上轨1
                    else if (signal[i - 1] == -1)
                    {
                        cash = cash * (1 - slipRatio);
                        position1 = -cash / 2 / underlying1AvgPrice;
                        position2 = cash / 2 / underlying2AvgPrice;
                        transaction.openTime = today;
                        transaction.position = -1;
                        transaction.openPrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                        trailingProfit = 0;
                        trailingStop = false;
                    }
                }
                //多头情况
                else if (position1 > 0 && position2 < 0 && trade == true)
                {

                    //追踪止损
                    if (trailingStop == true)
                    {
                        cash += position1 * underlying1AvgPrice * (1 - slipRatio) + position2 * underlying2AvgPrice * (1 + slipRatio);
                        position1 = 0;
                        position2 = 0;
                        transaction.closeTime = today;
                        transaction.closePrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                        transaction.closeStatus = "追踪止损";
                        data.Add(transaction);
                        transaction = new OneByOneTransaction();
                        trailingStop = false;
                        trailingProfit = 0;
                    }
                }
                //空头情况
                else if (position1 < 0 && position2 > 0 && trade == true)
                {
                    //追踪止损
                    if (trailingStop == true)
                    {
                        cash += position1 * underlying1AvgPrice * (1 + slipRatio) + position2 * underlying2AvgPrice * (1 - slipRatio);
                        position1 = 0;
                        position2 = 0;
                        transaction.closeTime = today;
                        transaction.closePrice = Math.Log(underlying1AvgPrice / underlying2AvgPrice);
                        transaction.closeStatus = "追踪止损";
                        data.Add(transaction);
                        transaction = new OneByOneTransaction();
                        trailingStop = false;
                        trailingProfit = 0;
                    }
                }
                //计算追踪止损点位
                double closePrice = Math.Log(underlying1ClosePrice / underlying2ClosePrice);
                if (position1 > 0 && position2 < 0) //多头
                {
                    if (closePrice > trailingProfit + transaction.openPrice)
                    {
                        trailingProfit = closePrice - transaction.openPrice;
                    }
                    else if (trailingProfit - (closePrice - transaction.openPrice) > trailingParameter)
                    {
                        trailingStop = true;
                    }
                }
                else if (position1 < 0 && position2 > 0) //空头
                {
                    if (closePrice < -trailingProfit + transaction.openPrice)
                    {
                        trailingProfit = -closePrice + transaction.openPrice;
                    }
                    else if (trailingProfit - (-closePrice + transaction.openPrice) > trailingParameter)
                    {
                        trailingStop = true;
                    }
                }
                nv = cash + position1 * underlying1ClosePrice + position2 * underlying2ClosePrice;
                nvToday = new netvalueDaily();
                nvToday.netvalue = nv;
                nvToday.date = today;
                netvalueList.Add(nvToday);
            }
            return true;
        }

        //利用OLS方法计算相关性
        //double alpha = 0, beta = 0;
        //for (int i = 0; i < tradedays.Count()-m; i++)
        //{
        //    for (int j  = 0; j < m; j++)
        //    {
        //        int k = i + j;
        //        y[k] = Math.Log(underlyingDaily1[k].Close * underlyingDaily1[k].AdjFactor);
        //        x[k] = Math.Log(underlyingDaily2[k].Close * underlyingDaily2[k].AdjFactor);
        //    }
        //    MathUtility.OLS(x, y, ref alpha, ref beta);
        //    alphaList[i + m] = alpha;
        //    betaList[i + m] = beta;
        //   // Console.WriteLine("Date:{2},alpha:{0},beta:{1}", alpha, beta,tradedays[i].Date.ToString());
        //}
        //for (int i = 0; i < tradedays.Count(); i++)
        //{
        //    cointegrationSequence[i] = y[i] - 1.5 * x[i];
        //}
        //for (int i = 0; i < tradedays.Count(); i++)
        //{
        //    pairDifference difference0 = new pairDifference();
        //    difference0.code1 = code1;
        //    difference0.code2 = code2;
        //    difference0.date = tradedays[i].Date;
        //    difference0.difference =cointegrationSequence[i];
        //    difference.Add(difference0);
        //}
        //DataTableExtension.SaveCSV(DataTableExtension.ToDataTable<pairDifference>(difference), "E:\\result\\pairtrading\\difference.csv");

    }
}
