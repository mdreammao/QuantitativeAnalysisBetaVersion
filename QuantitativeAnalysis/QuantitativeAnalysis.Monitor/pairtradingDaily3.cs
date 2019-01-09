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
    public class pairtradingDaily3
    {

        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code1, code2;
        private string stockBoard;
        private List<DateTime> tradedays;
        private DateTime startTime, endTime;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private Dictionary<DateTime, List<StockTransaction>> underlying = new Dictionary<DateTime, List<StockTransaction>>();
        private Dictionary<string, List<StockTransaction>> underlyingAllStocks = new Dictionary<string, List<StockTransaction>>();

        public pairtradingDaily3(StockDailyRepository stockDailyRepo, string stockBoard,DateTime startTime,DateTime endTime)
        {
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
            this.stockBoard = stockBoard;
            this.startTime = startTime;
            this.endTime = endTime;
            this.tradedays = dateRepo.GetStockTransactionDate(startTime, endTime);
        }

        private List<pairOfStocks> getStockPairs(string stockBoard, DateTime startTime, DateTime endTime)
        {
            List<pairOfStocks> pairs = new List<pairOfStocks>();
            return pairs;
        }

        private double getStockPairsPerformanceGlobal(pairOfStocks pair)
        {
            double sharpe=0;
            return sharpe;
        }
        
        private double getStockPairsPerformanceGivenPeriod(string code1,string code2,DateTime startTime,DateTime endTime)
        {
            double sharpe = 0;
            return sharpe;
        }

        private double getStockParisPerformanceGivenAllParameters(string code1, string code2, DateTime startTime, DateTime endTime,int duration,int period,double lambda,double trailingRatio)
        {
            double sharpe = 0;
            var stock1 = underlyingAllStocks[code1];
            var stock2 = underlyingAllStocks[code2];



            return sharpe;
        }

        //underlying的时间必须对齐
        private List<BollingerBandWithVarietyScale> getBollingerBandWithVarietyScales(double[] underlying1, double[] underlying2,List<DateTime> days,int period,int duration,double lambda)
        {
            List<BollingerBandWithVarietyScale> myBoll = new List<BollingerBandWithVarietyScale>();
            //找到标的股票对应的开始时间和结束时间
            int length = underlying1.Count();
            for (int i = period-1; i < length; i++)
            {
                BollingerBandWithVarietyScale boll = new BollingerBandWithVarietyScale();
                //先计算k
                double[] y = new double[period];
                double[] x = new double[period];
                List<double> epsilon = new List<double>();
                for (int j = i-period+1; j <=i; j++)
                {
                    y[j - i + period - 1] = underlying1[j];
                    x[j - i + period - 1] = underlying2[j];
                }
                double k = getSacleParameter(y, x);
                //在计算mean和std
                for (int j = 0;j<duration; j++)
                {
                    int index = period + j - duration-1;
                    epsilon.Add(y[index] - k * x[index]);
                }
                double mean = epsilon.Average();
                double std = MathUtility.std(epsilon);
                boll.mean = mean;
                boll.std = std;
                boll.up = mean + lambda * std;
                boll.low = mean - lambda * std;
                boll.k = k;
                boll.date = days[i];
                boll.modifiedPrice = epsilon.Last();
                boll.orignalPrice = y.Last() - x.Last();
                boll.lambda = lambda;
                myBoll.Add(boll);
            }
            return myBoll;
        }


        private double getSacleParameter(double[]y,double[]x)
        {
            double k = 0,b=0;
            MathUtility.OLS(x, y, ref b, ref k);
            return k;
        }


    }
}
