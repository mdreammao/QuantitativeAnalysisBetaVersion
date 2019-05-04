/* stockWithVolatility策略是根据股票波动率来进行交易的股票日内策略
 * 先计算股票日间波动率和分钟间波动率
 * 该策略基本思路是，当标的价格突破分钟波动率1倍标准差的时候，
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
    public class StockWithVolatility1
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
        //提前算好历史波动率存储起来
        private Dictionary<DateTime, double> volatilityDaily = new Dictionary<DateTime, double>();
        private Dictionary<DateTime, double> volatilityMinutely = new Dictionary<DateTime, double>();
        //预处理分钟线数据
        Dictionary<DateTime, List<StockTransaction>> underlyingKLine = new Dictionary<DateTime, List<StockTransaction>>();
        Dictionary<DateTime, List<StockTransaction>> indexKLine = new Dictionary<DateTime, List<StockTransaction>>();
        //记录分钟线上的信号
        Dictionary<DateTime, List<double>> signal = new Dictionary<DateTime, List<double>>();

        public StockWithVolatility1(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void backtest(string underlyingCode, string indexCode, DateTime startDate, DateTime endDate)
        {
            prepare(underlyingCode,indexCode, startDate, endDate);
            computeSignal(underlyingKLine);
        }

        private void prepare(string underlyingCode, string indexCode, DateTime startDate, DateTime endDate)
        {
            //获取交易日信息
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            //获取基本信息
            this.indexCode = indexCode;
            this.underlyingCode = underlyingCode;
            //获取日线数据
            var indexData = stockDailyRepo.GetStockTransactionWithRedis(indexCode, startDate, endDate);
            volatilityDaily=getDailyVolatility(indexData);
            //获取分钟线数据
            foreach (var date in tradedays)
            {
                var minuteKLine = stockMinutelyRepo.GetStockTransactionWithRedis(underlyingCode, date, date);
                underlyingKLine.Add(date, minuteKLine);
                var minuteKLine2 = stockMinutelyRepo.GetStockTransactionWithRedis(indexCode, date, date);
                indexKLine.Add(date, minuteKLine2);
            }
            volatilityMinutely = getMinutelyVolatility(indexKLine,1);
        }

        private Dictionary<DateTime, List<double>> computeSignal(Dictionary<DateTime, List<StockTransaction>> data)
        {
            Dictionary<DateTime, List<double>> signal = new Dictionary<DateTime, List<double>>();
            foreach (var item in data)
            {
                var today = item.Key;
                var KLine = item.Value;
                if (volatilityMinutely.ContainsKey(today))
                {
                    double vol = volatilityMinutely[today];
                    double volPerMinute = vol / Math.Sqrt(252 * 240);
                    for (int i = 1; i < 240; i++)
                    {
                        double openPrice = KLine[i].Open;
                        double up = 0;
                        double down = 0;
                        double signal0 = 0;
                        for (int j = Math.Max(0,i-5); j < i; j++)
                        {
                            double openPircePrevious = KLine[j].Open;
                            double difference =Math.Log(openPrice/openPircePrevious);
                            double mystd = volPerMinute * Math.Sqrt(i - j);
                            double k1 = 1;
                            double k2 = 100;
                            
                            if (difference>k1*mystd && difference<=k2*mystd)
                            {
                                up += 1;
                            }
                            else if (difference>k2*mystd)
                            {
                                down -= 1;
                            }
                            else if (difference<-k1*mystd && difference>-k2*mystd)
                            {
                                down -= 1;
                            }
                            else if (difference<-k2*mystd)
                            {
                                up += 1;
                            }
                        }
                        if (up+down>=3)
                        {
                            signal0 = 1;
                        }
                        else if (up+down<=-3)
                        {
                            signal0 = -1;
                        }
                        Console.WriteLine("time:{0}, up:{1}, down:{2}, total:{3}, signal:{4}, price:{5}", KLine[i].DateTime,up,down,up+down,signal0,openPrice);
                    }
                }
            }
            return signal;
        }



        private Dictionary<DateTime, double> getDailyVolatility(List<StockTransaction> data,int N=30)
        {
            Dictionary<DateTime, double> vol = new Dictionary<DateTime, double>();
            N = N + 1;
            int n = data.Count();
            for (int i = N; i < n; i++)
            {
                List<double> yieldList = new List<double>();
                for (int j = i-N+1; j < i; j++)
                {
                    if (data[j].Amount>0)
                    {
                        double yield =Math.Log(data[j].Close/data[j-1].Close);
                        yieldList.Add(yield);
                    }
                }
                if (yieldList.Count>N/2)
                {
                    double volToday = MathUtility.std(yieldList)*Math.Sqrt(252);
                    vol.Add(data[i].DateTime, volToday);
                }
            }
            return vol;
        }

        private Dictionary<DateTime,double> getMinutelyVolatility(Dictionary<DateTime, List<StockTransaction>> data,int N=30)
        {
            Dictionary<DateTime, double> vol = new Dictionary<DateTime, double>();
            N = N + 1;
            for (int i = N; i <data.Count; i++)
            {
                List<double> yieldList = new List<double>();
                for (int j = i-N+1; j < i; j++)
                {
                    var dataToday = data.ElementAt(j).Value;
                    for (int k = 1; k < dataToday.Count(); k++)
                    {
                        if (dataToday[k].Volume>0)
                        {
                            double yield =Math.Log(dataToday[k].Close / dataToday[k - 1].Close);
                            yieldList.Add(yield);
                        }
                    }
                }
                if (yieldList.Count > N / 3*240)
                {
                    double volToday = MathUtility.std(yieldList)*Math.Sqrt(252*240);
                    vol.Add(data.ElementAt(i).Value[0].DateTime.Date, volToday);
                }
            }
            return vol;
        }

    }
}
