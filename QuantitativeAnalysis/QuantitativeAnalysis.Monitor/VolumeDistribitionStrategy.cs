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
    public class VolumeDistribitionStrategy
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
        
        public VolumeDistribitionStrategy(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, string code)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            this.code = code;
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void compute(DateTime startDate, DateTime endDate,int steps=40)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var etfall = stockMinutelyRepo.GetStockTransactionWithRedis(code, tradedays.First(), tradedays.Last());
            int duration = 5;
            int minutes = 240;
            //var distribution0 = getDistribution(etfall, 500, tradedays.Count()-1, 240);
            //ListToCSV.SaveDataToCSVFile<double>(distribution0, ListToCSV.CreateFile("E:\\result\\vd\\", "distribution"), "distribution");
            for (int i = steps+1; i < tradedays.Count(); i++)
            {
                List<double> bigVolume = new List<double>();
                int endDateNum = i-1;
                int startDateNum = Math.Max((i - steps-1), 0);
                var distribution = getDistribution(etfall, startDateNum, endDateNum, minutes);
                var fluctuation = getFluctuation(etfall, startDateNum, endDateNum, minutes, duration);
                var distributionToday = getDistributionToday(etfall, distribution, i, minutes);
                for (int j = 0; j < minutes-duration; j++)
                {
                    if (distributionToday[j]-distribution[j]>0.002)
                    {
                        bigVolume.Add((etfall[i * minutes + j + duration].Close / etfall[i * minutes + j].Close - 1));
                    }
                }
                var all = ListExtension.mean(fluctuation);
                var today = ListExtension.mean(bigVolume);
                Console.WriteLine("Date:{0},all:{1},  today:{2}!", etfall[i * minutes].DateTime.Date.ToString(), Math.Round(all, 5), Math.Round(today, 5));
            }  
        }

        List<double> getFluctuation(List<StockTransaction> stockData, int startDays, int endDays,int minutes,int duration)
        {
            List<double> fluctuation = new List<double>();
            for (int i = startDays; i <=endDays; i++)
            {
                for (int j = 0; j < minutes-duration; j++)
                {
                    fluctuation.Add((stockData[i * minutes + j + duration].Close / stockData[i * minutes + j].Close - 1));
                }
            }
            return fluctuation;
        }

        List<double> getDistributionToday(List<StockTransaction> stockData,List<double> distributionAll,int todayNum,int minutes)
        {
            double[] distribution = new double[minutes];
            double denominatorToday = 0;
            double denominatorAll = 0;
            for (int i  = 0; i < minutes; i++)
            {
                denominatorAll += distributionAll[i];
                denominatorToday += stockData[todayNum * minutes + i].Volume;
                distribution[i] = stockData[todayNum * minutes + i].Volume / denominatorToday * denominatorAll;
            }
            return distribution.ToList();
        }

        List<double> getDistribution(List<StockTransaction> stockData,int startDays,int endDays,int minutes)
        {
            double[] distribution = new double[minutes];
            double total = 0;
            for (int i = startDays; i <= endDays; i++)
            {
                for (int j = 0; j < minutes; j++)
                {
                    distribution[j] += stockData[i * minutes + j].Volume;
                    total+= stockData[i * minutes + j].Volume;
                }
            }
            for (int j = 0; j < minutes; j++)
            {
                distribution[j] /= total;
            }
            return distribution.ToList();
        }

    }
}
