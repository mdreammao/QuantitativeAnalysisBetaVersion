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

namespace QuantitativeAnalysis.Statistics
{
    public class ImbalanceInfactor
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockTickRepository stockRepo;
        private StockDailyRepository stockDailyRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;

        public ImbalanceInfactor(StockTickRepository stockRepo, StockDailyRepository stockDailyRepo, string code)
        {
            this.stockRepo = stockRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
            this.code = code;
        }

        public void computeImbalanceInfactor(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {
                double[] imbalanceInfactor = new double[28802];
                double[] priceChange = new double[28802];
                var stockData = stockRepo.GetStockTransaction(code, date, date.AddHours(17));
                var stock = DataTimeStampExtension.ModifyStockTickData(stockData);
                for (int i = 0; i < 28802; i++)
                {
                    if (stock[i]!=null)
                    {
                        imbalanceInfactor[i] = stock[i].BidV1 / (stock[i].AskV1 + stock[i].BidV1);
                    }
                    if (i<28801 && stock[i] != null && stock[i+1]!=null)
                    {
                        priceChange[i] = Math.Round(stock[i + 1].Bid1 - stock[i].Bid1,4);
                    }
                }
                double total = 0;
                double right = 0;
                for (int i = 0; i < 28802; i++)
                {
                    //if (priceChange[i]!=0 && Math.Abs(imbalanceInfactor[i]-0.5)>=0)
                    {
                        total += 1;
                        if (priceChange[i]>0 && imbalanceInfactor[i]>0.75)
                        {
                            right += 1;
                        }
                        if (priceChange[i] < 0 && imbalanceInfactor[i] < 0.25)
                        {
                            right += 1;
                        }
                    }
                }
                Console.WriteLine(Math.Round(right / total, 3));
            }

        }

    }
}
