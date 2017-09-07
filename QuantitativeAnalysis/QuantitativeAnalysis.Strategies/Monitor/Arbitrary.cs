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
using NLog;
using Autofac;
using QuantitativeAnalysis.Transaction;


namespace QuantitativeAnalysis.Strategy
{
    public class Arbitrary
    {
        private DateTime startDate;
        private DateTime endDate;
        private double rate;
        static TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        static Logger logger = LogManager.GetCurrentClassLogger();
        private string underlying = "510050.SH";
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;

        public Arbitrary(DateTime start, DateTime end, double rate = 0.04)
        {
            startDate = start;
            endDate = end;
            this.rate = rate;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
        }

        public void record()
        {
            var tradedays=dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {
                compute(date);
            }
        }

        private void compute(DateTime date)
        {
            var repo = InstanceFactory.Get<OptionInfoRepository>(conn_type);
            var list = repo.GetStockOptionInfo(underlying, date, date);
            var source = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockDailyDataSource>());
            var etfRepo = InstanceFactory.Get<StockMinuteRepository>(conn_type, source);
            var etf = etfRepo.GetStockTransaction(underlying, date,date);
        }

    }
}

