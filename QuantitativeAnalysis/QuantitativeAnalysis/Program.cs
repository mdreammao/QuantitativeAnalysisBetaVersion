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
using QuantitativeAnalysis.Monitor;

namespace QuantitativeAnalysis
{
    class Program
    {
        static TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        static TypedParameter conn_type170 = new TypedParameter(typeof(ConnectionType), ConnectionType.Server170);
        static Logger logger = LogManager.GetCurrentClassLogger();


        static void Main(string[] args)
        {
            logger.Info("main method start...");
            Initializer.Initialize(ConnectionType.Default);
            //var monitor = new Arbitrary("2017-08-01 09:00:00".ToDateTime(), "2017-08-30 17:00:00".ToDateTime());
            //monitor.record();
            var optionSource = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockOptionTickDataSource>(new TypedParameter(typeof(ConnectionType), ConnectionType.Server170)));
            var optionRepo = InstanceFactory.Get<StockOptionTickRepository>(conn_type, optionSource);
            var stockSource = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockTickDataSource>(new TypedParameter(typeof(ConnectionType), ConnectionType.Server170)));
            var etfRepo = InstanceFactory.Get<StockTickRepository>(conn_type, stockSource);
            var infoRepo = InstanceFactory.Get<OptionInfoRepository>(conn_type);
            Arbitrary test = new Arbitrary(infoRepo, optionRepo, etfRepo);
            test.record("2016-01-01".ToDateTime(), "2017-08-31".ToDateTime());
            logger.Info("main method end...");
        }


        

        private static void StockMinuteTranSimulatorDemo()
        {
            var source = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockDailyDataSource>());
            var repo = InstanceFactory.Get<StockMinuteRepository>(conn_type,source);
            var repo_para = new TypedParameter(typeof(IStockRepository), repo);
            var simulator = InstanceFactory.Get<StockMinuteTransactionSimulator>(repo_para,new TypedParameter(typeof(double),0.1));
            var s = new Signal()
            {
                Code="000001.SZ",
                Price = 9.15,
                StartTradingTime = "2017-01-09 09:50:00".ToDateTime(),
                EndTradingTime = "2017-01-09 15:00:00".ToDateTime(),
                Volume = 30000
            };
            simulator.Trade(s);
        }

        static void StockDailyDemo()
        {
            var source = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockDailyDataSource>());
            var repo = InstanceFactory.Get<StockDailyRepository>(conn_type,source);

            var res = repo.GetStockTransaction("000001.SZ", "2017-01-01".ToDateTime(), "2017-08-01".ToDateTime());
        }

        static void StockMinuteDemo()
        {
            var source = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockDailyDataSource>());
            var repo = InstanceFactory.Get<StockMinuteRepository>(conn_type,source);

            var res = repo.GetStockTransaction("000001.SZ", "2017-01-01 13:00:00".ToDateTime(), "2017-08-01 15:00:00".ToDateTime());
        }

        static void StockTickDemo()
        {
            var source = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockTickDataSource>(new TypedParameter(typeof(ConnectionType), ConnectionType.Server217)));
            var repo = InstanceFactory.Get<StockTickRepository>(conn_type,source);

            var res =repo.GetStockTransaction("000001.SZ", "2017-01-01 14:10:40".ToDateTime(), "2017-02-11 14:50:00".ToDateTime());
        }

        static void TradingDateDemo()
        {
            var repo = InstanceFactory.Get<TransactionDateTimeRepository>(conn_type);

            var res = repo.GetStockTransactionDate("2017-01-01".ToDateTime(), "2017-12-31".ToDateTime());

            var current = "2017-02-01".ToDateTime();
            var next_transaction_date = repo.GetNextTransactionDate(current);
            var last_transaction_date = repo.GetPreviousTransactionDate(current);

            var first_transaction_day_of_current_month = repo.GetFirstTransactionDate(current, DateLevel.Month);
            var last_transaction_day_of_current_month = repo.GetLastTransactionDate(current, DateLevel.Month);

            var first_transaction_day_of_current_year = repo.GetFirstTransactionDate(current, DateLevel.Year);
            var last_transaction_day_of_current_year = repo.GetLastTransactionDate(current, DateLevel.Year);

        }

        static void UpdateStockInfoDemo()
        {
            var repo = InstanceFactory.Get<StockInfoRepository>(conn_type);

            repo.UpdateStockInfoToNow();
        }

        static void UpdateOptionInfoDemo()
        {
            var repo = InstanceFactory.Get<OptionInfoRepository>(conn_type);

            repo.UpdateOptionInfo("510050.SH");
        }
    }
}
