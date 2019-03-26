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
using QuantitativeAnalysis.Monitor.EstimateStockBonus;
using QuantitativeAnalysis.Monitor.TickDataToMinuteData;
using NLog;
using Autofac;
using QuantitativeAnalysis.Transaction;
using QuantitativeAnalysis.Monitor;
using QuantitativeAnalysis.Statistics;
using static QuantitativeAnalysis.Utilities.DateTimeExtension;
using QuantitativeAnalysis.Monitor.IndexRelated;
using QuantitativeAnalysis.Monitor.DualTrust;
using QuantitativeAnalysis.Monitor.StockIntraday.Volatility;
using QuantitativeAnalysis.Monitor.StockIntraday.MovingAverage;
using QuantitativeAnalysis.Monitor.StockIntraday.ExtremeCase;

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
            
            //获取tick数据
            var optionSource = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockOptionTickDataSource>(new TypedParameter(typeof(ConnectionType), ConnectionType.Server170)));
            var optionRepo = InstanceFactory.Get<StockOptionTickRepository>(conn_type, optionSource);
            var stockSource = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockTickDataSource>(new TypedParameter(typeof(ConnectionType), ConnectionType.Server170)));
            var stockTickRepo = InstanceFactory.Get<StockTickRepository>(conn_type, stockSource);
            //获取日线数据
            var stockDailysource = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockDailyDataSource>());
            var stockDailyRepo = InstanceFactory.Get<StockDailyRepository>(conn_type, stockDailysource);
            var optionDailySource=new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockOptionDailyDataSource>());
            var optionDailyRepo= InstanceFactory.Get<StockOptionDailyRepository>(conn_type, optionDailySource);
            //获取股票基本信息数据
            var infoRepo = InstanceFactory.Get<OptionInfoRepository>(conn_type);
            var stockInfoRepo = InstanceFactory.Get<StockInfoRepository>(conn_type);
            var stockInfoDailysource = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockInfoDailyDataSource>());
            var stockInfoDailyRepo = InstanceFactory.Get<StockInfoDailyRepository>(conn_type, stockInfoDailysource);

            //获取分钟线数据(来源万德)
            var stockMinutelySource= new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockMinuteDataSource>());
            var stockMinutelyRepo= InstanceFactory.Get<StockMinuteRepository>(conn_type, stockMinutelySource);
            //获取分钟数据(来源tick数据)
            var stockMinutelyRepo2= InstanceFactory.Get<StockMinuteFromTickRepository>(conn_type,stockSource);

            //获取日期数据
            TransactionDateTimeRepository dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            DateUtils.setTradeDays(dateRepo.GetStockTransactionDate("2007-01-01".ToDateTime(), "2019-12-31".ToDateTime()));







            DateTime lastDay =DateUtils.LatestTradeDay(DateTime.Now.AddDays(-1));


            //priceCeilingMoving moving = new priceCeilingMoving(stockMinutelyRepo, stockDailyRepo, stockTickRepo, stockInfoRepo);
            //moving.backtest("000166.SZ", "2015-01-26".ToDateTime(), "2019-03-10".ToDateTime());
            //moving.backtestByIndexCode("000300.SH", "2013-01-01".ToDateTime(), "2019-03-10".ToDateTime());
            //Monitor.Bond.ConvertibleBond.Intraday1 bond = new Monitor.Bond.ConvertibleBond.Intraday1(stockMinutelyRepo, stockDailyRepo, stockTickRepo, dateRepo);
            //bond.backtest("2010-01-01".ToDateTime(), "2019-03-04".ToDateTime());


            //该区域为跑参数或者数据
            #region
            //Monitor.Bond.ConvertibleBond.IntradayMonitor bond = new Monitor.Bond.ConvertibleBond.IntradayMonitor(stockMinutelyRepo, stockDailyRepo, stockTickRepo, dateRepo);
            StockIndexBonus myBonus = new StockIndexBonus(stockInfoRepo, stockDailyRepo, dateRepo, lastDay, "000016.SH");
            myBonus = new StockIndexBonus(stockInfoRepo, stockDailyRepo, dateRepo, lastDay, "000905.SH");
            myBonus = new StockIndexBonus(stockInfoRepo, stockDailyRepo, dateRepo, lastDay, "000300.SH");
            //IndexAnalysis indexAnalysis = new IndexAnalysis(dateRepo, "2019-02-11".ToDateTime());
            //indexAnalysis.differ("510180.OF", "000300.SH");
            //indexAnalysis.differ("159901.OF", "000300.SH");
            //StockTickToMinute myStore = new StockTickToMinute(dateRepo, stockDailyRepo, stockMinutelyRepo2, stockTickRepo, stockInfoRepo);
            //myStore.getStockMinuteFromSqlByIndex("000300.SH", "2010-01-01".ToDateTime(), "2019-03-10".ToDateTime());
            //stockInfoRepo.UpdateStockInfoToNow();
            #endregion





            //OptionMonitor50ETF2019 optionMonitor = new OptionMonitor50ETF2019(infoRepo, dateRepo, stockDailyRepo, stockMinutelyRepo,optionDailyRepo,"2015-02-09".ToDateTime(), "2019-01-14".ToDateTime());

            //trendT0 myt0 = new trendT0(stockMinutelyRepo, stockDailyRepo, "000016.SH", "2016-02-01".ToDateTime(), "2019-01-14".ToDateTime());


            //DualTrust dt0 = new DualTrust(stockMinutelyRepo, stockDailyRepo, "000300.SH", "IF.CFE");
            //dt0.compute("2018-02-01".ToDateTime(), "2019-01-30".ToDateTime());

            //DualTrust2 dt2 = new DualTrust2(stockMinutelyRepo, stockDailyRepo);
            //dt2.backtest("IF.CFE", "000300.SH", "2018-02-01".ToDateTime(), "2019-02-14".ToDateTime());

            //DualTrust3 dt3 = new DualTrust3(stockMinutelyRepo, stockDailyRepo);
            //dt3.backtest("IF.CFE", "000300.SH", "2018-02-01".ToDateTime(), "2019-02-14".ToDateTime());




            //Monitor.StockIntraday.DualTrust.DualTrust2 stockDt = new Monitor.StockIntraday.DualTrust.DualTrust2(stockMinutelyRepo, stockDailyRepo, stockInfoRepo);
            //stockDt.backtest("600519.SH", "2016-03-07".ToDateTime(), "2019-03-04".ToDateTime());
            //stockDt.backtestByIndexCode("000016.SH", "2016-03-07".ToDateTime(), "2019-03-04".ToDateTime());

            //Monitor.StockIntraday.Volatility.StockWithVolatility2 stockVol = new Monitor.StockIntraday.Volatility.StockWithVolatility2(stockMinutelyRepo, stockDailyRepo,dateRepo);
            //stockVol.backtest("510500.SH", "2016-03-01".ToDateTime(), "2019-02-19".ToDateTime());

            //StockWithVolatility1 stockVol = new  StockWithVolatility1(stockMinutelyRepo, stockDailyRepo);
            //stockVol.backtest("IF.CFE", "000300.SH", "2018-02-01".ToDateTime(), "2019-02-19".ToDateTime());

            //StockDataStore myStore = new StockDataStore(stockMinutelyRepo, stockDailyRepo, dateRepo, stockInfoRepo);
            //myStore.getStockData("000300.SH", "2016-03-01".ToDateTime(), "2019-02-19".ToDateTime());

            //MA1 ma1 = new MA1(stockMinutelyRepo, stockDailyRepo);
            //ma1.backtest("510050.SH", "2016-03-01".ToDateTime(), "2019-02-19".ToDateTime());


            //pairtradingDaily2 mypair = new pairtradingDaily2(stockDailyRepo,"600030.SH", "601688.SH");
            //mypair = new pairtradingDaily2(stockDailyRepo, "000333.SZ", "000651.SZ");
            //mypair = new pairtradingDaily2(stockDailyRepo, "601398.SH", "601939.SH");
            //mypair = new pairtradingDaily2(stockDailyRepo, "601318.SH", "601601.SH");
            //mypair.compute("2010-01-01".ToDateTime(), "2018-12-28".ToDateTime());
            //RBreakStrategy mybreak = new RBreakStrategy(stockMinutelyRepo, stockDailyRepo, "IC.CFE");
            //mybreak.compute("2016-01-01".ToDateTime(), "2018-11-20".ToDateTime());
            //DiagonalSpread backtest = new DiagonalSpread(stockMinutelyRepo, stockDailyRepo, "510050.SH");
            //backtest.compute("2016-01-01".ToDateTime(), "2018-09-25".ToDateTime());
            //VolumeDistribitionStrategy vd = new VolumeDistribitionStrategy(stockMinutelyRepo, stockDailyRepo, "510050.SH");
            //vd.compute("2016-01-01".ToDateTime(), "2018-11-20".ToDateTime());
            //TDstrategy td = new TDstrategy(stockMinutelyRepo, stockDailyRepo, "RB.SHF");
            //td.compute("2016-01-01".ToDateTime(), "2018-11-27".ToDateTime());
            //CallDeltaHedge hedgeDemo = new CallDeltaHedge(stockTickRepo, stockDailyRepo, "510050.SH", 60);
            //hedgeDemo.compute("2018-01-10".ToDateTime(), "2018-08-10".ToDateTime());
            //var twap = new TWAP(stockTickRepo,dateRepo, stockMinutelyRepo,"603939.SH");
            //twap.computeTWAP("2018-01-01".ToDateTime(), "2018-06-06".ToDateTime());
            //var twap = new STWAP(stockTickRepo, dateRepo, "000544.SZ");
            //twap.computeSTWAP("2018-06-20".ToDateTime(), "2018-06-20".ToDateTime());
            //twap = new STWAP(stockTickRepo, dateRepo, "300274.SZ");
            //twap.computeSTWAP("2015-01-01".ToDateTime(), "2018-06-06".ToDateTime());
            //twap = new STWAP(stockTickRepo, dateRepo, "000738.SZ");
            //twap.computeSTWAP("2015-01-01".ToDateTime(), "2018-06-06".ToDateTime());
            //twap = new STWAP(stockTickRepo, dateRepo, "300230.SZ");
            //twap.computeSTWAP("2015-01-01".ToDateTime(), "2018-06-06".ToDateTime());

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
