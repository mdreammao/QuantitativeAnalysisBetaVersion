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


namespace QuantitativeAnalysis.Monitor
{
    public class Arbitrary
    {
        private double rate;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
      
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string underlying = "510050.SH";
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private List<StockOptionParity> parityList;
        private OptionInfoRepository orRepo;
        private StockOptionTickRepository optionRepo;
        private StockTickRepository stockRepo;

        public Arbitrary(OptionInfoRepository orRepo, StockOptionTickRepository optionRepo,StockTickRepository stockRepo,double rate = 0.04)
        {
            this.orRepo = orRepo;
            this.optionRepo = optionRepo;
            this.stockRepo = stockRepo;
            this.rate = rate;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
        }

        public void record(DateTime startDate, DateTime endDate)
        {

            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {
                var list = orRepo.GetStockOptionInfo(underlying, date, date);
                parityList = new List<StockOptionParity>();
                foreach (var item in list)
                {
                    if (item.type == "认购" && item.expireDate <= date.AddDays(40) && item.listedDate <= date)
                    {
                        var parity = StockOptionExtension.GetParity(list, item);
                        Console.WriteLine("Date {0}: strike {1} expireDate {2} option1 {3} option2 {4}", date, item.strike, item.expireDate, item.name, parity.name);
                        var pair = new StockOptionParity { call = item.code, put = parity.code, strike = item.strike, expireDate = item.expireDate };
                        parityList.Add(pair);
                    }
                }
                compute(date);
            }
        }

        private void compute(DateTime date)
        {
            Dictionary<string, List<StockOptionTickTransaction>> data = new Dictionary<string, List<StockOptionTickTransaction>>();
            foreach (var item in parityList)
            {
                //var optionSource = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockOptionTickDataSource>(new TypedParameter(typeof(ConnectionType), ConnectionType.Server170)));
                //var optionRepo = InstanceFactory.Get<StockOptionTickRepository>(conn_type, optionSource);
                //var original = optionRepo.GetStockTransaction(item.call, date, date.AddHours(17));
                //var modify = DataTimeStampExtension.ModifyOptionTickData(original);

                data.Add(item.call, DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.call, date, date.AddHours(17))));
                data.Add(item.put, DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.put, date, date.AddHours(17))));
            }

            //var repo = InstanceFactory.Get<OptionInfoRepository>(conn_type);
            //var list = repo.GetStockOptionInfo(underlying, date, date);
            //var source = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockMinuteDataSource>());
            //var etfRepo = InstanceFactory.Get<StockMinuteRepository>(conn_type, source);
            //var etf = etfRepo.GetStockTransaction(underlying, date,date);
            //var source2 = new TypedParameter(typeof(IDataSource), InstanceFactory.Get<DefaultStockTickDataSource>(new TypedParameter(typeof(ConnectionType), ConnectionType.Server170)));
            //var etfRepo2 = InstanceFactory.Get<StockTickRepository>(conn_type, source2);
            //var etf2 = DataTimeStampExtension.ModifyStockTickData(etfRepo2.GetStockTransaction(underlying, date, date.AddHours(17)));

        }

    }
}

