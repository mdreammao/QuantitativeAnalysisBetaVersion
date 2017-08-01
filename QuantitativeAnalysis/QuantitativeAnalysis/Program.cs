using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Utilities;
using StackExchange.Redis;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess;

namespace QuantitativeAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            var redisWriter = new RedisWriter();
            Console.WriteLine(DateTime.Now.TimeOfDay);
            //IDataSource ds = new DefaultStockTickDataSource(DataAccess.Infrastructure.ConnectionType.Server217);
            //var tickRepo = new StockTickRepository(DataAccess.Infrastructure.ConnectionType.Default, ds);
            //var res = tickRepo.GetStockTransaction("510050.sh", "2012-06-01 9:00:00".ToDateTime(), "2017-06-30 15:00:00".ToDateTime());

            //IDataSource ds = new DefaultStockOptionTickDataSource(DataAccess.Infrastructure.ConnectionType.Server217);
            //var tickRepo = new StockOptionTickRepository(DataAccess.Infrastructure.ConnectionType.Default, ds);

            // var res = tickRepo.GetStockTransaction("10000001.sh", "2015-02-09 9:00:00".ToDateTime(), "2015-03-25 15:00:00".ToDateTime());
            //IDataSource ds = new DefaultStockMinuteDataSource();
            //var minuteRepo = new StockMinuteRepository(DataAccess.Infrastructure.ConnectionType.Default, ds);
            //var res = minuteRepo.GetStockTransaction("600000.SH", "2017-01-01 9:00:00".ToDateTime(), "2017-7-30 15:00:00".ToDateTime());
            //IDataSource ds = new DefaultStockDailyDataSource();
            //var minuteRepo = new StockDailyRepository(DataAccess.Infrastructure.ConnectionType.Default, ds);
            //var res = minuteRepo.GetStockTransaction("510050.SH", "2017-01-01 9:00:00".ToDateTime(), "2017-7-30 15:00:00".ToDateTime());
            IDataSource ds = new DefaultStockOptionDailyDataSource();
            var minuteRepo = new StockOptionDailyRepository(DataAccess.Infrastructure.ConnectionType.Default, ds);
            var res = minuteRepo.GetStockOptionTransaction("10000001.SH", "2015-02-09 9:00:00".ToDateTime(), "2015-3-25 15:00:00".ToDateTime());

            Console.WriteLine("Total Fetch Count:" + res.Count);
            Console.WriteLine(DateTime.Now.TimeOfDay);
            //redisWriter.Clear(0);

        }
    }
}
