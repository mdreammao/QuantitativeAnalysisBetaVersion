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
            //var res = tickRepo.GetStockTransaction("510300.sh", "2012-05-31 9:00:00".ToDateTime(), "2017-06-30 15:00:00".ToDateTime());

            //IDataSource ds = new DefaultStockOptionTickDataSource(DataAccess.Infrastructure.ConnectionType.Server217);
            //var tickRepo = new StockOptionTickRepository(DataAccess.Infrastructure.ConnectionType.Default, ds);

            // var res = tickRepo.GetStockTransaction("10000001.sh", "2015-02-09 9:00:00".ToDateTime(), "2015-03-25 15:00:00".ToDateTime());
            IDataSource ds = new DefaultStockMinuteDataSource();
            var minuteRepo = new StockMinuteRepository(DataAccess.Infrastructure.ConnectionType.Default, ds);
            var res = minuteRepo.GetStockTransaction("600000.SH", "2014-07-31 9:00:00".ToDateTime(), "2017-6-30 15:00:00".ToDateTime());
            Console.WriteLine("Total Fetch Count:" + res.Count);
            Console.WriteLine(DateTime.Now.TimeOfDay);
            //redisWriter.Clear(0);

        }
    }
}
