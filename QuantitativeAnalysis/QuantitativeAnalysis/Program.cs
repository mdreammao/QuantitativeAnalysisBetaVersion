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
            var start = "2017-01-01 9:00:00".ToDateTime(); var redisWriter = new RedisWriter();
            //redisWriter.Clear(0);
            var end = "2017-07-21 14:11:56".ToDateTime();
            Console.WriteLine(DateTime.Now.TimeOfDay);
            IDataSource ds = new DefaultStockMinuteDataSource();
            var minuteRepo = new StockMinuteRepository(DataAccess.Infrastructure.ConnectionType.Default,ds);
            var ress11 = minuteRepo.GetStockTransaction("000001.SZ", start, end);
            Console.WriteLine(ress11.Count);
            Console.WriteLine(DateTime.Now.TimeOfDay);

            //return;
            //Console.WriteLine(DateTime.Now.TimeOfDay);
            //Console.WriteLine(DateTime.Now.TimeOfDay);
            //var redisWriter = new RedisWriter();
            ////redisWriter.Clear(0);
            //var repo = new StockDailyRepository();
            //var res = repo.GetStockTransaction("000002.SZ","2014-01-12".ToDateTime(), "2014-02-21".ToDateTime());
            //Console.WriteLine(res.Count);
            //Console.WriteLine(DateTime.Now.TimeOfDay);
        }
    }
}
