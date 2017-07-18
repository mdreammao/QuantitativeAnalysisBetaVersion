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
            Console.WriteLine(DateTime.Now.TimeOfDay);
            var redisWriter = new RedisWriter();
            //redisWriter.Clear(0);
            var repo = new StockDailyRepository();
            var res = repo.GetStockTransaction("000002.SZ","2014-04-12".ToDateTime(), "2017-02-21".ToDateTime());
            Console.WriteLine(res.Count);
            Console.WriteLine(DateTime.Now.TimeOfDay);
        }
    }
}
