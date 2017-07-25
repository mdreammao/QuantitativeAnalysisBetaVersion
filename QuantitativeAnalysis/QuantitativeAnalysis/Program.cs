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
            IDataSource ds = new DefaultStockTickDataSource(DataAccess.Infrastructure.ConnectionType.Local);
            var tickRepo = new StockTickRepository(DataAccess.Infrastructure.ConnectionType.Default, ds);
            var res = tickRepo.GetStockTransaction("510180.sh", "2017-06-01 13:27:40".ToDateTime(), "2017-06-24 14:50:21.040".ToDateTime());
            Console.WriteLine("Total Fetch Count:" + res.Count);
            Console.WriteLine(DateTime.Now.TimeOfDay);

            //redisWriter.Clear(0);

        }
    }
}
