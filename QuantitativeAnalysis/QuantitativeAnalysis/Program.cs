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
            var server = RedisClientSingleton.Instance.GetServer(RedisClientSingleton.Instance.GetEndPoints()[0]);
            server.FlushDatabase(0);
            StockDailyRepository repo1 = new StockDailyRepository();
            var res6 = repo1.GetStockTransaction("000002.SZ", "2015-06-17".ToDateTime(), "2017-07-16".ToDateTime());
            return;
            var list1 = new List<int>();//request
            list1.Add(2);
            list1.Add(3);
            list1.Add(4);
            list1.Add(5);
            list1.Add(7);
            list1.Add(8);
            list1.Add(9);
            list1.Add(10);
            list1.Add(11);
            var list2 = new List<int>();//sql
            list2.Add(3);
            list2.Add(4);
            list2.Add(5);
            list2.Add(7);
            list2.Add(8);
            var pairs = new Dictionary<int,KeyValuePair<int,int>>(); //index begin end 
            var res = list1.Except(list2).ToList();//new begin end 比较Index
            var interval = Computor.GetNoExistedInterval<int>(list1, list2);
            foreach(var item in res)
            {
                var index = list1.IndexOf(item);
                if(!pairs.ContainsKey(index-1))
                    pairs.Add(index, new KeyValuePair<int, int>(item,item));
                else
                {
                    pairs.Add(index, new KeyValuePair<int, int>(pairs[index - 1].Key, item));
                    pairs.Remove(index-1);
                }
            }
            foreach(var item in pairs)
            {
                var star = item.Value.Key;
                var end = item.Value.Value;
            }
            return;

            RedisWriter writer = new RedisWriter();
            var code = "000001.SZ";
            var date = DateTime.Now.ToString("yyyy-MM-dd");
               var enties = new HashEntry[]
            {
                new HashEntry(StockTransaction.OpenName,12),
                new HashEntry(StockTransaction.HighName,13.3),
                new HashEntry(StockTransaction.LowName,11),
                new HashEntry(StockTransaction.CloseName,11.5),
                new HashEntry(StockTransaction.VolumeName,1300),
                new HashEntry(StockTransaction.AmountName,2600),
                new HashEntry(StockTransaction.AdjFactorName,2.4),
                new HashEntry(StockTransaction.TradeStatusName,"交易"),
                new HashEntry(StockTransaction.DateTimeName,date),
                new HashEntry(StockTransaction.CodeName,code)
            };
            writer.HSetBulk(code + "-" + date, enties);
            var repo = new StockDailyRepository();
            //var res11 = repo.GetStockDaily(code, DateTime.Now);
            //writer.HDelete("Person1", "Name");
            //RedisReader reader = new RedisReader();
            //var name = reader.HGet("Person1", "Name");
            //var connStr = "server=(local);uid=sa;pwd=maoheng0;";
            //var reader = new SqlServerReader(ConnectionType.Local);
            //var dt = reader.GetDataTable("SELECT TOP 1000 * FROM [optionTickData].[dbo].[MarketData_10000001_SH]");
            //var reader1 = new WindReader();
            //var times = reader1.GetTransactionDate("2017-06-11".ToDateTime(), "2017-07-14".ToDateTime());

            //return;
            //var dt = reader1.GetDailyDataTable("000002.SZ", "open,high,low,close,volume,amt,adjfactor,trade_status", "2017-07-10".ToDateTime(), "2013-07-09".ToDateTime(), "");
            //dt.TableName = "[DailyTransaction].[dbo].[Stock]";
            //var writer = new SqlServerWriter(ConnectionType.Local);
            //writer.InsertBulk(dt,dt.TableName);

        }
    }
}
