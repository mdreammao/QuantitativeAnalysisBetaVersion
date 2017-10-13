using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;

namespace QuantitativeAnalysis.Utilities
{
    public class DataTimeStampExtension
    {
        public static List<TimeSpan> stockTickStamp=new List<TimeSpan>();

        public static List<TimeSpan> GetStockTickStamp()
        {
            List<TimeSpan> stamp = stockTickStamp;
            if (stockTickStamp==null || stockTickStamp.Count==0)
            {
                stamp.AddRange(getStamp(new TimeSpan(9, 30, 0), new TimeSpan(11, 30, 0), new TimeSpan(0, 0, 0, 0, 500)));
                stamp.AddRange(getStamp(new TimeSpan(13, 00, 0), new TimeSpan(15, 00, 0), new TimeSpan(0, 0, 0, 0, 500)));
                stockTickStamp = stamp;
            }
            return stamp;
        }

        public static List<StockOptionTickTransaction> ModifyOptionTickData(List<StockOptionTickTransaction> original)
        {
            var timelist = GetStockTickStamp();
            StockOptionTickTransaction[] modify = new StockOptionTickTransaction[timelist.Count];
            int timeIndex = 0;
            DateTime today = new DateTime(original[0].TransactionDateTime.Year, original[0].TransactionDateTime.Month, original[0].TransactionDateTime.Day);
            for (int j = 0; j < original.Count; j++)
            {
                while (original[j].TransactionDateTime.TimeOfDay >= timelist[timeIndex] && timeIndex<timelist.Count-1)
                {
                    timeIndex++;
                }
                if (original[j].TransactionDateTime.TimeOfDay < timelist[timeIndex])
                {
                    var data0 = original[j];
                    modify[timeIndex-1] = new StockOptionTickTransaction { Ask1 = data0.Ask1, Ask2 = data0.Ask2, Ask3 = data0.Ask3, Ask4 = data0.Ask4, Ask5 = data0.Ask5, AskV1 = data0.AskV1, AskV2 = data0.AskV2, AskV3 = data0.AskV3, AskV4 = data0.AskV4, AskV5 = data0.AskV5, Bid1 = data0.Bid1, Bid2 = data0.Bid2, Bid3 = data0.Bid3, Bid4 = data0.Bid4, Bid5 = data0.Bid5, BidV1 = data0.BidV1, BidV2 = data0.BidV2, BidV3 = data0.BidV3, BidV4 = data0.BidV4, BidV5 = data0.BidV5, Amount = data0.Amount, Code = data0.Code, LastPrice = data0.LastPrice, TransactionDateTime = data0.TransactionDateTime, Volume = data0.Volume,OpenInterest=data0.OpenInterest };
                }
                
            }
            for (int i = 1; i < timelist.Count; i++)
            {
                if (modify[i] == null && modify[i - 1] != null)
                {
                    var data0 = modify[i - 1];
                    modify[i] = new StockOptionTickTransaction { Ask1 = data0.Ask1, Ask2 = data0.Ask2, Ask3 = data0.Ask3, Ask4 = data0.Ask4, Ask5 = data0.Ask5, AskV1 = data0.AskV1, AskV2 = data0.AskV2, AskV3 = data0.AskV3, AskV4 = data0.AskV4, AskV5 = data0.AskV5, Bid1 = data0.Bid1, Bid2 = data0.Bid2, Bid3 = data0.Bid3, Bid4 = data0.Bid4, Bid5 = data0.Bid5, BidV1 = data0.BidV1, BidV2 = data0.BidV2, BidV3 = data0.BidV3, BidV4 = data0.BidV4, BidV5 = data0.BidV5, Amount = data0.Amount, Code = data0.Code, LastPrice = data0.LastPrice, TransactionDateTime = data0.TransactionDateTime, Volume = data0.Volume,OpenInterest=data0.OpenInterest };
                    modify[i].TransactionDateTime = today + timelist[i];
                }
            }
            return modify.ToList();
        }

        public static List<StockTickTransaction> ModifyStockTickData(List<StockTickTransaction> original)
        {
            var timelist = GetStockTickStamp();
            StockTickTransaction[] modify = new StockTickTransaction[timelist.Count];
            int timeIndex = 0;
            DateTime today =new DateTime(original[0].TransactionDateTime.Year,original[0].TransactionDateTime.Month,original[0].TransactionDateTime.Day);
            for (int j = 0; j < original.Count; j++)
            {
                while (original[j].TransactionDateTime.TimeOfDay >= timelist[timeIndex] && timeIndex < timelist.Count - 1)
                {
                    timeIndex++;
                }
                if (original[j].TransactionDateTime.TimeOfDay < timelist[timeIndex])
                {
                    var data0 = original[j];
                    modify[timeIndex-1] =new StockTickTransaction { Ask1=data0.Ask1,Ask2=data0.Ask2,Ask3=data0.Ask3,Ask4=data0.Ask4,Ask5=data0.Ask5,AskV1=data0.AskV1,AskV2=data0.AskV2,AskV3=data0.AskV3,AskV4=data0.AskV4,AskV5=data0.AskV5,Bid1=data0.Bid1,Bid2=data0.Bid2,Bid3=data0.Bid3,Bid4=data0.Bid4,Bid5=data0.Bid5,BidV1=data0.BidV1,BidV2=data0.BidV2,BidV3=data0.BidV3,BidV4=data0.BidV4,BidV5=data0.BidV5,Amount=data0.Amount,Code=data0.Code,LastPrice=data0.LastPrice,TransactionDateTime=data0.TransactionDateTime,Volume=data0.Volume};
                }

            }
            for (int i = 1; i < timelist.Count; i++)
            {
                if (modify[i] == null && modify[i-1]!=null)
                {
                    var data0 = modify[i - 1];
                    modify[i] = new StockTickTransaction { Ask1 = data0.Ask1, Ask2 = data0.Ask2, Ask3 = data0.Ask3, Ask4 = data0.Ask4, Ask5 = data0.Ask5, AskV1 = data0.AskV1, AskV2 = data0.AskV2, AskV3 = data0.AskV3, AskV4 = data0.AskV4, AskV5 = data0.AskV5, Bid1 = data0.Bid1, Bid2 = data0.Bid2, Bid3 = data0.Bid3, Bid4 = data0.Bid4, Bid5 = data0.Bid5, BidV1 = data0.BidV1, BidV2 = data0.BidV2, BidV3 = data0.BidV3, BidV4 = data0.BidV4, BidV5 = data0.BidV5, Amount = data0.Amount, Code = data0.Code, LastPrice = data0.LastPrice, TransactionDateTime = data0.TransactionDateTime, Volume = data0.Volume };
                    modify[i].TransactionDateTime =today+ timelist[i];
                }
            }
            return modify.ToList();
        }

        private static List<TimeSpan> getStamp(TimeSpan start,TimeSpan end,TimeSpan span)
        {
            List<TimeSpan> list = new List<TimeSpan>();
            if (end<=start)
            {
                return list;
            }
            while (start<end)
            {
                var now = new TimeSpan();
                now = start;
                list.Add(now);
                start = start.Add(span);
            }
            list.Add(end);
            return list;
        }
    }
}
