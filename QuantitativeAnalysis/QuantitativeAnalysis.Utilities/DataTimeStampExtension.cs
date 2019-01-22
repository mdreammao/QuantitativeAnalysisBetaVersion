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

        public static TimeSpan GetStockMinuteTimeByIndex(int index)
        {
            TimeSpan now = new TimeSpan();
            TimeSpan open1 = new TimeSpan(9, 30, 00);
            TimeSpan close1 = new TimeSpan(11, 30, 00);
            TimeSpan open2 = new TimeSpan(13, 00, 00);
            TimeSpan close2 = new TimeSpan(15, 00, 00);
            if (index >= 0 && index <= 119)
            {
                TimeSpan addtional = new TimeSpan(0, index, 0);
                now = open1.Add(addtional);
            }
            else if (index >= 120 && index <= 239)
            {
                TimeSpan addtional = new TimeSpan(0, index-120, 0);
                now = open2.Add(addtional);
            }
            return now;
        }


        public static TimeSpan GetStockSecondsTimeByIndex(int index)
        {
            TimeSpan now = new TimeSpan();
            TimeSpan open1 = new TimeSpan(9, 30, 00);
            TimeSpan close1 = new TimeSpan(11, 30, 00);
            TimeSpan open2 = new TimeSpan(13, 00, 00);
            TimeSpan close2 = new TimeSpan(15, 00, 00);

            if (index>=0 && index<=7200)
            {
                TimeSpan addtional = new TimeSpan(0, 0, index);
                now = open1.Add(addtional);
            }
            else if (index>=7201 && index<=14401)
            {
                TimeSpan addtional = new TimeSpan(0, 0, index-7201);
                now = open2.Add(addtional);
            }
            return now;
        }


        public static int GetStockSecondsIndex(DateTime time)
        {
            int index = -1;
            TimeSpan now = time.TimeOfDay;
            TimeSpan open1 = new TimeSpan(9, 30, 00);
            TimeSpan close1= new TimeSpan(11, 30, 00);
            TimeSpan open2 = new TimeSpan(13, 00, 00);
            TimeSpan close2 = new TimeSpan(15, 00, 00);
            if (now<=close1)
            {
                index =Convert.ToInt32((now - open1).TotalSeconds);
            }
            else if (now<open2)
            {
                index = Convert.ToInt32((close1 - open1).TotalSeconds);
            }
            else if (now<=close2)
            {
                index = Convert.ToInt32((close1 - open1).TotalSeconds + (now - open2).TotalSeconds)+1;
            }
            else
            {
                index= Convert.ToInt32((close1 - open1).TotalSeconds + (close2 - open2).TotalSeconds)+1;
            }
            return index;
        }

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
            if (original==null || original.Count()==0)
            {
                return null;
            }
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
                if (modify[i]!=null)
                {
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
                if (modify[i] != null)
                {
                    modify[i].TransactionDateTime = today + timelist[i];
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
