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
            for (int j = 0; j < original.Count; j++)
            {
                while (original[j].TransactionDateTime.TimeOfDay > timelist[timeIndex] && timeIndex<timelist.Count-1)
                {
                    timeIndex++;
                }
                if (original[j].TransactionDateTime.TimeOfDay <= timelist[timeIndex])
                {
                    modify[timeIndex] = original[j];
                }
                
            }
            for (int i = 1; i < timelist.Count; i++)
            {
                if (modify[i]==null)
                {
                    modify[i] = modify[i - 1];
                }
            }
            return modify.ToList();
        }

        public static List<StockTickTransaction> ModifyStockTickData(List<StockTickTransaction> original)
        {
            var timelist = GetStockTickStamp();
            StockTickTransaction[] modify = new StockTickTransaction[timelist.Count];
            int timeIndex = 0;
            for (int j = 0; j < original.Count; j++)
            {
                while (original[j].TransactionDateTime.TimeOfDay > timelist[timeIndex] && timeIndex < timelist.Count - 1)
                {
                    timeIndex++;
                }
                if (original[j].TransactionDateTime.TimeOfDay <= timelist[timeIndex])
                {
                    modify[timeIndex] = original[j];
                }

            }
            for (int i = 1; i < timelist.Count; i++)
            {
                if (modify[i] == null)
                {
                    modify[i] = modify[i - 1];
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
