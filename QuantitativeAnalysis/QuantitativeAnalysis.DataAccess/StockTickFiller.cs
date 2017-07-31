using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.Utilities;
namespace QuantitativeAnalysis.DataAccess
{
    public class StockTickFiller
    {
        public static List<StockTickTransaction> Fill(List<StockTickTransaction> source)
        {
            var dest = new List<StockTickTransaction>();
            var groups = source.GroupBy(c => c.TransactionDateTime.ToString("yyyy-MM-dd HH:mm:ss"));
            DateTime previous=default(DateTime);
            foreach( var g in groups)
            {
                if (NotInExpectedTimeSpan(g.Key.ToDateTime()))
                    break;
                int i = 1;
                foreach (var item in g)
                {
                    switch (i){
                        case 1:
                            item.TransactionDateTime = DateTime.Parse(item.TransactionDateTime.ToString("yyyy-MM-dd HH:mm:ss"));
                            FillBetween(dest, previous, item.TransactionDateTime);
                            dest.Add(item);
                            previous = item.TransactionDateTime;
                            break;
                        case 2:
                            item.TransactionDateTime = DateTime.Parse(item.TransactionDateTime.ToString("yyyy-MM-dd HH:mm:ss.500"));
                            FillBetween(dest, previous, item.TransactionDateTime);
                            dest.Add(item);
                            previous = item.TransactionDateTime;
                            break;
                    }
                    i++;
                }
            }
            return dest;
        }

        private static bool NotInExpectedTimeSpan(DateTime dateTime)
        {
            var time = dateTime.TimeOfDay;
            if (time >= new TimeSpan(0, 9, 30, 0, 0) && time <= new TimeSpan(0, 11, 30, 0, 0))
                return false;
            if (time >= new TimeSpan(0, 13, 0, 0, 0) && time <= new TimeSpan(0, 15, 0, 0, 0))
                return false;
            return true;
        }

        private static void FillBetween(List<StockTickTransaction> dest, DateTime previous, DateTime current)
        {
            if (previous != default(DateTime))
            {
                var previousTime = previous.TimeOfDay;
                var nextTime = GetNextTime(previousTime);
                var currentTime = current.TimeOfDay;
                if (previous.Date == current.Date)
                {
                    while (nextTime < currentTime && nextTime!=new TimeSpan(0,9,0,0,0))
                    {
                        dest.Add(new StockTickTransaction() { Code = dest.Last().Code, TransactionDateTime = new DateTime(previous.Year, previous.Month, previous.Day, nextTime.Hours, nextTime.Minutes, nextTime.Seconds, nextTime.Milliseconds) });
                        nextTime = GetNextTime(nextTime);

                    }
                }
                if (previous.Date < current.Date)
                {
                    while (nextTime <= new TimeSpan(0, 15, 0, 0, 0))
                    {
                        dest.Add(new StockTickTransaction() { Code = dest.Last().Code, TransactionDateTime = new DateTime(previous.Year, previous.Month, previous.Day, nextTime.Hours, nextTime.Minutes, nextTime.Seconds, nextTime.Milliseconds) });
                        var cn = nextTime;
                        nextTime = GetNextTime(nextTime);
                        if (nextTime == new TimeSpan(0, 9, 30, 0, 0) && cn.Hours > 13)
                            break;
                    }
                    while (nextTime < currentTime && nextTime != new TimeSpan(0, 9, 0, 0, 0))
                    {
                        dest.Add(new StockTickTransaction() { Code = dest.Last().Code, TransactionDateTime = new DateTime(current.Year, current.Month, current.Day, nextTime.Hours, nextTime.Minutes, nextTime.Seconds, nextTime.Milliseconds) });
                        nextTime = GetNextTime(nextTime);
                    }
                }
            }
        }

        private static TimeSpan GetNextTime(TimeSpan current)
        {
            var amStart = new TimeSpan(0, 9, 30, 0, 0);
            var amEnd = new TimeSpan(0, 11, 30, 0, 0);
            var pmStart = new TimeSpan(0, 13, 0, 0, 0);
            var pmEnd = new TimeSpan(0, 15, 0, 0, 0);
            if (current >= amEnd && current<pmStart)
                return pmStart;
            if (current >= pmEnd)
                return amStart;
            return current.AddMillisecond(500);

        }
    }
}
