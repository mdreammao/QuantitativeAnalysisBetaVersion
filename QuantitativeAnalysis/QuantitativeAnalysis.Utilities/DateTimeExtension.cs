using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public static class DateTimeExtension
    {
        public static TimeSpan AddMillisecond(this TimeSpan ts,int millisecond)
        {
            return new TimeSpan(ts.Days, ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds + millisecond);
        }

        public static TimeSpan TimeWithoutMillisecond(this DateTime dt)
        {
            return new TimeSpan(0, dt.Hour, dt.Minute, dt.Second, 0);
        }
    }
}
