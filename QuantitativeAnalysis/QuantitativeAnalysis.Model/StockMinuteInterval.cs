using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class StockMinuteInterval : IDateTimeInterval
    {
        public StockMinuteInterval(DateTime start,DateTime end,List<DateTime> transactionDates)
        {
            this.Start = start;
            this.End = end;
            this.transactionDates = transactionDates;
            this._current = start.AddMinutes(-1);
        }
        public DateTime Start { get;  }
        public DateTime End { get;  }
        private DateTime _current;
        private List<DateTime> transactionDates;

        public DateTime Current {
            get{
                return _current;
            }
        }

        public DateTime GetNext(DateTime dt)
        {
            var nextTime = dt.AddMinutes(1);
            if (transactionDates.Contains(nextTime.Date))
            {
                var time = new TimeSpan(nextTime.Hour, nextTime.Minute, 0);
                if (time <= new TimeSpan(9, 30, 0))
                    return new DateTime(nextTime.Year, nextTime.Month, nextTime.Day, 9, 30, 0);
                if (time > new TimeSpan(9, 30, 0) && time <= new TimeSpan(11, 29, 0))
                    return new DateTime(nextTime.Year, nextTime.Month, nextTime.Day, nextTime.Hour, nextTime.Minute, 0);
                if (time > new TimeSpan(11, 29, 0) && time <= new TimeSpan(13, 0, 0))
                    return new DateTime(nextTime.Year, nextTime.Month, nextTime.Day, 13, 0, 0);
                if (time > new TimeSpan(13, 0, 0) && time <= new TimeSpan(14, 59, 0))
                    return new DateTime(nextTime.Year, nextTime.Month, nextTime.Day, nextTime.Hour, nextTime.Minute, 0);
                if(time >new TimeSpan(14,59,0)&& nextTime.Date < transactionDates.Last())
                {
                    var nextDate = transactionDates[transactionDates.IndexOf(nextTime.Date) + 1];
                    return new DateTime(nextDate.Year, nextDate.Month, nextDate.Day, 9, 30, 0);
                }
            }
            else
            {
                var res = transactionDates.FirstOrDefault(c => c > nextTime.Date);
                if (res != default(DateTime))
                    return new DateTime(res.Year, res.Month, res.Day, 9, 30, 0);
            }
            return End.AddDays(1);
        }

        public DateTime GetPrevious(DateTime dt)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext()
        {
            _current = GetNext(_current);
            return _current.Date <= End.Date;
        }

        #region unimportant method
        public void Reset()
        {
            _current = Start;
        }

        object IEnumerator.Current
        {
            get { return this.Current; }
        }

        public void Dispose()
        {

        }
        #endregion
    }
}
