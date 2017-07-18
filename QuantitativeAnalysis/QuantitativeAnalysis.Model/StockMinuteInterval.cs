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
            this._current = start;
            this.transactionDates = transactionDates;
            if (!transactionDates.Contains(start.Date) || !transactionDates.Contains(end.Date))
                throw new Exception("开始日期和结束日期必须为交易日期！");
        }
        public DateTime Start { get;  }
        public DateTime End { get;  }
        private DateTime _current;
        private List<DateTime> transactionDates;

        public DateTime Current {
            get{
                var cu = _current;
                _current = GetNext(_current);
                return cu;
            }
        }

        public DateTime GetNext(DateTime dt)//这里的dt应该是交易日期,分别确认 交易日期和交易时间 2017-05-21 10:30:21
        {
            int hour = dt.Hour, minute = dt.Minute, second = dt.Second;

            var dtSwitch = new Dictionary<Func<DateTime, bool>, Action>
            {
                {x=> x>new DateTime(x.Year,x.Month,x.Day,15,0,0)&&x<new DateTime(x.Year,x.Month,x.Day,9,25,0),()=>{ } }
            };
            return DateTime.Now;
        }

        public DateTime GetPrevious(DateTime dt)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext()
        {
            return _current <= End;
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
