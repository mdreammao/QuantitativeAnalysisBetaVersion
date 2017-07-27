using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class Interval
    {
        private readonly TimeSpan begin, end, span;
        public Interval(TimeSpan begin, TimeSpan end,TimeSpan span)
        {
            this.begin = begin;
            this.end = end;
            this.span = span;
        }
        public TimeSpan Begin { get { return begin; } }
        public TimeSpan End { get { return end; } }
        public TimeSpan Span { get { return span; } }
    }
}
