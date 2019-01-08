using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class OneByOneTransaction
    {
        public DateTime openTime { get; set; }
        public DateTime closeTime { get; set; }
        public double position { get; set; }
        public double openPrice { get; set; }
        public double closePrice { get; set; }
        public string closeStatus { get; set; }
    }

    public class netvalueDaily
    {
        public DateTime date { get; set; }
        public double netvalue { get; set; }
    }

}
