using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class ETFConsitituent
    {
        public DateTime date { get; set; }
        public string code { get; set; }
        public string stockName { get; set; }
        public double volume { get; set; }
        public string cash_substitution_mark { get; set; }
        public double premium_ratio { get; set;}
        public double substitution_amout { get; set; }
    }
}
