using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class StockOptionParity
    {
        public string call { get; set; }
        public string put { get; set; }
        public double strike { get; set; }
        public DateTime expireDate { get; set; }
    }

    public class StockOptionParityProfit
    {
        public DateTime date { get; set; }
        public DateTime maturitydate { get; set; }
        public int expiredate  { get; set; }
        public double profit { get; set; }
        public double strike { get; set; }
        public double etfPrice { get; set; }
        public double cost { get; set; }
        public double callPrice { get; set; }
        public double putPrice { get; set; }

    }
}
