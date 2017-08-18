using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class TransactionResult
    {
        public Signal Signal { get; set; }
        public double TradedVolume { get; set; }
        public double TradedAmount { get; set; }
    }
}
