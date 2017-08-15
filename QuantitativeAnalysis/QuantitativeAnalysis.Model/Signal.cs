using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class Signal
    {
        public string Code { get; set; }
        public DateTime StartTradingTime { get; set; }
        public DateTime EndTradingTime { get; set; }
        public decimal Price { get; set; }
        public decimal Volume { get; set; }
        public TradingType Type { get; set; }

    }

    public enum TradingType
    {
        Ask,
        Bid
    }
}
