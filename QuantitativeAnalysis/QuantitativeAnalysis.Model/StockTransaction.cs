using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class StockTransaction
    {
        public const string CodeName = "Code";
        public const string DateTimeName = "DateTime";
        public const string OpenName = "Open";
        public const string HighName = "High";
        public const string LowName = "Low";
        public const string CloseName = "Close";
        public const string VolumeName = "Volume";
        public const string AmountName = "Amount";
        public const string AdjFactorName = "AdjFactor";
        public const string TradeStatusName = "TradeStatus";

        public string Code { get; set; }
        public DateTime DateTime { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public double Volume { get; set; }
        public double Amount { get; set; }
        public double AdjFactor { get; set; }
        public string TradeStatus { get; set; }
        public StockTransactionLevel Level { get; set; }
    }

    public enum StockTransactionLevel
    {
        Daily,
        Minute
    }

}
