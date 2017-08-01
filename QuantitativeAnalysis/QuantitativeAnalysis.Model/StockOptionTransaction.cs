using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace QuantitativeAnalysis.Model
{
    public class StockOptionTransaction
    {
        public const string CodeName = "Code";
        public const string DateTimeName = "DateTime";
        public const string OpenName = "Open";
        public const string HighName = "High";
        public const string LowName = "Low";
        public const string CloseName = "Close";
        public const string VolumeName = "Volume";
        public const string AmountName = "Amount";
        public const string SettleName = "Settle";
        public const string OpenInterestName = "OpenInterest";

        public string Code { get; set; }
        public DateTime DateTime { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public double Volume { get; set; }
        [JsonProperty("Amt")]
        public double Amount { get; set; }
        public double Settle { get; set; }
        [JsonProperty("Oi")]
        public double OpenInterest { get; set; }
        public StockOptionTransactionLevel Level { get; set; }

        public static StockOptionTransaction Default(string code, DateTime date, StockOptionTransactionLevel level)
        {
            return new StockOptionTransaction() { Code = code, DateTime = date, Level = level };
        }
    }

    public enum StockOptionTransactionLevel
    {
        Daily,
        Minute
    }

}
