using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class StockTickTransaction
    {
        public string Code { get; set; }
        public DateTime TransactionDateTime { get; set; }
        public double LastPrice { get; set; }
        public double Ask1 { get; set; }
        public double AskV1 { get; set; }
        public double Ask2 { get; set; }
        public double AskV2 { get; set; }
        public double Ask3 { get; set; }
        public double AskV3 { get; set; }
        public double Ask4 { get; set; }
        public double AskV4 { get; set; }
        public double Ask5 { get; set; }
        public double AskV5 { get; set; }
        public double Bid1 { get; set; }
        public double BidV1 { get; set; }
        public double Bid2 { get; set; }
        public double BidV2 { get; set; }
        public double Bid3 { get; set; }
        public double BidV3 { get; set; }
        public double Bid4 { get; set; }
        public double BidV4 { get; set; }
        public double Bid5 { get; set; }
        public double BidV5 { get; set; }
        public double Volume { get; set; }
        public double Amount { get; set; }
    }
}
