using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class BollingerBand
    {
        public double mean { get; set; }
        public double std { get; set; }
    }

    public class BollingerBandwithPrice
    {
        public double mean { get; set; }
        public double std { get; set; }
        public double price { get; set; }
        public DateTime time { get; set; }
        public double up1 { get; set; }
        public double up2 { get; set; }
        public double low1 { get; set; }
        public double low2 { get; set; }
        public double area { get; set; }
    }

    public class signalWithTime
    {
        public double signal { get; set; }
        public DateTime time { get; set; }
    }

    public class BollingerBandWithVarietyScale
    {
        public DateTime date { get; set; }
        public double mean { get; set; }
        public double std { get; set; }
        public double orignalPrice { get; set; }
        public double modifiedPrice { get; set; }
        public double k { get; set; }
        public double lambda { get; set; }
        public double up { get; set; }
        public double low { get; set; }
    }


}
