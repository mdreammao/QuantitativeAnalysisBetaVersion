using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class ConvertibleBondInfo
    {
        public string code { get; set; }
        public string name { get; set; }
        public string stockCode { get; set; }
        public DateTime startDate { get; set; }
        public DateTime endDate { get; set; }
    }

    public class ConvertibleBondDailyInfo
    {
        public string code { get; set; }
        public string name { get; set; }
        public string stockCode { get; set; }
        public DateTime date { get; set; }
        public DateTime startDate { get; set; }
        public DateTime endDate { get; set; }
        public double conversionPrice { get; set;}
        public DateTime forceConvertDate { get; set; }
        public DateTime conversionStartDate { get; set; }
        public DateTime conversionEndDate { get; set; }
    }

    public class ConvertibleBondMonitor
    {
        public string code { get; set; }
        public string name { get; set; }
        public string stockCode { get; set; }
        public DateTime updateTime { get; set; }
        public double conversionPrice { get; set; }
        public double nonCeilingStockPrice { get; set; }
        public double nonCeilingBondPrice { get; set; }
        public double ceilingStockPrice { get; set; }
        public double estimateCeilingBondPrice { get; set; }
        public double estimateCeilingBondPrice2 { get; set; }
    }

}
