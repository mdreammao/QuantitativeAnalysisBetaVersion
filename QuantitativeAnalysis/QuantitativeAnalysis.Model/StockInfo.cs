using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{

    public class StockCode
    {
        public string code { get; set; }
        public string name { get; set; }
    }

    public class StockIPOInfo
    {
        public string code { get; set; }
        public string name { get; set; }
        public DateTime IPODate { get; set; }
        public DateTime DelistDate { get; set; }
        public DateTime updateTime { get; set; }
    }

    public class StockBonusInfo
    {
        public DateTime exDividendDate { get; set; }
        public string code { get; set; }
        public string secName { get; set; }
        public double cashPayoutRatio { get; set; }
        public double stockSplitRatio { get; set; }
        public double stockDividendRatio { get; set; }
        public double seoRatio { get; set; }
        public double seoPrice { get; set; }
        public double rightsIssuePrice { get; set; }
        public double rightsIssueRatio { get; set; }
        public string exDividendNote { get; set; }
        public DateTime updateTime { get; set; }
    }


    public class indexStockInfo
    {
        public DateTime date { get; set; }
        public string code { get; set; }
        public string secName { get; set; }
        public double close { get; set; }
        public double weight { get; set; }
    }


    public class StockBonusPlan
    {
        public DateTime reportDate { get; set; }
        public string code { get; set; }
        public string name { get; set; }
        public string status { get; set; }
        public double dividend { get; set; }
        public DateTime planDate { get; set; }
    }

    public class StockBonusEstimate
    {
        public string code { get; set; }
        public string secName { get; set; }
        public DateTime shareRegisterDate { get; set; }
        public DateTime dividendDate { get; set; }
        public double dividend { get; set; }
        public double points { get; set; }
        public string status { get; set; }
    }
    

}
