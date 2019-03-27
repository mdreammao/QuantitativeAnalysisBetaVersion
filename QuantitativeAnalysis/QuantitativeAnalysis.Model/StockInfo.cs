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

    //记录股票基本信息的类：行业、市值、PE、PB、资金流等
    public class StockBasicInfo
    {
        public string code { get; set; }
        public string name { get; set; }
        public string industryCode_sw { get; set; }
        public string industryName_sw { get; set; }
        public string IPODate { get; set; } //上市日期
        public double longAmount { get; set; } //融资余额
        public double shortAmount { get; set; } //融券余额
        public double buyAmount { get; set; } //资金流入量
        public double sellAmount { get; set;} //资金流出量
        public double mainBuyAmountPercent { get; set; } //主力流入量比例
        public double marketValue { get; set; } //市值
        public double PE { get; set; }
        public double PB { get; set; }
        public double PS { get; set; }

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
        public bool dateConfirm { get; set; }
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
