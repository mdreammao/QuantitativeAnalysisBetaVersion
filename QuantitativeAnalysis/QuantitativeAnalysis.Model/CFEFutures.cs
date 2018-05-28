using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class CFEFutures
    {
        public string code { get; set; }
        public DateTime expireDate { get; set; }
    }

    public class CFEFuturesChoice
    {
        public string code { get; set; }
        public DateTime expireDate { get; set; }
        public double annulizedBasis { get; set; }
        public double indexPrice { get; set; }
        public double price { get; set; }
        public double slip { get; set; }
    }

    public class CFEFuturesTradeRecord
    {
        public DateTime time { get; set; }
        public string code { get; set; }
        public double direction { get; set; }
        public double indexPrice { get; set; }
        public double price { get; set; }
        public DateTime expireDate { get; set; }
    }

    public class basisNetValue
    {
        public DateTime time { get; set; }
        public string code { get; set; }
        public DateTime expireDate { get; set; }
        public double basis { get; set; }
        public double annulizedBasis { get; set; }
        public double basisWithSlip { get; set; }
        public double annulizedBasisWithSlip { get; set; }
    }

    public class specialBasis
    {
        public DateTime time { get; set; }
        public DateTime expireDate1 { get; set; }
        public DateTime expireDate2 { get; set; }
        public string future1 { get; set; }
        public string future2 { get; set; }
        public double price1 { get; set; }
        public double price2 { get; set; }
        public double indexPrice { get; set;}
        public double duration1 { get; set; }
        public double duration2 { get; set; }
        public double basis1 { get; set; }
        public double basis2 { get; set; }
        public double basis12 { get; set; }
    }

    public class paraPerformance
    {
        //参数表现
        public double profit { get; set; }
        public double rateOfWinning { get; set; }
        public double rateOfWinningAfterSlip { get; set; }
        public double maxDrawdown { get; set; }
        //参数
        public double slip { get; set; }
        public double openStdMultiple { get; set; }
        public double closeStdMultiple { get; set; }
        public double stopLossStdMultiple { get; set; }
        public double numOfHistoricalDays { get; set; }
        public double KLineScale { get; set; }

    }
    

}
