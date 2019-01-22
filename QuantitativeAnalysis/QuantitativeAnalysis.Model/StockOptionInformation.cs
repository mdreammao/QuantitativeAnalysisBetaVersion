using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class StockOptionInformation
    {
        public string code {get;set;}
        public string name {get;set;}
        public string underlying {get;set;}
        public string exerciseMode {get;set;}
        public double strike {get;set;}
        public int unit {get;set;}
        public string type {get;set;}
        public DateTime listedDate {get;set;}
        public DateTime expireDate {get;set;}
    }

    public class StockOptionInformationWithModified
    {
        public string code { get; set; }
        public string name { get; set; }
        public string underlying { get; set; }
        public string exerciseMode { get; set; }
        public double strike { get; set; }
        public int unit { get; set; }
        public double strikeBeforeModified { get; set; }
        public int unitBeforeModifed { get; set; }
        public string type { get; set; }
        public DateTime listedDate { get; set; }
        public DateTime expireDate { get; set; }
        public DateTime dividendDate { get; set; }
        public double dividend { get; set; }
        public bool existsModified { get; set; }
    }

    public class optionGreeks
    {
        public string code { get; set; }
        public string name { get; set; }
        public string underlying { get; set; }
        public double strike { get; set; }
        public int unit { get; set; }
        public DateTime expireDate { get; set; }
        public double duration { get; set; }
        public double modifiedDuration { get; set; }
        public DateTime today { get; set; }
        public double Basis { get; set; }
        public double cashBasis { get; set; }
        public double delta { get; set; }
        public double cashDelta { get; set; }
        public double gamma { get; set; }
        public double cashGamma { get; set; }
        public double theta { get; set; }
        public double thetaOfInterest { get; set; }
        public double thetaOfNonInterest { get; set; }
        public double vega { get; set; }
    }


    public class underlyingDividendInformation
    {
        public string underlying { get; set; }
        public DateTime date { get; set; }
        public DateTime yesterday { get; set; }
        public double dividend { get; set; }
    }

}
