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


    public class underlyingDividendInformation
    {
        public string underlying { get; set; }
        public DateTime date { get; set; }
        public DateTime yesterday { get; set; }
        public double dividend { get; set; }
    }

}
