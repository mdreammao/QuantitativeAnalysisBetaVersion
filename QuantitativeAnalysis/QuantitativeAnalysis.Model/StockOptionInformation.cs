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
}
