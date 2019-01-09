using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class stockInfo
    {
        public string code { get; set; }
        public DateTime startDate { get; set; }
        public DateTime endDate { get; set; }
    }

    public class parameterPair
    {
        public string code { get; set; }
        public DateTime date { get; set; }
        public string strategy { get; set; }
        public double parameter1 { get; set; }
        public double parameter2 { get; set; }
        public double parameter3 { get; set; }
    }

}
