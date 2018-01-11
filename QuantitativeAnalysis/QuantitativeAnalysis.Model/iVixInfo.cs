using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class iVixInfo
    {
        public double strike { get; set; }
        public double duration { get; set; }
        public double ask { get; set;}
        public double bid { get; set; }
        public double askv { get; set; }
        public double bidv { get; set; }
        public double minutelyVolume { get; set; }
        public double sigma { get; set; }
        public double vega { get; set; }
        public double coefficient { get; set; }
    }

}
