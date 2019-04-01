using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class OneByOneTransaction
    {
        public DateTime date { get; set; }
        public DateTime openTime { get; set; }
        public DateTime closeTime { get; set; }
        public string code { get; set; }
        public double maxOpenAmount { get; set; }
        public double maxCloseAmount { get; set; }
        public double position { get; set; }
        public double openPrice { get; set; }
        public double closePrice { get; set; }
        public string closeStatus { get; set; }
        public double yield { get; set; }
        public double parameter { get; set; }
    }

    public class GrabCeilingTransaction: OneByOneTransaction
    {
        public double fiveMinutesIncreaseBefore { get; set; }
        public double fiveMinutesIncreaseAfter { get; set; }
        public double oneMinuteIncreaseAfter { get; set; }
        public double limitPrice { get; set; }
    }


    public class OneByOneTransactionDaily : OneByOneTransaction
    {
        public double openAdjust { get; set; }
        public double closeAdjust { get; set; }
    }

    public class netvalueDaily
    {
        public DateTime date { get; set; }
        public double netvalue { get; set; }
    }

}
