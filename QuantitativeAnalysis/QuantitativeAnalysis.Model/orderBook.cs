using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Model
{
    public class Book
    {
        public string code { get; set; }
        public DateTime time { get; set; }
        public int volume { get; set; }
        public double price { get; set; }
    }

    public class OrderBook : Book
    {
        public int waitingVolume { get; set; }
        public int orderId { get; set; }
    }

    public class TradeBook : Book
    {
        public int orderId { get; set; }
        public int tradeId { get; set; }
    }
}
