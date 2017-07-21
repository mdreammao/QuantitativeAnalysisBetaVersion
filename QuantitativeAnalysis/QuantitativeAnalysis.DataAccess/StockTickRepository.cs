using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;

namespace QuantitativeAnalysis.DataAccess
{
    public class StockTickRepository : IStockRepository
    {
        public List<StockTransaction> GetStockTransaction(string code, DateTime start, DateTime end)
        {
            throw new NotImplementedException();
        }
    }
}
