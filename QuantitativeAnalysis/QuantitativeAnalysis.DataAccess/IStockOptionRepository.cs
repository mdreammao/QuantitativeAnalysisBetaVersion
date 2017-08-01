using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
namespace QuantitativeAnalysis.DataAccess
{
    public interface IStockOptionRepository
    {
        List<StockOptionTransaction> GetStockOptionTransaction(string code, DateTime start, DateTime end);
    }
}
