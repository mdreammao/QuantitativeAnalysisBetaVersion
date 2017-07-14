using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
namespace QuantitativeAnalysis.DataAccess
{
    public interface IStockRepository
    {
        StockTransaction GetStockDailyTransaction(string code, DateTime date);
        StockTransaction GetStockMinuteTransaction(string code, DateTime dateTime);

    }
}
