using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;

namespace QuantitativeAnalysis.DataAccess
{
    public class StockMinuteRepository : IStockRepository
    {
        private TransactionDateTimeRepository dateTimeRepo = new TransactionDateTimeRepository(Infrastructure.ConnectionType.Default);

        public List<StockTransaction> GetStockTransaction(string code, DateTime start, DateTime end)
        {
            var stocks = new List<StockTransaction>();
            var tradingDate = dateTimeRepo.GetStockTransactionDate(start, end);
            
            return stocks;
        }

    }
}
