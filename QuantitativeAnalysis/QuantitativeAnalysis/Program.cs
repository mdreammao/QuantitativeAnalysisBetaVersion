using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Utilities;
using StackExchange.Redis;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.DataAccess.Option;

namespace QuantitativeAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            Initializer.CreateDBAndTableIfNecessary(DataAccess.Infrastructure.ConnectionType.Default);
            var stockRepo = new StockInfoRepository(DataAccess.Infrastructure.ConnectionType.Default);
            stockRepo.UpdateStockInfoToNow();
            
        }
    }
}
