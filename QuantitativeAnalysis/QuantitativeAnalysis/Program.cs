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
using NLog;
namespace QuantitativeAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            var logger = LogManager.GetCurrentClassLogger();
            logger.Info("main method start...");
            Initializer.Initialize(DataAccess.Infrastructure.ConnectionType.Default);
            var stockRepo = new StockInfoRepository(DataAccess.Infrastructure.ConnectionType.Default);
            //stockRepo.UpdateStockInfoToNow();
            logger.Info("main method end...");
        }
    }
}
