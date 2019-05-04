using Autofac;
using NLog;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Monitor
{
    public class DiagonalSpread
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private StockMinuteRepository stockMinutelyRepo;
        private List<DateTime> tradedays = new List<DateTime>();

        public DiagonalSpread(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, string code)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            this.code = code;
        }

        public void compute(DateTime startDate, DateTime endDate)
        {
            DataPreparation(startDate, endDate);
        }
        private void DataPreparation(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var etfDaily = stockDailyRepo.GetStockTransactionWithRedis(code, startDate, endDate);
        }

        
    }
}
