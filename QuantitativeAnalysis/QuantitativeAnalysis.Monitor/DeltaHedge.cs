using Autofac;
using NLog;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.DataAccess.Stock;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Monitor
{
    public class DeltaHedge
    {
        private double rate;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockTickRepository stockRepo;
        private StockDailyRepository stockDailyRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;


        public DeltaHedge(StockTickRepository stockRepo, StockDailyRepository stockDailyRepo, string code)
        {
            this.stockRepo = stockRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
            this.code = code;
        }

        public void compute(DateTime startDate, DateTime endDate,string stockCode)
        {
            
        }
    }
}
