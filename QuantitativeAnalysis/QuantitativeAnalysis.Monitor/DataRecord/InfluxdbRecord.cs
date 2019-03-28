using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Utilities;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.DataAccess.Option;
using QuantitativeAnalysis;
using NLog;
using Autofac;
using QuantitativeAnalysis.Transaction;
using System.Data;
using System.Configuration;
using QuantitativeAnalysis.DataAccess.ETF;
using static QuantitativeAnalysis.Utilities.DateTimeExtension;
using InfluxData.Net.Common.Enums;
using InfluxData.Net.InfluxDb;
using InfluxData.Net.InfluxDb.Models;


namespace QuantitativeAnalysis.Monitor.DataRecord
{
    public class InfluxdbRecord
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private List<DateTime> tradedays = new List<DateTime>();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private StockMinuteRepository stockMinutelyRepo;
        private StockInfoRepository stockInfoRepo;
        private StockTickRepository tickRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReaderLocal;
        private SqlServerReader sqlReaderSource;
        private WindReader windReader = new WindReader();

        public InfluxdbRecord(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, TransactionDateTimeRepository dateRepo, StockInfoRepository stockInfoRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.dateRepo = dateRepo;
            this.stockInfoRepo = stockInfoRepo;
            this.sqlReaderLocal = new SqlServerReader(ConnectionType.Local);
            this.sqlReaderSource = new SqlServerReader(ConnectionType.Local);
            this.sqlWriter = new SqlServerWriter(ConnectionType.Local);
        }


        public void test()
        {
            //连接InfluxDb的API地址、账号、密码
            var infuxUrl = "http://localhost:8086/";
            var infuxUser = "sa";
            var infuxPwd = "maoheng0";
            var influx = new InfluxdbUtility(infuxUrl, infuxUser, infuxPwd);
            influx.AddData();
        }
        
      
    }


}
