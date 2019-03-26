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

namespace QuantitativeAnalysis.Monitor.DataRecord
{
    public class DailyDataRecord
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
        private SqlServerReader sqlReader;
        private WindReader windReader = new WindReader();


        public DailyDataRecord(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, StockTickRepository tickRepo, TransactionDateTimeRepository dateRepo,StockInfoRepository stockInfoRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.tickRepo = tickRepo;
            this.dateRepo = dateRepo;
            this.stockInfoRepo = stockInfoRepo;
        }

        public void getStockDailyData(DateTime startDate, DateTime endDate)
        {
            var list = stockInfoRepo.GetStockListInfoFromSql();
            foreach (var item in list)
            {
                DateTime startTime = startDate;
                DateTime endTime = endDate;
                if (startDate<item.IPODate)
                {
                    startTime = item.IPODate;
                }
                if (endDate>item.DelistDate)
                {
                    endTime = item.DelistDate;
                }
                var data = stockDailyRepo.GetStockTransaction(item.code, startTime, endTime);
                Console.WriteLine("code:{0} dailyData form {1} to {2} complete!", item.code, startTime, endTime);
            }
        }

        public void getStockFromIndexDailyData(string index)
        {
            DateTime yesterday = DateTime.Now.AddDays(-1).Date;
            var list = getIndexStocks(yesterday, index);
            var stockAll = stockInfoRepo.GetStockListInfoFromSql();
            foreach (var item in stockAll)
            {
                if (list.Contains(item.code)==true)
                {
                    DateTime startDate = item.IPODate;
                    DateTime endDate = item.DelistDate;
                    if (endDate>yesterday)
                    {
                        endDate = yesterday;
                    }
                    var data = stockDailyRepo.GetStockTransaction(item.code, startDate, endDate);
                    Console.WriteLine("code:{0} dailyData form {1} to {2} complete!", item.code, startDate, endDate);
                }
                
            }
        }


        private List<string> getIndexStocks(DateTime date, string index)
        {
            var rawData = windReader.GetDataSetTable("sectorconstituent", string.Format("date={0};windcode={1}", date.Date, index));
            List<string> codeList = new List<string>();
            foreach (DataRow dr in rawData.Rows)
            {
                codeList.Add(Convert.ToString(dr[1]));
            }
            return codeList;
        }
    }
}
