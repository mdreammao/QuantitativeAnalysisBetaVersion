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
using ExcelDataReader;
using System.IO;

namespace QuantitativeAnalysis.Monitor.IndexRelated
{
    public class StockDataStore
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private List<DateTime> tradedays = new List<DateTime>();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData;
        private StockMinuteRepository stockMinutelyRepo;
        private StockInfoRepository stockInfoRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private WindReader windReader;



        public StockDataStore(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, TransactionDateTimeRepository dateRepo,StockInfoRepository stockInfoRepo)
        {
            this.stockDailyRepo = stockDailyRepo;
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.dateRepo = dateRepo;
            this.stockInfoRepo = stockInfoRepo;
            this.windReader = new WindReader();
            //stockInfoRepo.UpdateStockInfoToNow();
            
        }

        public void getStockData(string index,DateTime startDate,DateTime endDate)
        {
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var dic = getStockList(tradedays, index);
            foreach (var stock in dic)
            {
                if (stock.Value.IPODate>startDate)
                {
                    startDate = stock.Value.IPODate;
                }
                if (stock.Value.DelistDate<endDate)
                {
                    endDate = stock.Value.DelistDate;
                }
                var data = stockMinutelyRepo.GetStockTransaction(stock.Key, startDate, endDate);
                Console.WriteLine("code:{0} complete!", stock.Key);
            }
        }

        private Dictionary<string, StockIPOInfo> getStockList(List<DateTime> days,string index)
        {
            Dictionary<string, StockIPOInfo> stockDic = new Dictionary<string, StockIPOInfo>();
            var stockInfo=stockInfoRepo.GetStockListInfoFromSql();
            foreach (var date in days)
            {
                var list = getIndexStocks(date, index);
                foreach (var item in list)
                {
                    if (stockDic.ContainsKey(item)==false)
                    {
                        foreach (var stock in stockInfo)
                        {
                            if (stock.code==item)
                            {
                                stockDic.Add(item, stock);
                            }
                        }
                    }
                }

            }
            return stockDic;
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
