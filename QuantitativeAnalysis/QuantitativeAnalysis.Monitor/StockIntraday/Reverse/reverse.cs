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

namespace QuantitativeAnalysis.Monitor.StockIntraday.Reverse
{
    public class reverse
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private WindReader windReader;
        private List<DateTime> tradedays = new List<DateTime>();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<GrabCeilingTransaction> transactionData = new List<GrabCeilingTransaction>();
        private StockMinuteRepository stockMinutelyRepo;
        private StockTickRepository stockTickRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private string index;
        private StockInfoRepository stockInfoRepo;
        private List<StockTransaction> underlyingAll = new List<StockTransaction>();
        List<StockIPOInfo>[] codeList;
        DateTime startDate;
        DateTime endDate;
        private double slipRatio = 0.001;
        private double feeRatioBuy = 0.0001;
        private double feeRatioSell = 0.0011;
        private double priceUnit = 0.01;
        private double loss = 0.015;

        public reverse(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, StockTickRepository stockTickRepo, StockInfoRepository stockInfoRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.stockTickRepo = stockTickRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
            this.windReader = new WindReader();
            this.stockInfoRepo = stockInfoRepo;
        }

        public void allStockBackTest(DateTime startDate, DateTime endDate)
        {
            var dic = getStockInfoList("000905.SH", startDate, endDate);
            int num = 0;
            int core = 12;
            List<StockIPOInfo>[] codeList = new List<StockIPOInfo>[core];
            this.startDate = startDate;
            this.endDate = endDate;
            for (int i = 0; i < core; i++)
            {
                codeList[i] = new List<StockIPOInfo>();
            }
            foreach (var item in dic)
            {
                num += 1;
                codeList[num % core].Add(item.Value);
            }
            this.codeList = codeList;
            Task[] taskArray = new Task[core];
            Console.WriteLine(DateTime.Now);
            for (int i = 0; i < taskArray.Length; i++)
            {
                Console.WriteLine("thread {0} start!", i);
                taskArray[i] = Task.Factory.StartNew(stockBackTestByList,i);
            }
            Task.WaitAll(taskArray);
            Console.WriteLine(DateTime.Now);
        }


        public void stockBackTestByList(object id)
        {
            var infos = codeList[Convert.ToInt32(id.ToString())];
            foreach (var info in infos)
            {
                stockBackTest(info, this.startDate, this.endDate);
            }
        }

        public void stockBackTest(StockIPOInfo info,DateTime startDate,DateTime endDate)
        {
            //获取数据
            if (startDate<info.IPODate)
            {
                startDate = info.IPODate;
            }
            if (endDate>info.DelistDate)
            {
                endDate = info.DelistDate;
            }
            //获取日线数据
            //var dayNow = stockDailyRepo.GetStockTransactionWithRedis(info.code, startDate, endDate);
            //获取交易日信息
            //var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var minuteNow = stockMinutelyRepo.GetStockTransactionFromLocalSqlByCode(info.code,startDate,endDate);
            //回测
        }


        public Dictionary<string, StockIPOInfo> getStockInfoList(string index, DateTime startDate, DateTime endDate)
        {
            Dictionary<string, StockIPOInfo> allDic = new Dictionary<string, StockIPOInfo>();
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<DateTime> lastDate = new List<DateTime>();
            lastDate.Add(tradedays.Last());
            var dic = getStockList(lastDate, index);
            foreach (var stock in dic)
            {
                if (stock.Value.IPODate > startDate)
                {
                    startDate = stock.Value.IPODate;
                }
                if (stock.Value.DelistDate < endDate)
                {
                    endDate = stock.Value.DelistDate;
                }
                if (allDic.ContainsKey(stock.Value.code) == false)
                {
                    allDic.Add(stock.Value.code, stock.Value);
                }
                //Console.WriteLine("code:{0} complete!", stock.Key);
            }
            return allDic;
        }

        private Dictionary<string, StockIPOInfo> getStockList(List<DateTime> days, string index)
        {
            Dictionary<string, StockIPOInfo> stockDic = new Dictionary<string, StockIPOInfo>();
            var stockInfo = stockInfoRepo.GetStockListInfoFromSql();
            foreach (var date in days)
            {
                var list = getCodeListFromLocalFile();
                foreach (var item in list)
                {
                    if (stockDic.ContainsKey(item) == false)
                    {
                        foreach (var stock in stockInfo)
                        {
                            if (stock.code == item)
                            {
                                stockDic.Add(item, stock);
                            }
                        }
                    }
                }

            }
            return stockDic;
        }


        private List<string> getCodeListFromLocalFile(string filePath= "D:\\BTP\\LocalDataBase\\dailyFactors\\000905_SH.CSV")
        {
            List<string> codeList = new List<string>();
            var dt = DataTableExtension.CSVToDatatable(filePath);
            foreach (DataRow dr in dt.Rows)
            {
                string code = Convert.ToString(dr["wind_code"]);
                codeList.Add(code);
            }
            return codeList;
        }

        private List<string> getIndexStocksFromWind(DateTime date, string index)
        {
            var rawData = windReader.GetDataSetTable("sectorconstituent", string.Format("date={0};windcode={1}", date.Date, index));
            List<string> codeList = new List<string>();
            foreach (DataRow dr in rawData.Rows)
            {
                codeList.Add(Convert.ToString(dr[1]));
            }
            var indexStr = index.Split('.');
            string name = string.Format("D:\\BTP\\LocalDataBase\\dailyFactors\\{0}_{1}.csv", indexStr[0], indexStr[1]);
            DataTableExtension.SaveCSV(rawData, name);
            return codeList;
        }

    }
}
