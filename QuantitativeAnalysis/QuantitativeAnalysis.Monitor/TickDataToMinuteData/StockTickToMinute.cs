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

namespace QuantitativeAnalysis.Monitor.TickDataToMinuteData
{
    public class StockTickToMinute
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockTickRepository tickRepo;
        private StockInfoRepository infoRepo;
        private List<StockTransaction> transferedData = new List<StockTransaction>();
        private StockDailyRepository stockDailyRepo;
        private StockMinuteFromTickRepository stockMinutelyRepo;
        private List<DateTime> tradedays;
        private WindReader windReader=new WindReader();


        public StockTickToMinute(TransactionDateTimeRepository dateRepo, StockDailyRepository stockDailyRepo, StockMinuteFromTickRepository stockMinutelyRepo,StockTickRepository tickRepo,StockInfoRepository infoRepo)
        {
            this.tickRepo = tickRepo;
            this.dateRepo = dateRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.infoRepo = infoRepo;
           
        }

        public void getStockMinuteFromSqlByIndex(string index, DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var dic = getStockList(tradedays, index);
            foreach (var item in dic)
            {
                DateTime startTime = startDate;
                DateTime endTime = endDate;
                if (startTime<item.Value.IPODate)
                {
                    startTime = item.Value.IPODate;
                }
                if (endTime>item.Value.DelistDate)
                {
                    endTime = item.Value.DelistDate;
                }
                Console.WriteLine("stock:{0} start!", item.Value.code);
                getStockMinuteData(item.Value.code, startTime, endTime);
            }
        }

        private Dictionary<string, StockIPOInfo> getStockList(List<DateTime> days, string index)
        {
            Dictionary<string, StockIPOInfo> stockDic = new Dictionary<string, StockIPOInfo>();
            var stockInfo = infoRepo.GetStockListInfoFromSql();
            for (int i = 0; i < days.Count; i=i+20)
            {
                var date = days[i];
                var list = getIndexStocks(date, index);
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

        private void getStockMinuteData(string code,DateTime startDate,DateTime endDate)
        {
            var tmp=stockMinutelyRepo.GetStockTransaction(code, startDate, endDate);
        }

        private List<StockTransaction> tranferTickToMinuteDayByDay(string code,DateTime date,double openData,List<StockTickTransaction> tickData)
        {
            List<StockTransaction> minuteList = new List<StockTransaction>();

            //构造时间间隔的左闭右开区间利用9点30分的分钟线包含时间[9:30:00,9:31:00)
            List<Interval> timeList = new List<Interval>();
            for (int index = 0; index < 240; index++)
            {
                TimeSpan startTime = DataTimeStampExtension.GetStockMinuteTimeByIndex(index);
                TimeSpan endTime = startTime.Add(new TimeSpan(0, 0, 60));
                if (index==119 || index==239)
                {
                    endTime = startTime.Add(new TimeSpan(0, 0, 61));
                }
                Interval interval = new Interval(startTime,endTime,new TimeSpan(0,0,60));
                timeList.Add(interval);
            }
            double high = openData;
            double open = openData;
            double low = openData;
            double close = openData;
            double amount = 0;
            double volume = 0;
            double startAmount = 0;
            double endAmount = 0;
            double startVolume = 0;
            double endVolume = 0;
            foreach (var item in timeList)
            {
                int num = 0;
                for (int i = 0; i < tickData.Count(); i++)
                {
                    
                    if (tickData[i].TransactionDateTime.TimeOfDay >= item.Begin && tickData[i].TransactionDateTime.TimeOfDay < item.End)
                    {
                        num = num + 1;
                        endAmount = tickData[i].Amount;
                        endVolume = tickData[i].Volume;
                        close = tickData[i].LastPrice;
                        if (num == 1)
                        {
                            open = tickData[i].LastPrice;
                            high = open;
                            low = open;
                        }
                        if (tickData[i].LastPrice > high)
                        {
                            high = tickData[i].LastPrice;
                        }
                        if (tickData[i].LastPrice < low)
                        {
                            low = tickData[i].LastPrice;
                        }
                    }
                }
                if (num == 0) //这一分钟没有成交
                {
                    amount = 0;
                    volume = 0;
                    open = close;
                    high = close;
                    low = close;
                }
                else
                {
                    amount = endAmount - startAmount;
                    volume = endVolume - startVolume;
                    startAmount = endAmount;
                    startVolume = endVolume;
                }
                StockTransaction kLines = new StockTransaction();
                kLines.Open = open;
                kLines.High = high;
                kLines.Low = low;
                kLines.Close = close;
                kLines.Amount = amount;
                kLines.Volume = volume;
                kLines.Code = code;
                kLines.DateTime =date.Date+item.Begin;
                kLines.Level = StockTransactionLevel.Minute;
                minuteList.Add(kLines);
                open = close;
                high = close;
                low = close;
            }
            return minuteList;
            
        }
    }
}
