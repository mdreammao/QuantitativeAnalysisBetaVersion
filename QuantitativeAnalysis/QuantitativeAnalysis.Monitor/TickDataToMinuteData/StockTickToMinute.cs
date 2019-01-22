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
        private List<StockTransaction> transferedData = new List<StockTransaction>();
        private StockDailyRepository stockDailyRepo;
        private StockMinuteRepository stockMinutelyRepo;
        private List<DateTime> tradedays;
        

        public StockTickToMinute(TransactionDateTimeRepository dateRepo, StockDailyRepository stockDailyRepo, StockMinuteRepository stockMinutelyRepo,StockTickRepository tickRepo,DateTime startDate,DateTime endDate)
        {
            this.tickRepo = tickRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            getStockTickData("510050.SH");
        }

        private void getStockTickData(string code)
        {
            foreach (var date in tradedays)
            {
                var tick = tickRepo.GetStockTransaction(code, date, date);
                var day = stockDailyRepo.GetStockTransaction("510050.SH", date, date);
                double open = day[0].Open; 
                var minute = tranferTickToMinuteDayByDay(code,date,open,tick);
                var minute2 = stockMinutelyRepo.GetStockTransaction("510050.SH", date, date);
            }
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
