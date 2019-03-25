using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Configuration;
using System.Data;
using QuantitativeAnalysis.Utilities;
using NLog;
using static QuantitativeAnalysis.Utilities.DateTimeExtension;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class StockMinuteFromTickRepository 
    {
        private TransactionDateTimeRepository dateTimeRepo;
        private StockTickRepository tickRepo;
        private StockDailyRepository dailyRepo;
        private RedisReader redisReader;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private RedisWriter redisWriter;
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();



        public StockMinuteFromTickRepository(ConnectionType type, IDataSource ds)
        {
            dateTimeRepo = new TransactionDateTimeRepository(type);
            tickRepo = new StockTickRepository(type,ds);
            sqlWriter = new SqlServerWriter(type);
            sqlReader = new SqlServerReader(type);
            redisReader = new RedisReader();
            redisWriter = new RedisWriter();
        }


        //获取分钟线数据，先从sql数据库拉取数据,如果没有数据，就从tick数据库拉取数据，并转换成分钟数据并存入sql数据库
        public List<StockMinuteTransaction> GetStockTransaction(string code, DateTime start, DateTime end)
        {
            //logger.Info(string.Format("begin to fetch stock{0} minute data from {1} to {2}...", code, start, end));
            var stocks = new List<StockMinuteTransaction>();
            var stockLack=new List<StockMinuteTransaction>();
            var tradingDates = dateTimeRepo.GetStockTransactionDate(start.Date, end.Date == DateTime.Now.Date ? end.Date.AddDays(-1) : end.Date);
            var timeInterval = new StockMinuteInterval(start, end, tradingDates);
            foreach (var currentDay in tradingDates)
            {
                var currentTime = currentDay.Date;
                //Console.WriteLine(currentTime);
                var stock = LoadStockMinuteFromSql(code, currentTime);
                if (stock.Count() == 0)
                {
                    //BulkLoadStockMinuteToSqlFromSource(code, currentTime);
                    try
                    {
                      stock = LoadStockMinuteToSqlFromSouceDaily(code, currentTime, false);
                      stockLack.AddRange(stock);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("exception!!!  {0}", e.Message);
                    }
                    
                }
                stocks.AddRange(stock);
            }
            //logger.Info(string.Format("completed fetching stock{0} minute data from {1} to {2}...", code, start, end));
            var dt = transactionListToDataTable(stockLack);
            WriteToSql(dt);
            return stocks;
        }
        #region internal method
        private List<StockMinuteTransaction> LoadStockMinuteFromSql(string code, DateTime currentTime)
        {
            List<StockMinuteTransaction> data = new List<StockMinuteTransaction>();
            var exist = ExistInSqlServer(code, currentTime.Date);
            if (exist != false)
            {
                DateTime dayStart = currentTime.Date + new TimeSpan(9, 25, 00);
                DateTime dayEnd = currentTime.Date + new TimeSpan(15, 00, 00);
                var sqlStr = string.Format(@"select  [Code],[DateTime] ,[open],[HIGH],[LOW],[CLOSE],[VOLUME],[Amount] from [StockMinuteTransactionByTick{0}].[dbo].[Transaction{1}] 
where Code='{2}' and DateTime>='{3}' and DateTime<='{4}'",
currentTime.Year, currentTime.ToString("yyyy-MM"), code,dayStart,dayEnd);
                var dt = sqlReader.GetDataTable(sqlStr);
                //dt转化为list
                data=dataTableToTransactionList(dt);
            }
            return data;
        }

        private Dictionary<DateTime, DateTime> SplitDateTimeMonthly(DateTime start, DateTime end)
        {
            var dic = new Dictionary<DateTime, DateTime>();
            var begin = start;
            for (int i = start.Month; i <= end.Month; i++)
            {
                var currentLast = (i < 12 ? new DateTime(start.Year, i + 1, 1).AddHours(-1) : new DateTime(start.Year + 1, 1, 1).AddHours(-1));
                currentLast = currentLast > end ? end : currentLast;
                dic.Add(begin, currentLast);
                begin = currentLast.AddHours(9);
            }
            return dic;
        }


        private List<StockMinuteTransaction> LoadStockMinuteToSqlFromSouceDaily(string code, DateTime currentTime,bool record=true)
        {
            List<StockMinuteTransaction> list = new List<StockMinuteTransaction>();
            IdentifyOrCreateDBAndTable(currentTime);
            var dtToday = tickRepo.GetStockTransaction(code, currentTime,currentTime,record);
            dtToday=(from data in dtToday where (data.TransactionDateTime.TimeOfDay <=new TimeSpan(15,0,1)) select data).ToList();
            var timelist = DataTimeStampExtension.GetStockMinuteTimeList();
            List<StockMinuteTransaction> minuteNow = new List<StockMinuteTransaction>();
            bool exists = true;
            if (dtToday.Count() == 0 || dtToday[dtToday.Count() - 1].Volume <= 0)
            {
                exists = false;
            }
            if (exists==true)
            {
                //处理开盘前的价格和成交量
                var day = currentTime.Date;
                double totalVolume = 0;
                double totalAmount = 0;
                double high = 0;
                double low = 0;
                double close = 0;
                double open = 0;
                var preOpen = (from data in dtToday where (data.TransactionDateTime.TimeOfDay < timelist[0]) select data).ToList();
                if (preOpen.Count != 0)
                {
                    var last = preOpen[preOpen.Count() - 1];
                    totalVolume = last.Volume;
                    totalAmount = last.Amount;
                    close = last.LastPrice;
                    open = close;
                    high = close;
                    low = close;

                }

                for (int i = 0; i < timelist.Count() - 1; i++)
                {
                    var dtNow = (from data in dtToday where (data.TransactionDateTime.TimeOfDay >= timelist[i]) && (data.TransactionDateTime.TimeOfDay <= timelist[i + 1]) select data).ToList();
                    if (dtNow.Count() != 0)
                    {
                        high = dtNow.Max(x => x.LastPrice);
                        low = dtNow.Min(x => x.LastPrice);
                        var listNow = dtNow.ToList();
                        var startData = listNow[0];
                        var endData = listNow[listNow.Count() - 1];
                        open = startData.LastPrice;
                        close = endData.LastPrice;
                        double volumeNew = endData.Volume;
                        double amountNew = endData.Amount;
                        double volume = volumeNew - totalVolume;
                        double amount = amountNew - totalAmount;
                        totalAmount = amountNew;
                        totalVolume = volumeNew;
                        StockMinuteTransaction KLine = new StockMinuteTransaction();
                        KLine.Amount = amount;
                        KLine.Volume = volume;
                        KLine.Open = open;
                        KLine.Close = close;
                        KLine.High = high;
                        KLine.Low = low;
                        KLine.Code = code;
                        KLine.DateTime = day.Date + timelist[i];
                        minuteNow.Add(KLine);
                    }
                    else
                    {
                        StockMinuteTransaction KLine = new StockMinuteTransaction();
                        KLine.Amount = 0;
                        KLine.Volume = 0;
                        KLine.Open = close;
                        KLine.Close = close;
                        KLine.High = close;
                        KLine.Low = close;
                        KLine.Code = code;
                        KLine.DateTime = day.Date + timelist[i];
                        minuteNow.Add(KLine);
                    }
                }
                var nearClose = (from data in dtToday where (data.TransactionDateTime.TimeOfDay > timelist[timelist.Count() - 1]) select data).ToList();
                if (nearClose.Count() != 0)
                {
                    high = nearClose.Max(x => x.LastPrice);
                    low = nearClose.Min(x => x.LastPrice);
                    var listNow = nearClose.ToList();
                    var startData = listNow[0];
                    var endData = listNow[listNow.Count() - 1];
                    open = startData.LastPrice;
                    close = endData.LastPrice;
                    double volumeNew = endData.Volume;
                    double amountNew = endData.Amount;
                    double volume = volumeNew - totalVolume;
                    double amount = amountNew - totalAmount;
                    totalAmount = amountNew;
                    totalVolume = volumeNew;
                    StockMinuteTransaction KLine = new StockMinuteTransaction
                    {
                        Amount = amount,
                        Volume = volume,
                        Open = open,
                        Close = close,
                        High = high,
                        Low = low,
                        Code = code,
                        DateTime = day.Date + timelist[timelist.Count() - 1]
                    };
                    minuteNow.Add(KLine);
                }
                else
                {
                    StockMinuteTransaction KLine = new StockMinuteTransaction();
                    KLine.Amount = 0;
                    KLine.Volume = 0;
                    KLine.Open = close;
                    KLine.Close = close;
                    KLine.High = close;
                    KLine.Low = close;
                    KLine.Code = code;
                    KLine.DateTime = day.Date + timelist[timelist.Count() - 1];
                    minuteNow.Add(KLine);
                }
                list = minuteNow;
            }
            else
            {
               // Console.WriteLine("date:{0},no tickData!!!", currentTime);
            }
            return list;
        }



        private void BulkLoadStockMinuteToSqlFromSource(string code, DateTime currentTime)
        {
            IdentifyOrCreateDBAndTable(currentTime);
            var latestTime = GetLatestTimeFromSql(code, currentTime);
            latestTime = latestTime == default(DateTime) ? new DateTime(currentTime.Year, currentTime.Month, 1) : latestTime.AddMinutes(1);
            if (latestTime.TimeOfDay>=new TimeSpan(14,59,00))
            {
                latestTime = latestTime.Date.AddDays(1);
            }
            var endTime = GetEndTime(currentTime);
            if (latestTime < endTime)
            {
                var dataTable = tickRepo.GetStockTransaction(code, latestTime, endTime);
                var days = dateTimeRepo.GetStockTransactionDate(latestTime.Date, endTime.Date);
                var timelist = DataTimeStampExtension.GetStockMinuteTimeList();
                List<StockMinuteTransaction> minuteAll = new List<StockMinuteTransaction>();
                foreach (var day in days)
                {
                    //Console.WriteLine("date:{0} start!!", day);
                    List<StockMinuteTransaction> minuteNow = new List<StockMinuteTransaction>();
                    var dtToday = (from data in dataTable where data.TransactionDateTime.Date == day.Date select data).ToList();
                    if (dtToday[dtToday.Count()-1].Volume<=0)
                    {
                        continue;
                    }
                    if (dtToday.Count()!=0)
                    {
                        //处理开盘前的价格和成交量
                        double totalVolume = 0;
                        double totalAmount = 0;
                        double high = 0;
                        double low = 0;
                        double close = 0;
                        double open = 0;
                        var preOpen = (from data in dtToday where (data.TransactionDateTime.TimeOfDay < timelist[0]) select data).ToList();
                        if (preOpen.Count!=0)
                        {
                            var last = preOpen[preOpen.Count() - 1];
                            totalVolume = last.Volume;
                            totalAmount= last.Amount;
                            close = last.LastPrice;
                            open = close;
                            high = close;
                            low = close;

                        }

                        for (int i = 0; i < timelist.Count() - 1; i++)
                        {
                            var dtNow = (from data in dtToday where (data.TransactionDateTime.TimeOfDay >= timelist[i]) && (data.TransactionDateTime.TimeOfDay <= timelist[i + 1]) select data).ToList();
                            if (dtNow.Count() != 0)
                            {
                                high = dtNow.Max(x => x.LastPrice);
                                low = dtNow.Min(x => x.LastPrice);
                                var listNow = dtNow.ToList();
                                var startData = listNow[0];
                                var endData = listNow[listNow.Count() - 1];
                                open = startData.LastPrice;
                                close = endData.LastPrice;
                                double volumeNew = endData.Volume;
                                double amountNew = endData.Amount;
                                double volume = volumeNew - totalVolume;
                                double amount = amountNew - totalAmount;
                                totalAmount = amountNew;
                                totalVolume = volumeNew;
                                StockMinuteTransaction KLine = new StockMinuteTransaction();
                                KLine.Amount = amount;
                                KLine.Volume = volume;
                                KLine.Open = open;
                                KLine.Close = close;
                                KLine.High = high;
                                KLine.Low = low;
                                KLine.Code = code;
                                KLine.DateTime = day.Date + timelist[i];
                                minuteNow.Add(KLine);
                            }
                            else
                            {
                                StockMinuteTransaction KLine = new StockMinuteTransaction();
                                KLine.Amount = 0;
                                KLine.Volume = 0;
                                KLine.Open = close;
                                KLine.Close = close;
                                KLine.High = close;
                                KLine.Low = close;
                                KLine.Code = code;
                                KLine.DateTime = day.Date + timelist[i];
                                minuteNow.Add(KLine);
                            }
                        }
                        var nearClose= (from data in dtToday where(data.TransactionDateTime.TimeOfDay > timelist[timelist.Count()-1]) select data).ToList();
                        if (nearClose.Count() != 0)
                        {
                            high = nearClose.Max(x => x.LastPrice);
                            low = nearClose.Min(x => x.LastPrice);
                            var listNow = nearClose.ToList();
                            var startData = listNow[0];
                            var endData = listNow[listNow.Count() - 1];
                            open = startData.LastPrice;
                            close = endData.LastPrice;
                            double volumeNew = endData.Volume;
                            double amountNew = endData.Amount;
                            double volume = volumeNew - totalVolume;
                            double amount = amountNew - totalAmount;
                            totalAmount = amountNew;
                            totalVolume = volumeNew;
                            StockMinuteTransaction KLine = new StockMinuteTransaction();
                            KLine.Amount = amount;
                            KLine.Volume = volume;
                            KLine.Open = open;
                            KLine.Close = close;
                            KLine.High = high;
                            KLine.Low = low;
                            KLine.Code = code;
                            KLine.DateTime = day.Date + timelist[timelist.Count()-1];
                            minuteNow.Add(KLine);
                        }
                        else
                        {
                            StockMinuteTransaction KLine = new StockMinuteTransaction();
                            KLine.Amount = 0;
                            KLine.Volume = 0;
                            KLine.Open = close;
                            KLine.Close = close;
                            KLine.High = close;
                            KLine.Low = close;
                            KLine.Code = code;
                            KLine.DateTime = day.Date + timelist[timelist.Count() - 1];
                            minuteNow.Add(KLine);
                        }
                    }
                    else
                    {
                        //for (int i = 0; i < timelist.Count(); i++)
                        //{
                        //    StockMinuteTransaction KLine = new StockMinuteTransaction();
                        //    KLine.Amount = 0;
                        //    KLine.Volume = 0;
                        //    KLine.Open =0;
                        //    KLine.Close = 0;
                        //    KLine.High = 0;
                        //    KLine.Low = 0;
                        //    KLine.Code = code;
                        //    KLine.DateTime = day.Date + timelist[i];
                        //    minuteNow.Add(KLine);
                        //}
                        Console.WriteLine("date:{0},no tickData!!!", day);
                    }
                    minuteAll.AddRange(minuteNow);
                }
                if (minuteAll.Count()!=0)
                {
                    var dt = transactionListToDataTable(minuteAll);
                    WriteToSql(dt);
                }
                
            }
        }

        private List<StockMinuteTransaction> dataTableToTransactionList(DataTable dt)
        {
            List<StockMinuteTransaction> list = new List<StockMinuteTransaction>();
            foreach (DataRow dr in dt.Rows)
            {
                StockMinuteTransaction now = new StockMinuteTransaction();
                now.Code =Convert.ToString(dr["Code"]);
                now.DateTime = Convert.ToDateTime(dr["DateTime"]);
                now.Open = Convert.ToDouble(dr["open"]);
                now.High = Convert.ToDouble(dr["high"]);
                now.Low = Convert.ToDouble(dr["low"]);
                now.Close = Convert.ToDouble(dr["close"]);
                now.Volume = Convert.ToDouble(dr["volume"]);
                now.Amount = Convert.ToDouble(dr["amount"]);
                list.Add(now);
            }
            return list;
        }


        private DataTable transactionListToDataTable(List<StockMinuteTransaction> list)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("DateTime", typeof(DateTime));
            dt.Columns.Add("Code", typeof(string));
            dt.Columns.Add("open", typeof(decimal));
            dt.Columns.Add("high", typeof(decimal));
            dt.Columns.Add("low", typeof(decimal));
            dt.Columns.Add("close", typeof(decimal));
            dt.Columns.Add("volume", typeof(decimal));
            dt.Columns.Add("amount", typeof(decimal));
            foreach (var item in list)
            {
                DataRow dr = dt.NewRow();
                dr["DateTime"] = item.DateTime;
                dr["Code"] = item.Code;
                dr["open"] = item.Open;
                dr["high"] = item.High;
                dr["low"] = item.Low;
                dr["close"] = item.Close;
                dr["volume"] = item.Volume;
                dr["amount"] = item.Amount;
                dt.Rows.Add(dr);
            }
            return dt;
        }


        private void WriteToSql(DataTable dataTable)
        {
            Dictionary<DateTime, DataTable> monthData = SplitDataTableMonthly(dataTable);
            foreach (var item in monthData)
            {
                IdentifyOrCreateDBAndTable(item.Key);
                sqlWriter.InsertBulk(item.Value, string.Format("[StockMinuteTransactionByTick{0}].dbo.[Transaction{1}]", item.Key.Year, item.Key.ToString("yyyy-MM")));
            }
        }

        private Dictionary<DateTime, DataTable> SplitDataTableMonthly(DataTable dataTable)
        {
            var monthData = new Dictionary<DateTime, DataTable>();
            foreach (DataRow r in dataTable.Rows)
            {
                var date = r["DateTime"].ToString().ConvertTo<DateTime>();
                var key = new DateTime(date.Year, date.Month, 1);
                if (!monthData.ContainsKey(key))
                {
                    var dt = dataTable.Clone();
                    dt.ImportRow(r);
                    monthData.Add(key, dt);
                }
                else
                    monthData[key].ImportRow(r);
            }
            return monthData;
        }

        /// <summary>
        /// 计算当前日期下的最后交易时间
        /// </summary>
        /// <param name="currentTime"></param>
        /// <returns></returns>
        private DateTime GetEndTime(DateTime currentTime)
        {
            if (currentTime.Year < DateTime.Now.Year)
                return dateTimeRepo.GetLastTransactionDate(currentTime, DateLevel.Year).AddHours(15).AddMinutes(0);
            var date = new DateTime(currentTime.Year, DateTime.Now.Month, DateTime.Now.Day - 1, 15, 0, 0);
            date = DateUtils.PreviousTradeDay(date);
            return new DateTime(date.Year, date.Month, date.Day, 15, 0, 0);
        }

        /// <summary>
        /// 找到当年SQL数据对应标的的最后一条数据的时间。为了插入新的数据做准备。
        /// </summary>
        /// <param name="code"></param>
        /// <param name="currentTime"></param>
        /// <returns></returns>
        private DateTime GetLatestTimeFromSql(string code, DateTime currentTime)
        {
            DateTime latest = default(DateTime);
            var sqlStr = string.Format(@"declare @date date,@tb_name nvarchar(60), @index int,@latest_date datetime,@sqlStr nvarchar(300),@tem_date datetime
set @date ='{0}-01-01'
set @index =1
while @index <=12
begin
	set @tb_name='[StockMinuteTransactionByTick'+datename(year,@date)+'].dbo.[Transaction'+ datename(year,@date)+'-'+datename(month,@date)+']'
	set @sqlStr ='select @tem_date=max([DateTime]) from '+@tb_name+' where code=''{1}'''
	if object_id(@tb_name) is not null
	begin
		exec sp_executesql @sqlStr,N'@tem_date datetime output',@tem_date output;
		if @tem_date is not null
		begin
			set @latest_date=@tem_date
		end
	end
	set @date = dateadd(month,1,@date)
	set @index=@index+1
end
select @latest_date", currentTime.Year, code.ToUpper());
            latest = sqlReader.ExecuteScriptScalar<DateTime>(sqlStr);
            return latest;
        }

        private void IdentifyOrCreateDBAndTable(DateTime dateTime)
        {
            var sqlLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"USE [master]
if db_id('StockMinuteTransactionByTick{0}') is null
begin
CREATE DATABASE [StockMinuteTransactionByTick{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'StockMinuteTransactionByTick{0}', FILENAME = N'{1}\StockMinuteTransactionByTick{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'StockMinuteTransactionByTick{0}_log', FILENAME = N'{1}\StockMinuteTransactionByTick{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [StockMinuteTransactionByTick{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [StockMinuteTransactionByTick{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[StockMinuteTransactionByTick{0}].[dbo].[Transaction{2}]') is null
begin
CREATE TABLE [StockMinuteTransactionByTick{0}].[dbo].[Transaction{2}](
	[Code] [varchar](20) NOT NULL,
	[DateTime] [datetime] NOT NULL,
	[open] [decimal](12, 4) NULL,
	[high] [decimal](12, 4) NULL,
	[low] [decimal](12, 4) NULL,
	[close] [decimal](12, 4) NULL,
	[volume] [decimal](20, 4) NULL,
	[amount] [decimal](20, 4) NULL,
	[UpdatedDateTime] [datetime] NULL,
 CONSTRAINT [PK_Transaction{2}] PRIMARY KEY CLUSTERED 
(
	[Code] ASC,
	[DateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
SET ANSI_PADDING OFF
ALTER TABLE [StockMinuteTransactionByTick{0}].[dbo].[Transaction{2}] ADD  CONSTRAINT [DF_Transaction{2}_UpdatedDateTime]  DEFAULT (getdate()) FOR [UpdatedDateTime]
end ", dateTime.Year, sqlLocation, dateTime.ToString("yyyy-MM"));
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        //private StockTransaction FetchStockMinuteTransFromRedis(string code, DateTime currentTime)
        //{
        //    var key = string.Format("{0}-{1}", code, currentTime.Year);
        //    var field = currentTime.ToString("yyyy-MM-dd HH:mm:ss");
        //    var stockStr = redisReader.HGet(key, field);
        //    return ConvertToStockTransaction(code, currentTime, stockStr);
        //}

        //private StockTransaction ConvertToStockTransaction(string code, DateTime time, string stockStr)
        //{
        //    if (string.IsNullOrEmpty(stockStr))
        //        return null;
        //    var res = stockStr.Split(',');
        //    return new StockTransaction()
        //    {
        //        Code = code,
        //        DateTime = time,
        //        Open = res[0].ConvertTo<double>(),
        //        High = res[1].ConvertTo<double>(),
        //        Low = res[2].ConvertTo<double>(),
        //        Close = res[3].ConvertTo<double>(),
        //        Volume = res[4].ConvertTo<double>(),
        //        Amount = res[5].ConvertTo<double>(),
        //        Level = StockTransactionLevel.Minute
        //    };
        //}

        private bool ExistInSqlServer(string code, DateTime date)
        {
            var sqlScript = string.Format(@"use master
if db_id('StockMinuteTransactionByTick{0}') is not null
begin
	if object_id('[StockMinuteTransactionByTick{0}].dbo.[Transaction{0}-{1}]') is not null
	begin
		select 1 from [StockMinuteTransactionByTick{0}].dbo.[Transaction{0}-{1}] where rtrim(Code)='{2}'
	end
end
else
begin
select 0
end ", date.Year, date.ToString("MM"), code);
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }
        #endregion

    }
}
