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
namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class StockMinuteRepository : IStockRepository
    {
        private TransactionDateTimeRepository dateTimeRepo;
        private RedisReader redisReader;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private RedisWriter redisWriter;
        private IDataSource dataSource;
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();
        public StockMinuteRepository(ConnectionType type,IDataSource ds)
        {
            dateTimeRepo = new TransactionDateTimeRepository(type);
            sqlWriter = new SqlServerWriter(type);
            sqlReader = new SqlServerReader(type);
            redisReader = new RedisReader();
            redisWriter = new RedisWriter();
            dataSource = ds;
        }
        public List<StockTransaction> GetStockTransaction(string code, DateTime start, DateTime end)
        {
            logger.Info(string.Format("begin to fetch stock{0} minute data from {1} to {2}...", code, start, end));
            var stocks = new List<StockTransaction>();
            var tradingDates = dateTimeRepo.GetStockTransactionDate(start.Date, end.Date==DateTime.Now.Date?end.Date.AddDays(-1):end.Date);
            var timeInterval = new StockMinuteInterval(start, end, tradingDates);
            while (timeInterval.MoveNext())
            {
                var currentTime = timeInterval.Current;
                StockTransaction stock = FetchStockMinuteTransFromRedis(code, currentTime);
                if (stock == null)
                {
                    BulkLoadStockMinuteToSqlFromSource(code, currentTime);
                    BulkLoadStockMinuteToRedisFromSql(code, currentTime);
                    stock = FetchStockMinuteTransFromRedis(code, currentTime);
                }
                stocks.Add(stock);
            }
            logger.Info(string.Format("completed fetching stock{0} minute data from {1} to {2}...", code, start, end));
            return stocks;
        }
        #region internal method
        private void BulkLoadStockMinuteToRedisFromSql(string code, DateTime currentTime)
        {
            DateTime latestTime = GetLatestTimeFromRedis(code, currentTime);
            var start = latestTime == default(DateTime) ? new DateTime(currentTime.Year, 1, 1) : latestTime.AddMinutes(1);
            var end = GetEndTime(currentTime);
            if (start < end)
            {
                Dictionary<DateTime, DateTime> dateSpan = SplitDateTimeMonthly(start, end);
                foreach (var item in dateSpan)
                {
                    var exist = ExistInSqlServer(code, item.Value);
                    if (exist!=false)
                    {
                        var sqlStr = string.Format(@"select  [Code],[DateTime] ,[open],[HIGH],[LOW],[CLOSE],[VOLUME],[Amount] from [StockMinuteTransaction{0}].[dbo].[Transaction{1}] 
where Code='{2}' and DateTime>='{3}' and DateTime<='{4}'",
        item.Key.Year, item.Key.ToString("yyyy-MM"), code, item.Key, item.Value);
                        var dt = sqlReader.GetDataTable(sqlStr);
                        WriteToRedis(dt);
                    }
                }
            }
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

        private void WriteToRedis(DataTable dt)
        {
            foreach (DataRow row in dt.Rows)
            {
                var array = row.ItemArray;
                var date = array[1].ToString().ToDateTime();
                var key = string.Format("{0}-{1}", array[0], date.Year);
                string val = string.Join(",", array.Skip(2));
                redisWriter.HSet(key, date.ToString("yyyy-MM-dd HH:mm:ss"), string.Join(",", array.Skip(2)));
            }
        }

        private DateTime GetLatestTimeFromRedis(string code, DateTime currentTime)
        {
            var key = string.Format("{0}-{1}", code, currentTime.Year);
            var res = redisReader.HGetAllFields(key).ConvertTo<DateTime>();
            if (res == null || res.Count == 0)
                return default(DateTime);
            return res.Max();
        }

        private void BulkLoadStockMinuteToSqlFromSource(string code, DateTime currentTime)
        {
            IdentifyOrCreateDBAndTable(currentTime);
            var latestTime = GetLatestTimeFromSql(code, currentTime);
            latestTime = latestTime == default(DateTime) ? new DateTime(currentTime.Year, 1, 1) : latestTime.AddMinutes(1);
            var endTime = GetEndTime(currentTime);
            if (latestTime < endTime)
            {
                var dataTable = dataSource.Get(code, latestTime, endTime);
                WriteToSql(dataTable);
            }
        }

        private void WriteToSql(DataTable dataTable)
        {
            Dictionary<DateTime, DataTable> monthData = SplitDataTableMonthly(dataTable);
            foreach (var item in monthData)
            {
                IdentifyOrCreateDBAndTable(item.Key);
                sqlWriter.InsertBulk(item.Value, string.Format("[StockMinuteTransaction{0}].dbo.[Transaction{1}]", item.Key.Year, item.Key.ToString("yyyy-MM")));
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

        private DateTime GetEndTime(DateTime currentTime)
        {
            if (currentTime.Year < DateTime.Now.Year)
                return dateTimeRepo.GetLastTransactionDate(currentTime, DateLevel.Year).AddHours(15).AddMinutes(1);
            return new DateTime(currentTime.Year, DateTime.Now.Month, DateTime.Now.Day - 1, 15, 1, 0);
        }

        private DateTime GetLatestTimeFromSql(string code, DateTime currentTime)
        {
            DateTime latest = default(DateTime);
            var sqlStr = string.Format(@"declare @date date,@tb_name nvarchar(60), @index int,@latest_date datetime,@sqlStr nvarchar(300),@tem_date datetime
set @date ='{0}-01-01'
set @index =1
while @index <=12
begin
	set @tb_name='[StockMinuteTransaction'+datename(year,@date)+'].dbo.[Transaction'+ datename(year,@date)+'-'+datename(month,@date)+']'
	set @sqlStr ='select @tem_date=max([DateTime]) from '+@tb_name+' where code=''{1}'''
	if object_id(@tb_name) is not null
	begin
		exec sp_executesql @sqlStr,N'@tem_date datetime output',@tem_date output;
		if @tem_date is not null
		begin
			set @latest_date=@tem_date
		end
		else
			break;
	end
	set @date = dateadd(month,1,@date)
	set @index=@index+1
end
select @latest_date", currentTime.Year,code.ToUpper());
            latest = sqlReader.ExecuteScriptScalar<DateTime>(sqlStr);          
            return latest;
        }

        private void IdentifyOrCreateDBAndTable(DateTime dateTime)
        {
            var sqlLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"USE [master]
if db_id('StockMinuteTransaction{0}') is null
begin
CREATE DATABASE [StockMinuteTransaction{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'StockMinuteTransaction{0}', FILENAME = N'{1}\StockMinuteTransaction{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'StockMinuteTransaction{0}_log', FILENAME = N'{1}\StockMinuteTransaction{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [StockMinuteTransaction{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [StockMinuteTransaction{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[StockMinuteTransaction{0}].[dbo].[Transaction{2}]') is null
begin
CREATE TABLE [StockMinuteTransaction{0}].[dbo].[Transaction{2}](
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
ALTER TABLE [StockMinuteTransaction{0}].[dbo].[Transaction{2}] ADD  CONSTRAINT [DF_Transaction{2}_UpdatedDateTime]  DEFAULT (getdate()) FOR [UpdatedDateTime]
end ", dateTime.Year, sqlLocation, dateTime.ToString("yyyy-MM"));
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private StockTransaction FetchStockMinuteTransFromRedis(string code, DateTime currentTime)
        {
            var key = string.Format("{0}-{1}", code, currentTime.Year);
            var field = currentTime.ToString("yyyy-MM-dd HH:mm:ss");
            var stockStr = redisReader.HGet(key, field);
            return ConvertToStockTransaction(code, currentTime, stockStr);
        }

        private StockTransaction ConvertToStockTransaction(string code, DateTime time, string stockStr)
        {
            if (string.IsNullOrEmpty(stockStr))
                return null;
            var res = stockStr.Split(',');
            return new StockTransaction()
            {
                Code = code,
                DateTime = time,
                Open = res[0].ConvertTo<double>(),
                High = res[1].ConvertTo<double>(),
                Low = res[2].ConvertTo<double>(),
                Close = res[3].ConvertTo<double>(),
                Volume = res[4].ConvertTo<double>(),
                Amount = res[5].ConvertTo<double>(),
                Level = StockTransactionLevel.Minute
            };
        }

        private bool ExistInSqlServer(string code, DateTime date)
        {
            var sqlScript = string.Format(@"use master
if db_id('StockMinuteTransaction{0}') is not null
begin
	if object_id('[StockMinuteTransaction{0}].dbo.[Transaction{0}-{1}]') is not null
	begin
		select 1 from [StockMinuteTransaction{0}].dbo.[Transaction{0}-{1}] where rtrim(Code)='{2}'
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
