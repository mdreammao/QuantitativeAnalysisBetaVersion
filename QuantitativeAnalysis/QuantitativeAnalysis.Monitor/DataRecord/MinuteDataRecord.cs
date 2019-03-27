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


namespace QuantitativeAnalysis.Monitor.DataRecord
{
    public class MinuteDataRecord
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


        public MinuteDataRecord(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, TransactionDateTimeRepository dateRepo, StockInfoRepository stockInfoRepo, ConnectionType type= ConnectionType.Local2017)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.dateRepo = dateRepo;
            this.stockInfoRepo = stockInfoRepo;
            this.sqlReaderLocal = new SqlServerReader(ConnectionType.Local);
            this.sqlReaderSource = new SqlServerReader(type);
            this.sqlWriter = new SqlServerWriter(ConnectionType.Local);

        }

        public void getStockMinutelyData(DateTime startDate, DateTime endDate)
        {
            var list = stockInfoRepo.GetStockListInfoFromSql();
            int num = 0;
            foreach (var item in list)
            {
                DateTime startTime = startDate;
                DateTime endTime = endDate;
                if (startDate < item.IPODate)
                {
                    startTime = item.IPODate;
                }
                if (endDate > item.DelistDate)
                {
                    endTime = item.DelistDate;
                }
                num += 1;
                if (num>=0)
                {
                    //BulkLoadStockMinuteToSqlFromSql(item.code, startTime, endTime);
                    BulkLoadStockMinuteOrerByCode(item.code, startTime, endTime);
                }
               
                Console.WriteLine("code({3} of 3704):{0} dailyData form {1} to {2} complete! ", item.code, startTime, endTime,num);
            }
        }

        /// <summary>
        /// 删除所有原始股票数据，慎用！！！
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        public void deleteOldDataAll(DateTime startDate, DateTime endDate)
        {
            var list = stockInfoRepo.GetStockListInfoFromSql();
            foreach (var item in list)
            {
                DateTime startTime = startDate;
                DateTime endTime = endDate;
                if (startDate < item.IPODate)
                {
                    startTime = item.IPODate;
                }
                if (endDate > item.DelistDate)
                {
                    endTime = item.DelistDate;
                }
                DeleteDataByCode(item.code, startDate, endDate);
                Console.WriteLine("code:{0} dailyData form {1} to {2} delete complete!", item.code, startTime, endTime);
            }
        }
        
      
        public void BulkLoadStockMinuteOrerByCode(string code,DateTime startDate,DateTime endDate)
        {
            IdentifyOrCreateDBAndTableByCode(code);
            var existsLastestTime = GetLatestTimeFromSql(code);
            DateTime start = DateUtils.NextTradeDay(existsLastestTime.Date);
            //取出数据
            try
            {
                var originalData = getDataFromSql(code, start, endDate);
                //存入数据
               WriteToSqlByCode(originalData,code);
            }
            catch (Exception e)
            {

                Console.WriteLine(e.Message);
            }
        }



        /// <summary>
        /// 从sql源中取出数据并存入本地数据库
        /// </summary>
        /// <param name="code"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        public void BulkLoadStockMinuteToSqlFromSql(string code, DateTime startDate,DateTime endDate)
        {

            //将开始时间和结束时间按月切分
            var dateMonthly = splitDateMonthly(startDate, endDate);
            //逐月进行遍历，查看原数据库的缺失。如果没有缺失就跳过，如果有缺失就从源读取数据，并存入本地数据库
            foreach (var item in dateMonthly)
            {
                //Console.WriteLine("data of date:{0} processing........", item.Key);
                var dayList = item.Value;
                IdentifyOrCreateDBAndTable(dayList.First());//如果没有数据库和数据表就重新建立
                var existsLastestTime = GetLatestTimeOfMonthFromSql(code, dayList.First());
                if (existsLastestTime.Date>=dayList.Last().Date)
                {
                    //Console.WriteLine("data of date:{0} old Data exists!", item.Key);
                }
                else
                {
                    DateTime startThisMonth = DateUtils.NextTradeDay(existsLastestTime.Date);
                    if (startThisMonth<dayList.First())
                    {
                        startThisMonth = dayList.First();
                    }
                    DateTime endThisMonth = dayList.Last();
                    //取出数据
                    try
                    {
                        var originalData = getDataFromSql(code, startThisMonth, endThisMonth);
                        //存入数据
                        WriteToSqlMonthByMonth(originalData);
                    }
                    catch (Exception e)
                    {

                        Console.WriteLine(e.Message);
                    }
                    
                   

                }
                //Console.WriteLine("data of date:{0} complete!", item.Key);
            }
        }




        private void IdentifyOrCreateDBAndTableByCode(string code)
        {
            var sqlLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            code = code.ToUpper();
            string code1 = code.Split('.')[0];
            string code2 = code.Split('.')[1];
            var sqlScript = string.Format(@"USE [master]
if db_id('StockMinuteTransaction') is null
begin
CREATE DATABASE [StockMinuteTransaction]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'StockMinuteTransaction', FILENAME = N'{2}\StockMinuteTransaction.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'StockMinuteTransaction_log', FILENAME = N'{2}\StockMinuteTransaction_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [StockMinuteTransaction] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [StockMinuteTransaction].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[StockMinuteTransaction].[dbo].[{0}_{1}]') is null
begin
CREATE TABLE [StockMinuteTransaction].[dbo].[{0}_{1}](
	[Code] [varchar](20) NOT NULL,
	[DateTime] [datetime] NOT NULL,
	[open] [decimal](12, 4) NULL,
	[high] [decimal](12, 4) NULL,
	[low] [decimal](12, 4) NULL,
	[close] [decimal](12, 4) NULL,
	[volume] [decimal](20, 4) NULL,
	[amount] [decimal](20, 4) NULL,
	[UpdatedDateTime] [datetime] NULL,
 CONSTRAINT [PK_{0}_{1}] PRIMARY KEY CLUSTERED 
(
	[Code] ASC,
	[DateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
SET ANSI_PADDING OFF
ALTER TABLE [StockMinuteTransaction].[dbo].[{0}_{1}] ADD  CONSTRAINT [DF_{0}_{1}_UpdatedDateTime]  DEFAULT (getdate()) FOR [UpdatedDateTime]
end ", code1, code2,sqlLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }


        /// <summary>
        /// 创建数据库及数据表
        /// </summary>
        /// <param name="dateTime">日期</param>
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


        /// <summary>
        /// 找到当月SQL数据对应标的的最后一条数据的时间。为了插入新的数据做准备。
        /// </summary>
        /// <param name="code"></param>
        /// <param name="currentTime"></param>
        /// <returns></returns>
        private DateTime GetLatestTimeOfMonthFromSql(string code, DateTime currentTime)
        {
            DateTime latest = default(DateTime);
            var sqlStr = string.Format(@"SELECT max([DateTime])
  FROM [StockMinuteTransaction{0}].[dbo].[Transaction{1}] where Code='{2}'", currentTime.Year,currentTime.ToString("yyyy-MM"), code.ToUpper());
            latest = sqlReaderLocal.ExecuteScriptScalar<DateTime>(sqlStr);
            return latest;
        }

        private DateTime GetLatestTimeFromSql(string code)
        {
            DateTime latest = default(DateTime);
            code = code.ToUpper();
            string code1 = code.Split('.')[0];
            string code2 = code.Split('.')[1];
            var sqlStr = string.Format(@"SELECT max([DateTime])
  FROM [StockMinuteTransaction].[dbo].[{0}_{1}]", code1,code2);
            latest = sqlReaderLocal.ExecuteScriptScalar<DateTime>(sqlStr);
            return latest;
        }

        /// <summary>
        /// 找到当年SQL数据对应标的的最后一条数据的时间。为了插入新的数据做准备。
        /// </summary>
        /// <param name="code"></param>
        /// <param name="currentTime"></param>
        /// <returns></returns>
        private DateTime GetLatestTimeOfYearFromSql(string code, DateTime currentTime)
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
	end
	set @date = dateadd(month,1,@date)
	set @index=@index+1
end
select @latest_date", currentTime.Year, code.ToUpper());
            latest = sqlReaderLocal.ExecuteScriptScalar<DateTime>(sqlStr);
            return latest;
        }

        /// <summary>
        /// 计算当前日期下的最后交易时间
        /// </summary>
        /// <param name="currentTime"></param>
        /// <returns></returns>
        private DateTime GetEndTime(DateTime currentTime)
        {
            if (currentTime.Year < DateTime.Now.Year)
                return dateRepo.GetLastTransactionDate(currentTime, DateLevel.Year).AddHours(15).AddMinutes(0);
            var date = new DateTime(currentTime.Year, DateTime.Now.Month, DateTime.Now.Day - 1, 15, 0, 0);
            date = DateUtils.PreviousTradeDay(date);
            return new DateTime(date.Year, date.Month, date.Day, 15, 0, 0);
        }

        /// <summary>
        /// 根据股票代码逐月删除数据，慎用！！！
        /// </summary>
        /// <param name="code">股票代码</param>
        /// <param name="startDate">开始时间</param>
        /// /// <param name="endDate">结束时间</param>
        private void DeleteDataByCode(string code,DateTime startDate,DateTime endDate)
        {
            string firstDate = startDate.ToString("yyyy-MM-01");
            int firstYear = startDate.Year;
            int firstMonth = startDate.Month;
            int endYear = endDate.Year;
            int endMonth = endDate.Month;
            string sqlStr = string.Format(@"declare @date date,@tb_name nvarchar(60),@yearIndex int,@index int,@latest_date datetime,@sqlStr nvarchar(300),@tem_date datetime
set @date ='{0}'
set @yearIndex={1}
set @index={2}
while @yearIndex<={3} or (@yearIndex={3} and @index<={4})
begin
	if @yearIndex={1}
	begin
		set @index={2}
	end
    set @index =1
	while @index <=12
	begin
		set @tb_name='[StockMinuteTransaction'+datename(year,@date)+'].dbo.[Transaction'+ datename(year,@date)+'-'+datename(month,@date)+']'
		set @sqlStr ='delete from '+@tb_name+' where code=''{5}'''
		if object_id(@tb_name) is not null
		begin
			execute(@sqlStr)
		end
		set @date = dateadd(month,1,@date)
		set @index=@index+1
	end
	set @yearIndex=@yearIndex+1
end",firstDate,firstYear,firstMonth,endYear,endMonth,code.ToUpper()); 
            sqlWriter.WriteChanges(sqlStr);
        }


        /// <summary>
        /// 从sql源中读取数据
        /// </summary>
        /// <param name="code">股票代码</param>
        /// <param name="startDate">开始时间</param>
        /// <param name="endDate">结束时间</param>
        private DataTable getDataFromSql(string code, DateTime startDate, DateTime endDate)
        {
            DataTable dt = new DataTable();
            var sqlStr = string.Format(@"SELECT [stkcd] as [Code]
	  ,DATEADD(mi,-1,convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(rtrim(ttime),3,0,':'))) as [DateTime] 
      ,[Open] as [open]
      ,[High] as [high]
      ,[Low] as [low]
      ,[Close] as [close]
      ,[Volume] as [volume]
      ,[Amount] as [amount]
  FROM [1MinuteLine].[dbo].[Min1_{0}_{1}] where tdate>={2} and tdate<={3} order by [DateTime]",
        code.Split('.')[0], code.Split('.')[1],startDate.ToString("yyyyMMdd"), endDate.ToString("yyyyMMdd"));
            dt = sqlReaderSource.GetDataTable(sqlStr);
            return dt;
        }

        /// <summary>
        /// 将数据逐月存入数据库
        /// </summary>
        /// <param name="dataTable"></param>
        private void WriteToSqlMonthByMonth(DataTable dataTable)
        {
            Dictionary<DateTime, DataTable> monthData = SplitDataTableMonthly(dataTable);
            foreach (var item in monthData)
            {
                IdentifyOrCreateDBAndTable(item.Key);
                sqlWriter.InsertBulk(item.Value, string.Format("[StockMinuteTransaction{0}].dbo.[Transaction{1}]", item.Key.Year, item.Key.ToString("yyyy-MM")));
            }
        }

        private void WriteToSqlByCode(DataTable dataTable,string code)
        {
            IdentifyOrCreateDBAndTableByCode(code);
            string table = string.Format("[StockMinuteTransaction].dbo.[{0}_{1}]", code.ToUpper().Split('.')[0], code.ToUpper().Split('.')[1]);
            sqlWriter.InsertBulk(dataTable,table);
        }

        private Dictionary<int,List<DateTime>> splitDateMonthly(DateTime startDate,DateTime endDate)
        {
            var days = dateRepo.GetStockTransactionDate(startDate, endDate);
            Dictionary<int, List<DateTime>> dateMonthly = new Dictionary<int, List<DateTime>>();
            foreach (var day in days)
            {
                int monthNow = day.Year * 100 + day.Month;
                if (dateMonthly.ContainsKey(monthNow))
                {
                    var list = dateMonthly[monthNow];
                    list.Add(day);
                }
                else
                {
                    var list = new List<DateTime>();
                    list.Add(day);
                    dateMonthly.Add(monthNow, list);
                }
            }
            return dateMonthly;
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
    }
}
