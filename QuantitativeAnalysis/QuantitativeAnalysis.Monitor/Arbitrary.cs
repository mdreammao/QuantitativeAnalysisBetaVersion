﻿using System;
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

namespace QuantitativeAnalysis.Monitor
{
    public class Arbitrary
    {
        private double rate;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string underlying = "510050.SH";
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private List<StockOptionParity> parityList;
        private OptionInfoRepository infoRepo;
        private StockOptionTickRepository optionRepo;
        private StockTickRepository stockRepo;
        private List<StockOptionParityProfit> myList;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;

        public Arbitrary(OptionInfoRepository infoRepo, StockOptionTickRepository optionRepo,StockTickRepository stockRepo,double rate = 0.04)
        {
            this.infoRepo = infoRepo;
            this.optionRepo = optionRepo;
            this.stockRepo = stockRepo;
            this.rate = rate;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
        }


        public void recorddata(DateTime startDate,DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {
                stockRepo.GetStockTransaction("510050.SH", date, date.AddHours(17));
                var list = infoRepo.GetStockOptionInfo(underlying, date, date);
                list = OptionUtilities.modifyOptionListByETFBonus(list, date);
                foreach (var item in list)
                {
                    optionRepo.GetStockTransaction(item.code, date, date.AddHours(17));
                }
            }
        }

        public void record(DateTime startDate, DateTime endDate)
        {

            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            CreateDBOrTableIfNecessary(startDate);
            CreateDBOrTableIfNecessary(startDate.AddYears(1));
            var start = startDate;
            while (start < endDate)
            {
                if (!ExistInSqlServer(start))
                {
                    CreateDBOrTableIfNecessary(start);
                }
                start = start.AddYears(1);
            }
            if (!ExistInSqlServer(endDate))
            {
                CreateDBOrTableIfNecessary(endDate);
            }

            foreach (var date in tradedays)
            {
               
                var list = infoRepo.GetStockOptionInfo(underlying, date, date);
                list = OptionUtilities.modifyOptionListByETFBonus(list, date);
                parityList = new List<StockOptionParity>();
                foreach (var item in list)
                {
                    if (item.type == "认购" &&  item.expireDate <= date.AddDays(180) && item.listedDate <= date)
                    {
                        var parity = StockOptionExtension.GetParity(list, item);
                        Console.WriteLine("Date {0}: strike {1} expireDate {2} option1 {3} option2 {4}", date, item.strike, item.expireDate, item.name, parity.name);
                        var pair = new StockOptionParity { call = item.code, put = parity.code, strike = item.strike, expireDate = item.expireDate,unit=item.unit};
                        parityList.Add(pair);
                    }
                }
                compute(date);
            }
        }

        

        private void compute(DateTime date)
        {
            List<StockTickTransaction> etf = new List<StockTickTransaction>();
            etf =DataTimeStampExtension.ModifyStockTickData(stockRepo.GetStockTransaction("510050.SH", date, date.AddHours(17)));
            foreach (var item in parityList)
            {
                DataTable dt = new DataTable();
                dt.Columns.Add("tdatetime");
                dt.Columns.Add("expiredate");
                dt.Columns.Add("maturitydate");
                dt.Columns.Add("annualizedReturn");
                dt.Columns.Add("annualizedCloseCost");
                dt.Columns.Add("etfPrice");
                dt.Columns.Add("strike");
                dt.Columns.Add("callPrice");
                dt.Columns.Add("putPrice");
                dt.Columns.Add("callMinutelyPrice");
                dt.Columns.Add("putMinutelyPrice");
                dt.Columns.Add("minutelyVolume");
                double strike=item.strike;
                int expiredate=0;
                List<StockOptionTickTransaction> call = new List<StockOptionTickTransaction>();
                List<StockOptionTickTransaction> put = new List<StockOptionTickTransaction>();
                call=DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.call, date, date.AddHours(17)));
                put=DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.put, date, date.AddHours(17)));
                //计算套利空间
                myList = new List<StockOptionParityProfit>();
                TimeSpan span = item.expireDate - date;
               // var multiple = item.unit/10000.0;
                for (int i = 0; i < 28802; i++)
                {
                    StockOptionParityProfit result=new StockOptionParityProfit();
                    double callMinutelyVolume = 0;
                    double putMinutelyVolume = 0;
                    double callPrice = 0;
                    double putPrice = 0;
                    if (etf[i]!=null && call!=null&& put!=null &&call[i]!=null && put[i]!=null && etf[i].LastPrice!=0 && call[i].LastPrice!=0 && put[i].LastPrice!=0 && call[i].Ask1!=0 && put[i].Bid1!=0 && call[i].Bid1!=0 && put[i].Ask1!=0)
                    {
                        result.date = etf[i].TransactionDateTime;
                        result.strike = item.strike;
                        result.etfPrice = etf[i].LastPrice;
                        result.expiredate = span.Days + 1;
                        expiredate = result.expiredate;
                        result.maturitydate = item.expireDate;
                        double profit = result.strike - (etf[i].Ask1  - call[i].Bid1 + put[i].Ask1);
                        double margin = (etf[i].Ask1  - call[i].Bid1 + put[i].Ask1) + (call[i].LastPrice + Math.Max(0.12 * etf[i].LastPrice  - Math.Max(result.strike - etf[i].LastPrice , 0), 0.07 * etf[i].LastPrice ));
                        double annualizedReturn = (profit - etf[i].Ask1 * 0.0001 - 1.6 / 10000.0) / margin / (double)result.expiredate * 365.0;
                        double annualizedCloseCost = (-result.strike + (etf[i].Bid1  - call[i].Ask1 + put[i].Bid1) - etf[i].Bid1 * 0.0001 - 3.2 / 10000.0) / margin / (double)result.expiredate * 365.0;
                        result.profit = annualizedReturn;
                        result.cost = annualizedCloseCost;
                        result.callPrice = call[i].LastPrice;
                        result.putPrice = put[i].LastPrice;
                        if (i>120 && call[i-120]!=null)
                        {
                            callMinutelyVolume = call[i].Volume - call[i - 120].Volume;
                            if (callMinutelyVolume!=0)
                            {
                                callPrice = (call[i].Amount - call[i - 120].Amount) / callMinutelyVolume / item.unit;
                            }
                            
                        }
                        else
                        {
                            callMinutelyVolume = Math.Round(call[i].Volume / Convert.ToDouble(i+1) * 120.0,0);
                            if (callMinutelyVolume!=0)
                            {
                                callPrice = call[i].Amount / call[i].Volume / item.unit;
                            }
                        }
                        if (i > 120 && put[i - 120] != null)
                        {
                            putMinutelyVolume = put[i].Volume - put[i - 120].Volume;
                            if (putMinutelyVolume!=0)
                            {
                                putPrice = (put[i].Amount - put[i - 120].Amount) / putMinutelyVolume / item.unit;
                            }
                        }
                        else
                        {
                            putMinutelyVolume = Math.Round(put[i].Volume / Convert.ToDouble(i+1) * 120.0, 0);
                            if (putMinutelyVolume!=0)
                            {
                                putPrice = put[i].Amount / put[i].Volume / item.unit;
                            }
                        }
                        myList.Add(result);
                        DataRow dr = dt.NewRow();
                        dr["tdatetime"] = result.date;
                        dr["maturitydate"] = result.maturitydate;
                        dr["strike"] = Math.Round(result.strike,4);
                        dr["annualizedReturn"] =Math.Round(result.profit,4);
                        dr["annualizedCloseCost"] =Math.Round(result.cost,4);
                        dr["expiredate"] = result.expiredate;
                        dr["etfPrice"] = result.etfPrice;
                        dr["callPrice"] = result.callPrice;
                        dr["putPrice"] = result.putPrice;
                        dr["callMinutelyPrice"] = Math.Round(Convert.ToDecimal(callPrice),6);
                        dr["putMinutelyPrice"] = Math.Round(Convert.ToDecimal(putPrice),6);
                        dr["minutelyVolume"] = Math.Min(Convert.ToDecimal(callMinutelyVolume),Convert.ToDecimal(putMinutelyVolume));
                        if (result.date<result.date.Date+new TimeSpan(14,57,00))
                        {
                            dt.Rows.Add(dr);
                        }
                    }
                }
                SaveResultToMssql(date, dt,strike,expiredate);
                //SaveResultToMssql(date, dt,expiredate);
            }

        }

        private void SaveResultToMssql(DateTime date, DataTable dt,double strike,int expiredate)
        {
            var sql = string.Format(@"delete from [PutCallParity{0}].[dbo].[{1}] where tdatetime>'{2}' and tdatetime<'{3}' and strike='{4}' and expiredate='{5}'", date.Year, date.ToString("yyyy"),date.ToString("yyyy-MM-dd"),date.AddDays(1).ToString("yyyy-MM-dd"),strike,expiredate);
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[PutCallParity{0}].[dbo].[{1}]", date.Year, date.ToString("yyyy")));
        }

        private void SaveResultToMssql(DateTime date, DataTable dt, int expiredate)
        {
            var sql = string.Format(@"delete from [PutCallParity{0}].[dbo].[{1}] where tdatetime>'{2}' and tdatetime<'{3}' and expiredate='{4}'", date.Year, date.ToString("yyyy"), date.ToString("yyyy-MM-dd"), date.AddDays(1).ToString("yyyy-MM-dd"), expiredate);
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[PutCallParity{0}].[dbo].[{1}]", date.Year, date.ToString("yyyy")));
        }

        private void CreateDBOrTableIfNecessary(DateTime date)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('PutCallParity{0}') is null
begin
CREATE DATABASE [PutCallParity{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'PutCallParity{0}', FILENAME = N'{2}\PutCallParity{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'PutCallParity{0}_log', FILENAME = N'{2}\PutCallParity{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [PutCallParity{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [PutCallParity{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[PutCallParity{0}].dbo.[{1}]') is null
begin
CREATE TABLE [PutCallParity{0}].[dbo].[{1}](
	[tdatetime] [datetime] NOT NULL,
    [strike] [decimal](8, 4) NULL,
	[expiredate] [decimal](10, 0) NULL,
    [maturitydate] [datetime] NOT NULL,
	[annualizedReturn] [decimal](12, 4) NULL,
	[annualizedCloseCost] [decimal](12, 4) NULL,
	[etfPrice] [decimal](12, 4) NULL,
	[callPrice] [decimal](12, 4) NULL,
	[putPrice] [decimal](12, 4) NULL,
    [callMinutelyPrice] [decimal](12, 6) NULL,
	[putMinutelyPrice] [decimal](12, 6) NULL,
    [minutelyVolume] [decimal](12, 2) NULL,
	[LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [PutCallParity{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}] ON [PutCallParity{0}].[dbo].[{1}]
(
	[strike] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [PutCallParity{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", date.Year, date.ToString("yyyy"), fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(DateTime date)
        {
            var sqlScript = string.Format(@"use master
if db_id('PutCallParity{0}') is not null
begin
	if object_id('[PutCallParity{0}].dbo.[{1}]') is not null
	begin
		select 1 from [PutCallParity{0}].dbo.[{1}] 
	end
end
else
begin
select 0
end ", date.Year, date.ToString("yyyy"));
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }

    }
}

