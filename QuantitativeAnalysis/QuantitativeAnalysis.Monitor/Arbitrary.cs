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

        public Arbitrary(OptionInfoRepository infoRepo, StockOptionTickRepository optionRepo,StockTickRepository stockRepo,double rate = 0.04)
        {
            this.infoRepo = infoRepo;
            this.optionRepo = optionRepo;
            this.stockRepo = stockRepo;
            this.rate = rate;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Default);
        }

        public void record(DateTime startDate, DateTime endDate)
        {

            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
           
            foreach (var date in tradedays)
            {
                DataTable recordList = new DataTable();
                recordList.Columns.Add("time");
                recordList.Columns.Add("expiredate");
                recordList.Columns.Add("annualizedReturn");
                recordList.Columns.Add("annualizedCloseCost");
                recordList.Columns.Add("etfPrice");
                recordList.Columns.Add("strike");
                myList = new List<StockOptionParityProfit>();
                var list = infoRepo.GetStockOptionInfo(underlying, date, date);
                parityList = new List<StockOptionParity>();
                foreach (var item in list)
                {
                    if (item.type == "认购" && item.expireDate <= date.AddDays(40) && item.listedDate <= date)
                    {
                        var parity = StockOptionExtension.GetParity(list, item);
                        Console.WriteLine("Date {0}: strike {1} expireDate {2} option1 {3} option2 {4}", date, item.strike, item.expireDate, item.name, parity.name);
                        var pair = new StockOptionParity { call = item.code, put = parity.code, strike = item.strike, expireDate = item.expireDate };
                        parityList.Add(pair);
                    }
                }
                compute(date,recordList);

            }
        }

        private void WriteToSqlServer(DateTime date, DataTable dt)
        {
            var sql = string.Format(@"delete * from [PutCallParity].dbo.[{0}] ", date.ToString("yyyy-MM-dd"));
            sqlWriter.WriteChanges(sql);
            var str = string.Format(@"[PutCallParity].dbo.[{0}]", date.ToString("yyyy-MM-dd"));
            sqlWriter.InsertBulk(dt, str);
        }

        private void compute(DateTime date,DataTable dt)
        {
            List<StockTickTransaction> etf = new List<StockTickTransaction>();
            etf =DataTimeStampExtension.ModifyStockTickData(stockRepo.GetStockTransaction("510050.SH", date, date.AddHours(17)));
            foreach (var item in parityList)
            {
                List<StockOptionTickTransaction> call = new List<StockOptionTickTransaction>();
                List<StockOptionTickTransaction> put = new List<StockOptionTickTransaction>();
                call=DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.call, date, date.AddHours(17)));
                put=DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.put, date, date.AddHours(17)));
                //计算套利空间
                TimeSpan span = item.expireDate - date;
                for (int i = 0; i < 28802; i++)
                {
                    StockOptionParityProfit result=new StockOptionParityProfit();
                    if (etf[i]!=null && call[i]!=null && put[i]!=null && etf[i].LastPrice!=0 && call[i].LastPrice!=0 && put[i].LastPrice!=0)
                    {
                        result.date = etf[i].TransactionDateTime;
                        result.strike = item.strike;
                        result.etfPrice = etf[i].LastPrice;
                        result.expiredate = span.Days + 1;
                        double profit = result.strike - (etf[i].Ask1 - call[i].Bid1 + put[i].Ask1);
                        double margin = (etf[i].Ask1 - call[i].Bid1 + put[i].Ask1) + (call[i].LastPrice + Math.Max(0.12 * etf[i].LastPrice - Math.Max(result.strike - etf[i].LastPrice, 0), 0.07 * etf[i].LastPrice));
                        double annualizedReturn = (profit - etf[i].Ask1 * 0.0001 - 1.6 / 10000.0) / margin / (double)result.expiredate * 365.0;
                        double annualizedCloseCost = (-result.strike + (etf[i].Bid1 - call[i].Ask1 + put[i].Bid1) - etf[i].Bid1 * 0.0001 - 3.2 / 10000.0) / margin / (double)result.expiredate * 365.0;
                        result.profit = annualizedReturn;
                        result.cost = annualizedCloseCost;
                        myList.Add(result);
                        DataRow dr = dt.NewRow();
                        dr["time"] = result.date;
                        dr["strike"] = result.strike;
                        dr["annualizedReturn"] = result.profit;
                        dr["annualizedCloseCost"] = result.cost;
                        dr["expiredate"] = result.expiredate;
                        dr["etfPrice"] = result.etfPrice;
                        dt.Rows.Add(dr);
                    }
                }
            }

        }

    }
}

