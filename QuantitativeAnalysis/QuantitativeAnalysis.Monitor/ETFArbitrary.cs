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

namespace QuantitativeAnalysis.Monitor
{
    public class ETFArbitrary
    {
        private double rate;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockTickRepository stockRepo;
        private StockDailyRepository stockDailyRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        

        public ETFArbitrary(StockTickRepository stockRepo,StockDailyRepository stockDailyRepo,string code)
        {
            this.stockRepo = stockRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
            this.code = code;
        }

        public void compute(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<ETFConsitituent> etfInfo = new List<ETFConsitituent>();
            List<double> amountList = getAmount(code, startDate, endDate);
            double[] arbitraryPurchase = new double[28802];
            double[] arbitraryRedeem = new double[28802];
            bool[] isNan = new bool[28802];
            for (int i = 0; i < 28802; i++)
            {
                isNan[i] = true;   
            }
            for (int k = 0; k < tradedays.Count(); k++)
            {
                DateTime date = tradedays[k];
                etfInfo = getETFInfo(code, date);
                foreach (var item in etfInfo)
                {
                    if (item.cash_substitution_mark=="必须")
                    {
                        for (int i = 0; i < 28802; i++)
                        {
                            if (isNan[i]==true)
                            {
                                arbitraryPurchase[i] += -item.substitution_amout;
                            }
                        }
                    }
                    else
                    {
                        var stockData = stockRepo.GetStockTransaction(item.code, date, date.AddHours(17));
                        if (stockData!=null && stockData.Count>0)
                        {
                            var stock = DataTimeStampExtension.ModifyStockTickData(stockData);
                            for (int i = 0; i < stock.Count(); i++)
                            {
                                if (isNan[i] == true && stock[i] != null && stock[i].AskV1 != 0 && stock[i].BidV1 != 0)
                                {
                                    arbitraryPurchase[i] += -item.volume * stock[i].Ask1;
                                    //arbitraryRedeem[i] += item.volume * stock[i].Bid1;
                                }
                                if (stock[i] == null)
                                {
                                    isNan[i] = false;
                                    arbitraryPurchase[i] = 0;
                                    //arbitraryRedeem[i] = 0;
                                }
                            }
                        }
                        else
                        {
                            if (item.cash_substitution_mark=="禁止")
                            {
                                for (int i = 0; i < 28802; i++)
                                {
                                    arbitraryPurchase[i] = 0;
                                    isNan[i] = false;
                                }
                            }
                            else
                            {
                                var stock = stockDailyRepo.GetStockTransactionWithRedis(item.code, date, date);
                                for (int i = 0; i < 28802; i++)
                                {
                                    if (isNan[i] == true)
                                    {
                                        arbitraryPurchase[i] += -item.volume * stock[stock.Count() - 1].Close * (1 + item.premium_ratio / 100.0);
                                    }
                                }
                            }
                        }
                        
                    }
                    
                }
                var etf = DataTimeStampExtension.ModifyStockTickData(stockRepo.GetStockTransaction(code, date, date.AddHours(17)));
                for (int i = 0; i < etf.Count(); i++)
                {
                    if (isNan[i]==true && etf[i] != null && etf[i].AskV1 != 0 && etf[i].BidV1 != 0)
                    {
                        arbitraryPurchase[i] += amountList[k] * etf[i].Bid1;
                       // arbitraryRedeem[i] += -amountList[k] * etf[i].Ask1;
                    }
                    if (etf[i] == null)
                    {
                        isNan[i] = false;
                        arbitraryPurchase[i] = 0;
                        //arbitraryRedeem[i] = 0;
                    }
                }
                Console.WriteLine("today {0} change {1}", date, arbitraryPurchase.Max());
           }
        }

        private List<double> getAmount(string code,DateTime start,DateTime end)
        {
            var wData= WindClientSingleton.Instance.wsd(code, "fund_etfpr_minnav", start, end, "");
            var data = ((double[])wData.data).ToList();
            return data;
        }

        private List<ETFConsitituent> getETFInfo(string code,DateTime date)
        {
            List<ETFConsitituent> info = new List<ETFConsitituent>();
            var wData = WindClientSingleton.Instance.wset("etfconstituent", string.Format(@"date={0};windcode={1}", date.ToString("yyyy-MM-dd"), code));
            int length = wData.codeList.Length;
            int number = wData.fieldList.Length;
            var data = (object[])wData.data;
            for (int i = 0; i < length; i++)
            {
                ETFConsitituent item = new ETFConsitituent();
                item.code =Convert.ToString(data[i * number + 1]);
                item.stockName = Convert.ToString(data[i * number + 2]);
                item.volume = Convert.ToDouble(data[i * number + 3]);
                item.cash_substitution_mark = Convert.ToString(data[i * number + 4]);
                item.premium_ratio = Convert.ToDouble(data[i * number + 5]);
                item.substitution_amout = Convert.ToDouble(data[i * number + 6]);
                info.Add(item);
            }
            return info;
        }

    }
}
