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
    //利用趋势的方法来计算日内交易
    public class trendT0
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string stockBoard;
        private double slipRatio = 0.002;
        private List<DateTime> tradedays;
        private DateTime startDate, endDate;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private Dictionary<string, Dictionary<DateTime, List<StockTransaction>>> allStocks = new Dictionary<string, Dictionary<DateTime, List<StockTransaction>>>();
        public trendT0(StockDailyRepository stockDailyRepo, string stockBoard, DateTime startDate, DateTime endDate)
        {
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
            this.stockBoard = stockBoard;
            this.startDate = startDate;
            this.endDate = endDate;
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
        }

        //获取股票近三年的分钟信息
        private List<stockInfo> getAllStockData(string stockBorad)
        {
            List<stockInfo> info = new List<stockInfo>();
            return info;
        }

        private void searchAllStocks()
        {

        }

        private void rollingByTime()
        {

        }

        private void getBestParameters()
        {

        }


        private bool dualTrust(stockInfo stock,Dictionary<DateTime,parameterPair> parameters,ref List<netvalueDaily> nv,ref List<OneByOneTransaction> transactionData)
        {

            if (allStocks.ContainsKey(stock.code)==false)
            {
                return false;
            }
            var data = allStocks[stock.code];
            double cash = 1;
            double position = 0;
            var myTradeDays=dateRepo.GetStockTransactionDate(stock.startDate, stock.endDate);
            OneByOneTransaction transaction = new OneByOneTransaction();
            //观察期限30分钟
            int duration = 30;
            int minutes = 240;
            foreach (var date in myTradeDays)
            {
                if (data.ContainsKey(date)==false || parameters.ContainsKey(date)==false)
                {
                    return false;
                }
                var stockToday = data[date];
                var para = parameters[date];
                double k1 = para.parameter1;
                double k2 = para.parameter2;
                double trailing = para.parameter3;
                double maxProfit = 0;
                //通过每日前30分钟计算指标
                double HH = 0;//最高价的最高价
                double HC = 0; //收盘价的最高价
                double LC = 99999; //收盘价的最低价
                double LL = 99999; //最低价的最低价
                double Range = 0;
                double longPoint = -1;
                double shortPoint = -1;
                for (int j = 0; j < duration; j++)
                {
                    var dataNow = stockToday[j];
                    if (dataNow.High > HH)
                    {
                        HH = dataNow.High;
                    }
                    if (dataNow.Close > HC)
                    {
                        HC = dataNow.Close;
                    }
                    if (dataNow.Close < LC)
                    {
                        LC = dataNow.Close;
                    }
                    if (dataNow.Low < LL)
                    {
                        LL = dataNow.Low;
                    }
                }
                Range = Math.Max(HH - LC, HC - LL);
                longPoint =stockToday[0].Open + k1 * Range;
                shortPoint = stockToday[0].Open - k2 * Range;
                for (int j = duration; j < minutes; j++)
                {
                    var dataNow = stockToday[j];
                    var dataLast = stockToday[j - 1];
                    if (dataNow.Volume==0)
                    {
                        continue;
                    }
                    double avgPrice = dataNow.Amount / dataNow.Volume;
                    if (position==0 && j<=minutes-10)
                    {
                        //多头信号
                        if (dataNow.Open > longPoint && dataLast.Open < longPoint)
                        {
                            position = 1;
                            cash = cash - avgPrice - avgPrice * slipRatio;
                            transaction = new OneByOneTransaction();
                            transaction.position = 1;
                            transaction.openTime = dataNow.DateTime;
                            transaction.openPrice = avgPrice;
                            maxProfit = 0;

                        }
                        //空头信号
                        else if (dataNow.Open < shortPoint && dataLast.Open > shortPoint)
                        {
                            position = -1;
                            cash = cash + avgPrice - avgPrice * slipRatio;
                            transaction = new OneByOneTransaction();
                            transaction.position = -1;
                            transaction.openTime = dataNow.DateTime;
                            transaction.openPrice = avgPrice;
                            maxProfit = 0;
                        }
                    }
                    else if (position!=0 && j<=minutes-5) //非收盘前5分钟，按追踪止损平仓
                    {
                        if (position == 1)
                        {
                            if ((dataNow.Open / transaction.openPrice - 1) < maxProfit - trailing)
                            {
                                cash = cash + avgPrice - avgPrice * slipRatio;
                                position = 0;
                                transaction.closePrice = avgPrice;
                                transaction.closeTime = dataNow.DateTime;
                                transaction.closeStatus = "追踪止损";
                                transactionData.Add(transaction);
                                transaction = new OneByOneTransaction();
                            }
                        }
                        if (position == -1)
                        {
                            if ((transaction.openPrice / dataNow.Open - 1) < maxProfit - trailing)
                            {
                                cash = cash - avgPrice - avgPrice * slipRatio;
                                position = 0;
                                transaction.closePrice = avgPrice;
                                transaction.closeTime = dataNow.DateTime;
                                transaction.closeStatus = "追踪止损";
                                transactionData.Add(transaction);
                                transaction = new OneByOneTransaction();
                            }
                        }
                    }
                    else if (position!=0 && j==minutes-5)//收盘前5分钟强制平仓
                    {
                        if (position == 1)
                        {
                            cash = cash + avgPrice - avgPrice * slipRatio;
                            position = 0;
                            transaction.closePrice = avgPrice;
                            transaction.closeTime = dataNow.DateTime;
                            transaction.closeStatus = "收盘强平";
                            transactionData.Add(transaction);
                            transaction = new OneByOneTransaction();
                        }
                        if (position == -1 )
                        {
                            cash = cash - avgPrice - avgPrice * slipRatio;
                            position = 0;
                            transaction.closePrice = avgPrice;
                            transaction.closeTime = dataNow.DateTime;
                            transaction.closeStatus = "收盘强平";
                            transactionData.Add(transaction);
                            transaction = new OneByOneTransaction();
                        }
                    }

                    //计算追踪止损的参数
                    if (position == 1)
                    {
                        if ((dataNow.Close / transaction.openPrice - 1) > maxProfit)
                        {
                            maxProfit = (dataNow.Close / transaction.openPrice - 1);
                        }
                    }
                    if (position == -1)
                    {
                        if ((transaction.openPrice / dataNow.Close - 1) > maxProfit)
                        {
                            maxProfit = (transaction.openPrice / dataNow.Close - 1);
                        }
                    }
                    if (j==minutes-1)
                    {
                        dataNow = stockToday[j];
                        netvalueDaily nvToday = new netvalueDaily();
                        nvToday.date = dataNow.DateTime.Date;
                        nvToday.netvalue = cash + position * dataNow.Close;
                        nv.Add(nvToday);
                    }
                }   
                
            }
            return true;
        }
        
    }
}
