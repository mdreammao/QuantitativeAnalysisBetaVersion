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
    public class DualTrust
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private string underlyingCode;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData;
        private StockMinuteRepository stockMinutelyRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private int minutes = 240;
        private Dictionary<DateTime, List<StockTransaction>> underlying = new Dictionary<DateTime, List<StockTransaction>>();
        private List<StockTransaction> underlyingAll = new List<StockTransaction>();
        private double slipRatio = 0.0002;
        private double multiplicator = 1;

        public DualTrust(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, string code, string underlyingCode)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            this.code = code;
            this.underlyingCode = underlyingCode;
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
            if (code=="IF.CFE")
            {
                multiplicator = 300;
            }
        }

        public void compute(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<StockTransaction> codeDaily = new List<StockTransaction>();
            List<StockTransaction> underlyingCodeDaily = new List<StockTransaction>();
            //获取分钟线数据
            foreach (var date in tradedays)
            {
                var underlyingToday = stockMinutelyRepo.GetStockTransaction(underlyingCode, date, date);
                underlyingCodeDaily.AddRange(underlyingToday);
                var codeToday = stockMinutelyRepo.GetStockTransaction(code, date, date);
                codeDaily.AddRange(codeToday);
            }
            double k1 = 0, k2 = 0, trailParameter = 0;
            //getParameter(tradedays, underlyingCodeDaily, codeDaily, ref k1, ref k2, ref trailParameter);
            k1 = 1.6;k2 = 1.2;trailParameter = 0.011;
            double sharpe = computeChoiceParameter(tradedays, underlyingCodeDaily, codeDaily, k1, k2, trailParameter);
        }


        private void getParameter(List<DateTime> tradedays, List<StockTransaction> underlyingDaily1, List<StockTransaction> underlyingDaily2,ref double k1Max,ref double k2Max,ref double trailingParameterMax)
        {
            double max = -10;
            for (double  k1 = 0.2; k1 <= 3; k1=k1+0.2)
            {
                for (double k2 = 0.2; k2 <= 3; k2 = k2 + 0.2)
                {
                    for (double trailingParameter = 0.001; trailingParameter <= 0.1; trailingParameter = trailingParameter + 0.001)
                    {
                        double sharpe = computeChoiceParameter(tradedays, underlyingDaily1, underlyingDaily2, k1, k2,trailingParameter);
                        if (sharpe > max)
                        {
                            max = sharpe;
                            k1Max = k1;
                            k2Max = k2;
                            trailingParameterMax = trailingParameter;
                            Console.WriteLine("sharpe:{0}, k1:{1}, k2:{2}, trailing:{3}", max, k1Max, k2Max, trailingParameterMax);
                        }
                    }
                }
            }
        }

        private double computeChoiceParameter(List<DateTime> tradedays, List<StockTransaction> underlyingDaily1, List<StockTransaction> underlyingDaily2, double k1, double k2, double trailingParameter)
        {
            double sharpe = 0;
            List<OneByOneTransaction> data = new List<OneByOneTransaction>();
            List<netvalueDaily> netvalueList = new List<netvalueDaily>();
            //观察期限30分钟
            int duration = 30;
            int lengthOfDays = tradedays.Count();
            double[] signal = new double[lengthOfDays*minutes];
            double[] range = new double[lengthOfDays];
            for (int i = 0; i < lengthOfDays; i++)
            {
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
                    int index = i * minutes + j;
                    var dataNow = underlyingDaily1[index];
                    if (dataNow.High>HH)
                    {
                        HH = dataNow.High;
                    }
                    if (dataNow.Close>HC)
                    {
                        HC = dataNow.Close;
                    }
                    if (dataNow.Close<LC)
                    {
                        LC = dataNow.Close;
                    }
                    if (dataNow.Low<LL)
                    {
                        LL = dataNow.Low;
                    }
                }
                Range = Math.Max(HH - LC, HC - LL);
                range[i] = Range;
                longPoint = underlyingDaily2[i * minutes + duration].Open + k1 * Range;
                shortPoint= underlyingDaily2[i * minutes + duration].Open - k2 * Range;
                for (int j = duration; j < minutes; j++)
                {
                    var dataNow = underlyingDaily2[i * minutes + j];
                    var dataLast = underlyingDaily2[i * minutes + j - 1];
                    if (dataNow.Open>longPoint && dataLast.Open<longPoint)
                    {
                        signal[i * minutes + j] = 1;
                    }
                    else if (dataNow.Open<shortPoint && dataLast.Open>shortPoint)
                    {
                        signal[i * minutes + j] = -1;
                    }
                }
            }
            ComputeDualTrust(tradedays, underlyingDaily2, signal, range,trailingParameter, ref data, ref netvalueList);
            sharpe = Utilities.strategyPerformance.sharpeRatioByDailyNetValue(netvalueList.Select(s => s.netvalue).ToList());
            return sharpe;
        }


        private bool ComputeDualTrust(List<DateTime> tradedays,List<StockTransaction> underlying, double[] signal,  double[] range,double trailingParameter, ref List<OneByOneTransaction> data, ref List<netvalueDaily> netvalueList)
        {
            int lengthOfDays = tradedays.Count();
            double originalCash = 10000;
            double cash = originalCash;
            double position = 0;
            double maxProfit = 0;
            bool tradable = false;
            OneByOneTransaction transaction = new OneByOneTransaction();
            for (int i = 0; i < lengthOfDays; i++)
            {
                if (range[i]<10)
                {
                    continue;
                }
                int j = 0;
                int index = 0;
                StockTransaction dataNow = new StockTransaction();
                double avgPrice = 0;
                for (j = 0; j < minutes-5; j++)
                {
                    index = i * minutes + j;
                    dataNow = underlying[index];
                    if (dataNow.Volume > 0)
                    {
                        tradable = true;
                    }
                    else
                    {
                        tradable = false;
                        continue;
                    }
                    avgPrice = dataNow.Amount / dataNow.Volume/ multiplicator;
                    if (position==0) //开仓
                    {
                        if (signal[index]==1) //开多头
                        {
                            position = 1;
                            cash = cash - avgPrice - avgPrice * slipRatio;
                            transaction = new OneByOneTransaction();
                            transaction.position = 1;
                            transaction.openTime = dataNow.DateTime;
                            transaction.openPrice = avgPrice;
                            maxProfit = 0;
                        }
                        if (signal[index]==-1) //开空头
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
                    else
                    {
                        if (position==1)
                        {
                            if ((dataNow.Open / transaction.openPrice - 1)<maxProfit- trailingParameter)
                            {
                                cash = cash + avgPrice - avgPrice * slipRatio;
                                position = 0;
                                transaction.closePrice = avgPrice;
                                transaction.closeTime = dataNow.DateTime;
                                transaction.closeStatus = "追踪止损";
                                data.Add(transaction);
                                transaction = new OneByOneTransaction();
                            }
                        }
                        if (position==-1)
                        {
                            if ((transaction.openPrice / dataNow.Open - 1)<maxProfit- trailingParameter)
                            {
                                cash = cash - avgPrice - avgPrice * slipRatio;
                                position = 0;
                                transaction.closePrice = avgPrice;
                                transaction.closeTime = dataNow.DateTime;
                                transaction.closeStatus = "追踪止损";
                                data.Add(transaction);
                                transaction = new OneByOneTransaction();
                            }
                        }
                    }
                    if (position == 1)
                    {
                        if ((dataNow.Close/transaction.openPrice-1)>maxProfit)
                        {
                            maxProfit = (dataNow.Close / transaction.openPrice - 1);
                        }
                    }
                    if (position == -1)
                    {
                        if ((transaction.openPrice/dataNow.Close - 1) > maxProfit)
                        {
                            maxProfit = (transaction.openPrice / dataNow.Close - 1);
                        }
                    }

                }

                //日内最后3分钟平仓
                j = minutes - 3;
                index = i * minutes + j;
                dataNow = underlying[index];
                if (dataNow.Volume > 0)
                {
                    tradable = true;
                }
                else
                {
                    tradable = false;
                }
                avgPrice = dataNow.Amount / dataNow.Volume/multiplicator;
                if (position==1 && tradable)
                {
                    cash = cash + avgPrice - avgPrice * slipRatio;
                    position = 0;
                    transaction.closePrice = avgPrice;
                    transaction.closeTime = dataNow.DateTime;
                    transaction.closeStatus = "收盘强平";
                    data.Add(transaction);
                    transaction = new OneByOneTransaction();
                }
                if (position==-1 && tradable)
                {
                    cash = cash - avgPrice - avgPrice * slipRatio;
                    position = 0;
                    transaction.closePrice = avgPrice;
                    transaction.closeTime = dataNow.DateTime;
                    transaction.closeStatus = "收盘强平";
                    data.Add(transaction);
                    transaction = new OneByOneTransaction();
                }

                //计算每日收盘时候的净值
                index = i * minutes + minutes-1;
                dataNow = underlying[index];
                netvalueDaily nvToday = new netvalueDaily();
                nvToday.date = dataNow.DateTime.Date;
                nvToday.netvalue = cash+position*dataNow.Close;
                netvalueList.Add(nvToday);
            }
            return true;
        }
    }
}
