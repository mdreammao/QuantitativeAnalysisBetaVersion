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
    public class ivix
    {
        private double rate;
        private double cashVega = 10000;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string underlying = "510050.SH";
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private OptionInfoRepository infoRepo;
        private StockOptionTickRepository optionRepo;
        private StockTickRepository stockRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;

        public ivix(OptionInfoRepository infoRepo, StockOptionTickRepository optionRepo, StockTickRepository stockRepo, double rate = 0.04)
        {
            this.infoRepo = infoRepo;
            this.optionRepo = optionRepo;
            this.stockRepo = stockRepo;
            this.rate = rate;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
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
                DataTable dt = new DataTable();
                dt = initializeDataTable(dt);
                double[] sigma1Ask = new double[28802];
                double[] sigma1Bid = new double[28802];
                double[] sigma2Ask = new double[28802];
                double[] sigma2Bid = new double[28802];
                double[] vixAsk = new double[28802];
                double[] vixBid = new double[28802];
                
                var list = infoRepo.GetStockOptionInfo(underlying, date, date);
                list = OptionUtilities.modifyOptionListByETFBonus(list,date);
                List<StockOptionInformation> callListThisMonth = new List<StockOptionInformation>();
                List<StockOptionInformation> callListNextMonth = new List<StockOptionInformation>();
                List<StockOptionInformation> putListThisMonth = new List<StockOptionInformation>();
                List<StockOptionInformation> putListNextMonth = new List<StockOptionInformation>();
                var durationList=OptionUtilities.getDurationStructure(list, date);
                double durationThisMonth = 0;
                double durationNextMonth = 0;
                if (durationList[0]>7)
                {
                    durationThisMonth = durationList[0];
                    durationNextMonth = durationList[1];
                }
                else
                {
                    durationThisMonth = durationList[1];
                    durationNextMonth = durationList[2];
                }
                foreach (var item in list)
                {
                    if (OptionUtilities.getDuration(item,date)==durationThisMonth && item.unit==10000)
                    {
                        if (item.type == "认购")
                        {
                            callListThisMonth.Add(item);
                        }
                        else
                        {
                            putListThisMonth.Add(item);
                        }
                    }
                    else if (OptionUtilities.getDuration(item, date) == durationNextMonth && item.unit == 10000)
                    {
                        if (item.type == "认购")
                        {
                            callListNextMonth.Add(item);
                        }
                        else
                        {
                            putListNextMonth.Add(item);
                        }
                    }
                }
                callListThisMonth=callListThisMonth.OrderBy(x => x.strike).ToList();
                callListNextMonth=callListNextMonth.OrderBy(x => x.strike).ToList();
                putListThisMonth=putListThisMonth.OrderBy(x => x.strike).ToList();
                putListNextMonth=putListNextMonth.OrderBy(x => x.strike).ToList();
                //获取当日ETF及期权数据
                List<StockTickTransaction> etf = new List<StockTickTransaction>();
                etf = DataTimeStampExtension.ModifyStockTickData(stockRepo.GetStockTransaction("510050.SH", date, date.AddHours(17)));
                Dictionary<double, List<StockOptionTickTransaction>> callDataThisMonth = new Dictionary<double, List<StockOptionTickTransaction>>();
                Dictionary<double, List<StockOptionTickTransaction>> putDataThisMonth = new Dictionary<double, List<StockOptionTickTransaction>>();
                Dictionary<double, List<StockOptionTickTransaction>> callDataNextMonth = new Dictionary<double, List<StockOptionTickTransaction>>();
                Dictionary<double, List<StockOptionTickTransaction>> putDataNextMonth = new Dictionary<double, List<StockOptionTickTransaction>>();
                List<double> strikeListThisMonth = new List<double>();
                List<double> strikeListNextMonth = new List<double>();
                foreach (var item in callListThisMonth)
                {
                    strikeListThisMonth.Add(item.strike);
                    var call = DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.code, date, date.AddHours(17)));
                    callDataThisMonth.Add(item.strike, call);
                }
                foreach (var item in putListThisMonth)
                {
                    var put= DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.code, date, date.AddHours(17)));
                    putDataThisMonth.Add(item.strike, put);
                }
                foreach (var item in callListNextMonth)
                {
                    strikeListNextMonth.Add(item.strike);
                    var call = DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.code, date, date.AddHours(17)));
                    callDataNextMonth.Add(item.strike, call);
                }
                foreach (var item in putListNextMonth)
                {
                    //2016-2-17数据有缺失
                    var put = DataTimeStampExtension.ModifyOptionTickData(optionRepo.GetStockTransaction(item.code, date, date.AddHours(17)));
                    putDataNextMonth.Add(item.strike, put);
                }
                strikeListThisMonth=strikeListThisMonth.OrderBy(x => x).ToList();
                strikeListNextMonth=strikeListNextMonth.OrderBy(x => x).ToList();
                for (int index = 0; index < 28802; index++)
                {

                    bool hasData = true;
                    foreach (var item in strikeListThisMonth)
                    {
                        if (callDataThisMonth[item]==null || putDataThisMonth[item]==null || callDataThisMonth[item][index]==null || putDataThisMonth[item][index]==null || (callDataThisMonth[item][index].AskV1 == 0 && callDataThisMonth[item][index].BidV1 == 0)|| (putDataThisMonth[item][index].AskV1 == 0 && putDataThisMonth[item][index].BidV1 == 0))
                        {
                            hasData = false;
                            break;
                        }
                    }
                    //if (durationThisMonth <= 30)
                    {
                        foreach (var item in strikeListNextMonth)
                        {
                            if (callDataNextMonth[item]==null || putDataNextMonth[item]==null || callDataNextMonth[item][index] == null || putDataNextMonth[item][index] == null || callDataNextMonth[item][index].AskV1 == 0 || putDataNextMonth[item][index].AskV1 == 0 || callDataNextMonth[item][index].BidV1 == 0 || putDataNextMonth[item][index].BidV1 == 0)
                            {
                                hasData = false;
                                break;
                            }
                        }
                    }
                    if (hasData==false)
                    {
                        continue;
                    }
                    //初始化记录合约信息的列表
                    List<iVixInfo> thisMonthInfo = new List<iVixInfo>();
                    List<iVixInfo> nextMonthInfo = new List<iVixInfo>();
                    DataRow dr = dt.NewRow();
                    var now = callDataThisMonth[strikeListThisMonth[0]][index].TransactionDateTime;
                    var expiredate1 = callListThisMonth[0].expireDate;
                    var expiredate2 = callListNextMonth[0].expireDate;
                    var span = date.AddHours(15)-now;
                    //计算时间T NT：近月合约剩余到期时间（以分钟计） T：NT/365
                    double T1 =( durationThisMonth - 1 + (span.Hours*60+span.Minutes) / 840.0)/365.0;
                    //找到认购期权价格与认沽期权价格相差最小的执行价的K
                    //计算远期价格F S+exp(RT)×[认购期权价格 S −认沽期权价格 S ]
                    double distance1 = 100;
                    double kThisMonth = 0;
                    double F = 0;
                    for (int i = 0; i < strikeListThisMonth.Count(); i++)
                    {
                        double distance0 =Math.Abs((callDataThisMonth[strikeListThisMonth[i]][index].Ask1 + callDataThisMonth[strikeListThisMonth[i]][index].Bid1) / 2 - (putDataThisMonth[strikeListThisMonth[i]][index].Ask1 + putDataThisMonth[strikeListThisMonth[i]][index].Bid1) / 2);
                        if (distance0<distance1)
                        {
                            distance1 = distance0;
                            F = strikeListThisMonth[i] + Math.Exp(rate * T1) * ((callDataThisMonth[strikeListThisMonth[i]][index].Ask1 + callDataThisMonth[strikeListThisMonth[i]][index].Bid1) / 2 - (putDataThisMonth[strikeListThisMonth[i]][index].Ask1 + putDataThisMonth[strikeListThisMonth[i]][index].Bid1) / 2);
                        }
                    }
                    //找到K0
                    for (int i = 0; i < strikeListThisMonth.Count()-1; i++)
                    {
                        kThisMonth = strikeListThisMonth[i];
                        if (strikeListThisMonth[i+1] > F)
                        {
                            break;
                        }
                    }
                    //计算近月ivix
                    for (int i = 0; i < strikeListThisMonth.Count(); i++)
                    {
                        iVixInfo info = new iVixInfo();
                        double ask = 0;
                        double bid = 0;
                        double dK = 0;
                        double k = strikeListThisMonth[i];
                        if (i==strikeListThisMonth.Count()-1)
                        {
                            dK = strikeListThisMonth[strikeListThisMonth.Count() - 1] - strikeListThisMonth[strikeListThisMonth.Count() - 2];
                        }
                        else
                        {
                            dK = strikeListThisMonth[i + 1] - strikeListThisMonth[i];
                        }
                        info.strike = k;
                        info.duration = T1;
                        info.coefficient = 2 / info.duration * dK / Math.Pow(info.strike, 2) * Math.Exp(rate * info.duration);
                        if (strikeListThisMonth[i]<kThisMonth)
                        {
                            ask = putDataThisMonth[strikeListThisMonth[i]][index].Ask1;
                            bid = putDataThisMonth[strikeListThisMonth[i]][index].Bid1;
                            var mid = (ask + bid) / 2;
                            info.sigma = Math.Round(ImpliedVolatilityExtension.sigmaByFuture(F, mid, info.strike, info.duration, rate, "认沽"), 4);
                            info.vega = ImpliedVolatilityExtension.ComputeOptionVega(info.strike, info.duration, rate, 0, info.sigma, F * Math.Exp(-rate * info.duration))/100.0;
                            info.ask = ask;
                            info.askv = putDataThisMonth[strikeListThisMonth[i]][index].AskV1;
                            info.bid = bid;
                            info.bidv = putDataThisMonth[strikeListThisMonth[i]][index].BidV1;
                            info.minutelyVolume = ComputeMinutelyVolume(putDataThisMonth[strikeListThisMonth[i]], index);
                        }
                        else if (strikeListThisMonth[i]==kThisMonth)
                        {
                            ask = (putDataThisMonth[strikeListThisMonth[i]][index].Ask1 + callDataThisMonth[strikeListThisMonth[i]][index].Ask1)/2;
                            bid = (putDataThisMonth[strikeListThisMonth[i]][index].Bid1 + callDataThisMonth[strikeListThisMonth[i]][index].Bid1)/2;
                            var mid1 = (putDataThisMonth[strikeListThisMonth[i]][index].Ask1 + putDataThisMonth[strikeListThisMonth[i]][index].Bid1) / 2;
                            var mid2 = (callDataThisMonth[strikeListThisMonth[i]][index].Ask1 + callDataThisMonth[strikeListThisMonth[i]][index].Bid1) / 2;
                            var sigma1= Math.Round(ImpliedVolatilityExtension.sigmaByFuture(F, mid1, info.strike, info.duration, rate, "认沽"), 4);
                            var sigma2= Math.Round(ImpliedVolatilityExtension.sigmaByFuture(F, mid2, info.strike, info.duration, rate, "认购"), 4);
                            var vega1= ImpliedVolatilityExtension.ComputeOptionVega(k, info.duration, rate, 0, sigma1, F * Math.Exp(-rate * info.duration)) / 100.0;
                            var vega2 = ImpliedVolatilityExtension.ComputeOptionVega(k, info.duration, rate, 0, sigma2, F * Math.Exp(-rate * info.duration)) / 100.0;
                            info.sigma = (sigma1 + sigma2) / 2;
                            info.vega = (vega1 + vega2) / 2;
                            info.ask = ask;
                            info.askv = Math.Min(putDataThisMonth[strikeListThisMonth[i]][index].AskV1, callDataThisMonth[strikeListThisMonth[i]][index].AskV1) * 2;
                            info.bid = bid;
                            info.bidv = Math.Min(putDataThisMonth[strikeListThisMonth[i]][index].BidV1, callDataThisMonth[strikeListThisMonth[i]][index].BidV1) * 2;
                            var volumeCall = ComputeMinutelyVolume(callDataThisMonth[strikeListThisMonth[i]], index);
                            var volumePut = ComputeMinutelyVolume(putDataThisMonth[strikeListThisMonth[i]], index);
                            info.minutelyVolume = Math.Min(volumeCall, volumePut) * 2;
                        }
                        else
                        {
                            ask = callDataThisMonth[strikeListThisMonth[i]][index].Ask1;
                            bid = callDataThisMonth[strikeListThisMonth[i]][index].Bid1;
                            var mid = (ask + bid) / 2;
                            info.sigma = Math.Round(ImpliedVolatilityExtension.sigmaByFuture(F, mid, info.strike, info.duration, rate, "认购"), 4);
                            info.vega = ImpliedVolatilityExtension.ComputeOptionVega(k, info.duration, rate, 0, info.sigma, F * Math.Exp(-rate * info.duration)) / 100.0;
                            info.ask = ask;
                            info.askv = callDataThisMonth[strikeListThisMonth[i]][index].AskV1;
                            info.bid = bid;
                            info.bidv = callDataThisMonth[strikeListThisMonth[i]][index].BidV1;
                            info.minutelyVolume = ComputeMinutelyVolume(callDataThisMonth[strikeListThisMonth[i]], index);
                        }
                        sigma1Ask[index] += (2 / T1) * dK / (k * k) * Math.Exp(rate * T1) * ask;
                        sigma1Bid[index] += (2 / T1) * dK / (k * k) * Math.Exp(rate * T1) * bid;
                        thisMonthInfo.Add(info);
                    }
                    
                    sigma1Ask[index] += -1 / T1 * Math.Pow((F / kThisMonth) - 1, 2);
                    sigma1Bid[index] += -1 / T1 * Math.Pow((F / kThisMonth) - 1, 2);
                    sigma1Ask[index] = Math.Sqrt(sigma1Ask[index]);
                    sigma1Bid[index] = Math.Sqrt(sigma1Bid[index]);
                    if (durationThisMonth > 30)
                    {
                        vixAsk[index] = sigma1Ask[index];
                        vixBid[index] = sigma1Bid[index];
                    }
                    //计算时间T NT：近月合约剩余到期时间（以分钟计） T：NT/365
                    double T2 = (durationNextMonth - 1 + (span.Minutes) / 840)/365.0;
                    //找到认购期权价格与认沽期权价格相差最小的执行价的K
                    //计算远期价格F S+exp(RT)×[认购期权价格 S −认沽期权价格 S ]
                    distance1 = 100;
                    double kNextMonth = 0;
                    F = 0;
                    for (int i = 0; i < strikeListNextMonth.Count(); i++)
                    {
                        double distance0 = Math.Abs((callDataNextMonth[strikeListNextMonth[i]][index].Ask1 + callDataNextMonth[strikeListNextMonth[i]][index].Bid1) / 2 - (putDataNextMonth[strikeListNextMonth[i]][index].Ask1 + putDataNextMonth[strikeListNextMonth[i]][index].Bid1) / 2);
                        if (distance0 < distance1)
                        {
                            distance1 = distance0;
                            F = strikeListNextMonth[i] + Math.Exp(rate * T2) * ((callDataNextMonth[strikeListNextMonth[i]][index].Ask1 + callDataNextMonth[strikeListNextMonth[i]][index].Bid1) / 2 - (putDataNextMonth[strikeListNextMonth[i]][index].Ask1 + putDataNextMonth[strikeListNextMonth[i]][index].Bid1) / 2);
                        }
                    }
                    //找到K0
                    for (int i = 0; i < strikeListNextMonth.Count()-1; i++)
                    {
                        kNextMonth = strikeListNextMonth[i];
                        if (strikeListNextMonth[i+1] > F)
                        {
                            break;
                        }
                    }
                    //计算远月ivix
                    for (int i = 0; i < strikeListNextMonth.Count(); i++)
                    {
                        iVixInfo info = new iVixInfo();
                        double ask = 0;
                        double bid = 0;
                        double dK = 0;
                        double k = strikeListNextMonth[i];
                        if (i == strikeListNextMonth.Count() - 1)
                        {
                            dK = strikeListNextMonth[strikeListNextMonth.Count() - 1] - strikeListNextMonth[strikeListNextMonth.Count() - 2];
                        }
                        else
                        {
                            dK = strikeListNextMonth[i + 1] - strikeListNextMonth[i];
                        }
                        info.strike = k;
                        info.duration = T2;
                        info.coefficient = 2 / info.duration * dK / Math.Pow(info.strike, 2) * Math.Exp(rate * info.duration);
                       

                        if (strikeListNextMonth[i] < kNextMonth)
                        {
                            ask = putDataNextMonth[strikeListNextMonth[i]][index].Ask1;
                            bid = putDataNextMonth[strikeListNextMonth[i]][index].Bid1;
                            var mid = (ask + bid) / 2;
                            info.sigma = Math.Round(ImpliedVolatilityExtension.sigmaByFuture(F, mid, info.strike, info.duration, rate, "认沽"), 4);
                            info.vega = ImpliedVolatilityExtension.ComputeOptionVega(info.strike, info.duration, rate, 0, info.sigma, F * Math.Exp(-rate * info.duration)) / 100.0;
                            info.ask = ask;
                            info.askv = putDataNextMonth[strikeListNextMonth[i]][index].AskV1;
                            info.bid = bid;
                            info.bidv = putDataNextMonth[strikeListNextMonth[i]][index].BidV1;
                            info.minutelyVolume = ComputeMinutelyVolume(putDataNextMonth[strikeListNextMonth[i]], index);
                        }
                        else if (strikeListNextMonth[i] == kNextMonth)
                        {
                            ask = (putDataNextMonth[strikeListNextMonth[i]][index].Ask1 + callDataNextMonth[strikeListNextMonth[i]][index].Ask1) / 2;
                            bid = (putDataNextMonth[strikeListNextMonth[i]][index].Bid1 + callDataNextMonth[strikeListNextMonth[i]][index].Bid1) / 2;
                            var mid1 = (putDataNextMonth[strikeListNextMonth[i]][index].Ask1 + putDataNextMonth[strikeListNextMonth[i]][index].Bid1) / 2;
                            var mid2 = (callDataNextMonth[strikeListNextMonth[i]][index].Ask1 + callDataNextMonth[strikeListNextMonth[i]][index].Bid1) / 2;
                            var sigma1 = Math.Round(ImpliedVolatilityExtension.sigmaByFuture(F, mid1, info.strike, info.duration, rate, "认沽"), 4);
                            var sigma2 = Math.Round(ImpliedVolatilityExtension.sigmaByFuture(F, mid2, info.strike, info.duration, rate, "认购"), 4);
                            var vega1 = ImpliedVolatilityExtension.ComputeOptionVega(k, info.duration, rate, 0, sigma1, F * Math.Exp(-rate * info.duration)) / 100.0;
                            var vega2 = ImpliedVolatilityExtension.ComputeOptionVega(k, info.duration, rate, 0, sigma2, F * Math.Exp(-rate * info.duration)) / 100.0;
                            info.sigma = (sigma1 + sigma2) / 2;
                            info.vega = (vega1 + vega2) / 2;
                            info.ask = ask;
                            info.askv = Math.Min(putDataNextMonth[strikeListNextMonth[i]][index].AskV1, callDataNextMonth[strikeListNextMonth[i]][index].AskV1) * 2;
                            info.bid = bid;
                            info.bidv = Math.Min(putDataNextMonth[strikeListNextMonth[i]][index].BidV1, callDataNextMonth[strikeListNextMonth[i]][index].BidV1) * 2;
                            var volumeCall= ComputeMinutelyVolume(callDataNextMonth[strikeListNextMonth[i]], index);
                            var volumePut= ComputeMinutelyVolume(putDataNextMonth[strikeListNextMonth[i]], index);
                            info.minutelyVolume = Math.Min(volumeCall, volumePut) * 2;
                        }
                        else
                        {
                            ask = callDataNextMonth[strikeListNextMonth[i]][index].Ask1;
                            bid = callDataNextMonth[strikeListNextMonth[i]][index].Bid1;
                            var mid = (ask + bid) / 2;
                            info.sigma = Math.Round(ImpliedVolatilityExtension.sigmaByFuture(F, mid, info.strike, info.duration, rate, "认购"), 4);
                            info.vega = ImpliedVolatilityExtension.ComputeOptionVega(k, info.duration, rate, 0, info.sigma, F * Math.Exp(-rate * info.duration)) / 100.0;
                            info.ask = ask;
                            info.askv = callDataNextMonth[strikeListNextMonth[i]][index].AskV1;
                            info.bid = bid;
                            info.bidv = callDataNextMonth[strikeListNextMonth[i]][index].BidV1;
                            info.minutelyVolume = ComputeMinutelyVolume(callDataNextMonth[strikeListNextMonth[i]], index);
                        }
                        sigma2Ask[index] += (2 / T2) * dK / (k * k) * Math.Exp(rate * T2) * ask;
                        sigma2Bid[index] += (2 / T2) * dK / (k * k) * Math.Exp(rate * T2) * bid;
                        nextMonthInfo.Add(info);
                    }
                    sigma2Ask[index] += -1 / T2 * Math.Pow((F / kNextMonth) - 1, 2);
                    sigma2Bid[index] += -1 / T2 * Math.Pow((F / kNextMonth) - 1, 2);
                    sigma2Ask[index] = Math.Sqrt(sigma2Ask[index]);
                    sigma2Bid[index] = Math.Sqrt(sigma2Bid[index]);
                    if (durationThisMonth <= 30)
                    {
                        vixAsk[index] = Math.Sqrt((T1 * Math.Pow(sigma1Ask[index], 2) * (T2 - 30.0 / 365.0) / (T2 - T1) + T2 * Math.Pow(sigma2Ask[index], 2) * (30.0 / 365.0 - T1) / (T2 - T1)) * 365.0 / 30.0);
                        vixBid[index] = Math.Sqrt((T1 * Math.Pow(sigma1Bid[index], 2) * (T2 - 30.0 / 365.0) / (T2 - T1) + T2 * Math.Pow(sigma2Bid[index], 2) * (30.0 / 365.0 - T1) / (T2 - T1)) * 365.0 / 30.0);
                        foreach (var item in thisMonthInfo)
                        {
                            item.coefficient *= T1 * (T2 - 30.0 / 365.0) / (T2 - T1) * 365.0 / 30.0;
                        }
                        foreach (var item in nextMonthInfo)
                        {
                            item.coefficient *= T2 * (30.0 / 365.0 - T1) / (T2 - T1) * 365.0 / 30.0;
                        }
                    }
                    //计算整体的vega，以及盘口的量
                    double vegaTotal = 0;
                    double number = 0;
                    double percentAskMax = 0;
                    double percentAskMin = 1;
                    double percentBidMax = 0;
                    double percentBidMin = 1;
                    double percentVolumeMax = 0;
                    double percentVolumeMin = 1;
                    if (durationThisMonth > 30)
                    {
                        foreach (var item in thisMonthInfo)
                        {
                            vegaTotal += item.vega * item.coefficient*10000;
                        }
                        number = cashVega / vegaTotal;
                        foreach (var item in thisMonthInfo)
                        {
                            double percentAsk = item.askv / number;
                            double percentBid = item.bidv / number;
                            double percentVolume = item.minutelyVolume / number;
                            
                            if (percentAsk>percentAskMax)
                            {
                                percentAskMax = percentAsk;
                            }
                            if (percentAsk<percentAskMin)
                            {
                                percentAskMin = percentAsk;
                            }
                            if (percentBid > percentBidMax)
                            {
                                percentBidMax = percentBid;
                            }
                            if (percentBid < percentBidMin)
                            {
                                percentBidMin = percentBid;
                            }
                            if (percentVolume>percentVolumeMax)
                            {
                                percentVolumeMax = percentVolume;
                            }
                            if (percentVolume<percentVolumeMin)
                            {
                                percentVolumeMin = percentVolume;
                            }
                        }
                    }
                    else
                    {
                        foreach (var item in thisMonthInfo)
                        {
                            vegaTotal += item.vega * item.coefficient * 10000;
                        }
                        foreach (var item in nextMonthInfo)
                        {
                            vegaTotal += item.vega * item.coefficient * 10000;
                        }
                        number = cashVega / 2/vegaTotal;
                        foreach (var item in thisMonthInfo)
                        {
                            double percentAsk = item.askv / number /item.coefficient;
                            double percentBid = item.bidv / number /item.coefficient;
                            double percentVolume = item.minutelyVolume / number;
                            if (percentAsk > percentAskMax)
                            {
                                percentAskMax = percentAsk;
                            }
                            if (percentAsk < percentAskMin)
                            {
                                percentAskMin = percentAsk;
                            }
                            if (percentBid > percentBidMax)
                            {
                                percentBidMax = percentBid;
                            }
                            if (percentBid < percentBidMin)
                            {
                                percentBidMin = percentBid;
                            }
                            if (percentVolume > percentVolumeMax)
                            {
                                percentVolumeMax = percentVolume;
                            }
                            if (percentVolume < percentVolumeMin)
                            {
                                percentVolumeMin = percentVolume;
                            }
                        }
                        foreach (var item in nextMonthInfo)
                        {
                            double percentAsk = item.askv / number / item.coefficient;
                            double percentBid = item.bidv / number / item.coefficient;
                            double percentVolume = item.minutelyVolume / number;
                            if (percentAsk > percentAskMax)
                            {
                                percentAskMax = percentAsk;
                            }
                            if (percentAsk < percentAskMin)
                            {
                                percentAskMin = percentAsk;
                            }
                            if (percentBid > percentBidMax)
                            {
                                percentBidMax = percentBid;
                            }
                            if (percentBid < percentBidMin)
                            {
                                percentBidMin = percentBid;
                            }
                            if (percentVolume > percentVolumeMax)
                            {
                                percentVolumeMax = percentVolume;
                            }
                            if (percentVolume < percentVolumeMin)
                            {
                                percentVolumeMin = percentVolume;
                            }
                        }
                    }

                    dr["tdatetime"] = now;
                    dr["expiredate1"] = expiredate1;
                    dr["expiredate2"] = expiredate2;
                    dr["duration1"] = Math.Round(T1,6);
                    dr["duration2"] = Math.Round(T2,6);
                    dr["sigma1Ask"] = Math.Round(sigma1Ask[index]*100,4);
                    dr["sigma1Bid"] = Math.Round(sigma1Bid[index] * 100, 4);
                    dr["sigma2Ask"] = Math.Round(sigma2Ask[index] * 100, 4);
                    dr["sigma2Bid"] = Math.Round(sigma2Bid[index] * 100, 4);
                    dr["sigmaAsk"] = Math.Round(vixAsk[index] * 100, 4);
                    dr["sigmaBid"] = Math.Round(vixBid[index] * 100, 4);
                    dr["vegaTotal"] = Math.Round(vegaTotal,4);
                    dr["number"] = Math.Round(number,4);
                    dr["percentAskMax"] =Math.Round(percentAskMax,4);
                    dr["percentAskMin"] = Math.Round(percentAskMin,4);
                    dr["percentBidMax"] = Math.Round(percentBidMax,4);
                    dr["percentBidMin"] = Math.Round(percentBidMin,4);
                    dr["percentVolumeMax"] = Math.Round(percentVolumeMax, 4);
                    dr["percentVolumeMin"] = Math.Round(percentVolumeMin, 4);
                    if (now < date.Date + new TimeSpan(14, 57, 00))
                    {
                        dt.Rows.Add(dr);
                    }
                }
                SaveResultToMssql(date, dt);
            }
        }

       
        private double ComputeMinutelyVolume(List<StockOptionTickTransaction> data,int index)
        {
            double volume = 0;
            if (index > 120 && data[index - 120] != null)
            {
                volume = data[index].Volume - data[index - 120].Volume;
            }
            else
            {
                volume = Math.Round(data[index].Volume / Convert.ToDouble(index + 1) * 120.0, 0);
            }
            return volume;
        }

        private void SaveResultToMssql(DateTime date, DataTable dt)
        {
            var sql = string.Format(@"delete from [ivix{0}].[dbo].[{1}] where tdatetime>'{2}' and tdatetime<'{3}'", date.Year, date.ToString("yyyy"), date.ToString("yyyy-MM-dd"), date.AddDays(1).ToString("yyyy-MM-dd"));
            sqlWriter.WriteChanges(sql);
            sqlWriter.InsertBulk(dt, string.Format("[ivix{0}].[dbo].[{1}]", date.Year, date.ToString("yyyy")));
        }

        private void CreateDBOrTableIfNecessary(DateTime date)
        {
            var fileLocation = ConfigurationManager.AppSettings["SqlServerLocation"];
            var sqlScript = string.Format(@"use master
if db_id('ivix{0}') is null
begin
CREATE DATABASE [ivix{0}]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'ivix{0}', FILENAME = N'{2}\ivix{0}.mdf' , SIZE = 5120KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'ivix{0}_log', FILENAME = N'{2}\ivix{0}_log.ldf' , SIZE = 2048KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
ALTER DATABASE [ivix{0}] SET COMPATIBILITY_LEVEL = 120
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [ivix{0}].[dbo].[sp_fulltext_database] @action = 'enable'
end
end
go
if object_id('[ivix{0}].dbo.[{1}]') is null
begin
CREATE TABLE [ivix{0}].[dbo].[{1}](
	[tdatetime] [datetime] NOT NULL,
    [expiredate1] [datetime] NULL,
    [expiredate2] [datetime] NULL,
	[duration1] [decimal](10, 6) NULL,
    [duration2] [decimal](10, 6) NULL,
    [sigma1Ask] [decimal](10, 4) NULL,
    [sigma1Bid] [decimal](10, 4) NULL,
    [sigma2Ask] [decimal](10, 4) NULL,
    [sigma2Bid] [decimal](10, 4) NULL,
    [sigmaAsk] [decimal](10, 4) NULL,
    [sigmaBid] [decimal](10, 4) NULL,
    [vegaTotal] [decimal](10, 4) NULL,
    [number] [decimal](10, 4) NULL,
    [percentAskMax] [decimal](10, 4) NULL,
    [percentAskMin] [decimal](10, 4) NULL,
    [percentBidMax] [decimal](10, 4) NULL,
    [percentBidMin] [decimal](10, 4) NULL,
    [percentVolumeMax] [decimal](10, 4) NULL,
    [percentVolumeMin] [decimal](10, 4) NULL,
	[LastUpdatedTime] [datetime] NULL
) ON [PRIMARY]
ALTER TABLE [ivix{0}].[dbo].[{1}] ADD  CONSTRAINT [DF_{1}_LastUpdatedTime]  DEFAULT (getdate()) FOR [LastUpdatedTime]
CREATE NONCLUSTERED INDEX [IX_{1}_1] ON [ivix{0}].[dbo].[{1}]
(
	[tdatetime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)

end", date.Year, date.ToString("yyyy"), fileLocation);
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private bool ExistInSqlServer(DateTime date)
        {
            var sqlScript = string.Format(@"use master
if db_id('ivix{0}') is not null
begin
	if object_id('[ivix{0}].dbo.[{1}]') is not null
	begin
		select 1 from [ivix{0}].dbo.[{1}] 
	end
end
else
begin
select 0
end ", date.Year, date.ToString("yyyy"));
            var res = sqlReader.ExecuteScriptScalar<int>(sqlScript);
            return res > default(int);
        }

        private DataTable initializeDataTable(DataTable dt)
        {
            dt.Columns.Add("tdatetime");
            dt.Columns.Add("expiredate1");
            dt.Columns.Add("expiredate2");
            dt.Columns.Add("duration1");
            dt.Columns.Add("duration2");
            dt.Columns.Add("sigma1Ask");
            dt.Columns.Add("sigma1Bid");
            dt.Columns.Add("sigma2Ask");
            dt.Columns.Add("sigma2Bid");
            dt.Columns.Add("sigmaAsk");
            dt.Columns.Add("sigmaBid");
            dt.Columns.Add("vegaTotal");
            dt.Columns.Add("number");
            dt.Columns.Add("percentAskMax");
            dt.Columns.Add("percentAskMin");
            dt.Columns.Add("percentBidMax");
            dt.Columns.Add("percentBidMin");
            dt.Columns.Add("percentVolumeMax");
            dt.Columns.Add("percentVolumeMin");
            return dt;
        }

    }

}
