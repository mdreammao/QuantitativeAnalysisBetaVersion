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

namespace QuantitativeAnalysis.Monitor
{
    public class OptionMonitor50ETF2019
    {
        private double rate = 0.04;
        private double durationModifiedCoff = 0.05;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string underlying = "510050.SH";
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private OptionInfoRepository infoRepo;
        private StockDailyRepository stockDailyRepo;
        private StockMinuteRepository stockMinutelyRepo;
        private StockOptionDailyRepository optionDailyRepo;
        private List<DateTime> tradedays;
        private List<StockOptionInformation> optionList;
        private List<StockOptionInformationWithModified> optionListWithModified;
        private Dictionary<string, StockOptionInformationWithModified> optionListModifiedByCode;
        private Dictionary<DateTime, StockTransaction> underlyingDailyData;
        private Dictionary<DateTime, List<StockOptionTransaction>> optionDailyDataByDate;
        private Dictionary<string, List<StockOptionTransaction>> optionDailyDataByCode;
        private List<StockTransaction> underlyingDailyDataList;
        private List<underlyingDividendInformation> dividendList;
        

        public OptionMonitor50ETF2019(OptionInfoRepository infoRepo, TransactionDateTimeRepository dateRepo,StockDailyRepository stockDailyRepo, StockMinuteRepository stockMinutelyRepo,StockOptionDailyRepository optionDailyRepo,DateTime startDate,DateTime endDate,double rate = 0.04)
        {
            this.infoRepo = infoRepo;
            this.dateRepo = dateRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.optionDailyRepo = optionDailyRepo;
            this.rate = rate;
            this.tradedays= dateRepo.GetStockTransactionDate(startDate, endDate);
            this.optionList=getOptionInformation(startDate, endDate);
            this.underlyingDailyData = getUnderlyingDailyData(startDate, endDate);
            this.dividendList = getUnderlyingDividendInformation();
            this.optionListWithModified = getModifiedOptionList(optionList, dividendList);
            this.optionDailyDataByCode = getEtfOptionDailyDataByCode(startDate, endDate);
            this.optionDailyDataByDate = getEtfOptionDailyDataByDate(startDate, endDate);
            this.optionListModifiedByCode = getOptionInformationWithModifiedByCode(optionListWithModified);
            computeOptionStatus(startDate, endDate);
        }

        public void computeOptionStatus(DateTime startDate,DateTime endDate)
        {
            var tradedays= dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {

                var today = date.Date + new TimeSpan(15, 0, 0);
                getGreekInformation(today, underlyingDailyData[date],optionListModifiedByCode,optionDailyDataByDate[date]);

            }
        }

        private void getGreekInformation(DateTime date, StockTransaction underlyingData, Dictionary<string,StockOptionInformationWithModified> optionList, List<StockOptionTransaction> optionData)
        {
            double oneYearDays = 280;
            double underlyingClose = underlyingData.Close;
            
            Dictionary<DateTime, double> durationDic = new Dictionary<DateTime, double>();
            Dictionary<DateTime, List<double>> strikeDic = new Dictionary<DateTime, List<double>>();
            Dictionary<DateTime, double> basisList = new Dictionary<DateTime, double>();
            Dictionary<DateTime, Dictionary<double, optionGreeks>> greeks = new Dictionary<DateTime, Dictionary<double, optionGreeks>>();
            //希腊值的处理
            //预处理到期日和行权价
            foreach (var option in optionData)
            {
                var info = optionList[option.Code];
                var expiredate = info.expireDate;
                double strike = info.strike;
                double unit = info.unit;
                if (info.existsModified==true && date<info.dividendDate)
                {
                    strike = info.strike;
                    unit = info.unitBeforeModifed;
                }
                if (durationDic.ContainsKey(expiredate)==false)
                {
                    double modifiedDays = getModifiedDuration(date, expiredate, durationModifiedCoff);
                    oneYearDays = getModifiedDuration(expiredate.AddYears(-1), expiredate, durationModifiedCoff);
                    double duration = modifiedDays / oneYearDays;
                    durationDic.Add(expiredate, duration);
                }
                if (strikeDic.ContainsKey(expiredate)==false)
                {
                    List<double> strikeList = new List<double>();
                    strikeList.Add(strike);
                    strikeDic.Add(expiredate, strikeList);
                }
                else
                {
                    if (strikeDic[expiredate].Contains(strike)==false)
                    {
                        strikeDic[expiredate].Add(strike);
                    }
                }

            }
            //逐月逐行权价，计算基差,同时计算基本信息
            foreach (var list in strikeDic)
            {
                DateTime expireDate = list.Key;
                double avgBasis = 0;
                Dictionary<double, double> basisByStrike = new Dictionary<double, double>();
                foreach (var item in list.Value)
                {
                    double strike = item;
                    double callPrice=0;
                    double putPrice=0;
                    foreach (var option in optionData)
                    {
                        var info = optionList[option.Code];
                        //找出认购期权
                        if (info.type=="认购" && info.expireDate==expireDate)
                        {
                            if (info.existsModified == true && date < info.dividendDate)
                            {
                                if (info.strikeBeforeModified==strike)
                                {
                                    callPrice = option.Close;
                                }
                            }
                            if (info.existsModified == false && info.strike==strike)
                            {
                                callPrice = option.Close;
                            }
                        }
                        //找出认沽期权
                        if (info.type == "认沽" && info.expireDate == expireDate)
                        {
                            if (info.existsModified == true && date < info.dividendDate)
                            {
                                if (info.strikeBeforeModified == strike)
                                {
                                    putPrice = option.Close;
                                }
                            }
                            if (info.existsModified == false && info.strike == strike)
                            {
                                putPrice = option.Close;
                            }
                        }
                    }
                    double durationOfInterest = getInterestDuartion(date, expireDate);
                    double basis = callPrice - putPrice + strike * Math.Exp(-rate * durationOfInterest) - underlyingClose;
                    basisByStrike.Add(strike, basis);

                }
                List<double> strikeOrdered = list.Value.OrderBy(x => Math.Abs(x - underlyingClose)).ToList();
                for (int i = 0; i < 3; i++)
                {
                    avgBasis +=basisByStrike[strikeOrdered[i]];
                }
                avgBasis = avgBasis / 3;
                basisList.Add(expireDate, avgBasis);
            }


        }


        private Dictionary<string,StockOptionInformationWithModified> getOptionInformationWithModifiedByCode(List<StockOptionInformationWithModified> optionListWithModified)
        {
            Dictionary<string, StockOptionInformationWithModified> dic = new Dictionary<string, StockOptionInformationWithModified>();
            foreach (var item in optionListWithModified)
            {
                dic.Add(item.code, item);
            }
            return dic;
        }


        private List<StockOptionInformation> getOptionInformation(DateTime startDate,DateTime endDate)
        {
            var infoList=infoRepo.GetStockOptionInfo(underlying, startDate, endDate);
            return infoList;
        }

        private Dictionary<DateTime, StockTransaction> getUnderlyingDailyData(DateTime startDate,DateTime endDate)
        {
            Dictionary<DateTime, StockTransaction> underlyingDailyData = new Dictionary<DateTime, StockTransaction>();
            var data= stockDailyRepo.GetStockTransaction(underlying, startDate, endDate);
            foreach (var item in data)
            {
                underlyingDailyData.Add(item.DateTime, item);
            }
            this.underlyingDailyDataList = data;
            return underlyingDailyData;
        }

        private Dictionary<DateTime, List<StockOptionTransaction>> getEtfOptionDailyDataByDate(DateTime startDate,DateTime endDate)
        {
            Dictionary<DateTime, List<StockOptionTransaction>> data = new Dictionary<DateTime, List<StockOptionTransaction>>();
            tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {
                List<StockOptionTransaction> dataToday = new List<StockOptionTransaction>();
                foreach (var info in optionList)
                {
                    if (date>=info.listedDate && date<=info.expireDate)
                    {
                        var optionDataByCode = optionDailyDataByCode[info.code];
                        foreach (var optiondata0 in optionDataByCode)
                        {
                            if (optiondata0.DateTime.Date == date.Date)
                            {
                                dataToday.Add(optiondata0);
                            }
                        }
                    }
                }
                data.Add(date, dataToday);
            }
            return data;
        }

        private Dictionary<string, List<StockOptionTransaction>> getEtfOptionDailyDataByCode(DateTime startDate, DateTime endDate)
        {
            Dictionary<string, List<StockOptionTransaction>> data = new Dictionary<string, List<StockOptionTransaction>>();
            foreach (var option in optionList)
            {
                if (option.listedDate<=endDate && option.expireDate>=startDate)
                {
                    DateTime lastDate = option.expireDate;
                    if (lastDate>endDate)
                    {
                        lastDate = endDate;
                    }
                    var data0 = optionDailyRepo.GetStockOptionTransaction(option.code, option.listedDate, lastDate);
                    data.Add(option.code, data0);
                }
            }
            return data;
        }

        private List<underlyingDividendInformation> getUnderlyingDividendInformation()
        {
            List<underlyingDividendInformation> info = new List<underlyingDividendInformation>();
            for (int i = 1; i < underlyingDailyDataList.Count(); i++)
            {
                var yesterdayData = underlyingDailyDataList[i - 1];
                var todayData = underlyingDailyDataList[i];
                double dividend = 0;
                if (todayData.AdjFactor > yesterdayData.AdjFactor)
                {
                    dividend =Math.Round(yesterdayData.Close * (1-yesterdayData.AdjFactor/todayData.AdjFactor),3);
                    underlyingDividendInformation info0 = new underlyingDividendInformation();
                    info0.date = todayData.DateTime.Date;
                    info0.yesterday = yesterdayData.DateTime.Date;
                    info0.underlying = underlying;
                    info0.dividend = dividend;
                    info.Add(info0);
                }
            }
            return info;
        }

        private List<StockOptionInformationWithModified> getModifiedOptionList(List<StockOptionInformation> optionList, List<underlyingDividendInformation> dividendList)
        {
            List<StockOptionInformationWithModified> list = new List<StockOptionInformationWithModified>();
            foreach (var option in optionList)
            {
                StockOptionInformationWithModified modified = new StockOptionInformationWithModified();
                if (option.unit!=10000)
                {
                    foreach (var dividendInfo in dividendList)
                    {
                        if (dividendInfo.date>option.listedDate && dividendInfo.date<=option.expireDate)
                        {
                            modified.code = option.code;
                            modified.dividend = dividendInfo.dividend;
                            modified.dividendDate = dividendInfo.date;
                            modified.exerciseMode = option.exerciseMode;
                            modified.existsModified = true;
                            modified.expireDate = option.expireDate;
                            modified.listedDate = option.listedDate;
                            modified.name = option.name;
                            modified.strike = option.strike;
                            modified.strikeBeforeModified = Math.Round(option.strike * option.unit / 10000, 2);
                            modified.type = option.type;
                            modified.underlying = option.underlying;
                            modified.unit = option.unit;
                            modified.unitBeforeModifed = 10000;
                        }
                    }
                }
                else
                {
                    modified.code = option.code;
                    modified.dividend = 0;
                    modified.exerciseMode = option.exerciseMode;
                    modified.existsModified = false;
                    modified.expireDate = option.expireDate;
                    modified.listedDate = option.listedDate;
                    modified.name = option.name;
                    modified.strike = option.strike;
                    modified.strikeBeforeModified = option.strike;
                    modified.type = option.type;
                    modified.underlying = option.underlying;
                    modified.unit = option.unit;
                    modified.unitBeforeModifed = 10000;
                }
                list.Add(modified);
            }
            return list;
        }

       
        private double getInterestDuartion(DateTime date, DateTime expiringDate)
        {
            double duration = 0;
            TimeSpan days = expiringDate.Subtract(date);
            int numOfDays = days.Days;
            duration = numOfDays / 365.0;
            return duration;
        }

        /// <summary>
        /// 计算调整后的到期时间
        /// 非交易时间1s=交易时间的modifiedParameter*1s
        /// </summary>
        /// <param name="date">当前时间</param>
        /// <param name="expiringDate">到期时间</param>
        /// <param name="modifiedParameter">调整系数</param>
        /// <returns>调整后的时间</returns>
        private double getModifiedDuration(DateTime date,DateTime expiringDate,double modifiedParameter)
        {
            double duration = 0;
            var tradedays= dateRepo.GetStockTransactionDate(date, expiringDate);
            TimeSpan days = expiringDate.Subtract(date);
            int numOfDays = days.Days;
            //计算交易时间
            int numOfTradedays = tradedays.Count() - 1;
            //计算非交易日调整时间
            double numOfNonTradedays = (numOfDays - numOfTradedays) * (24 * modifiedParameter) / (4 * 1 + 20 * modifiedParameter);
            //计算当日到收盘的调整时间
            TimeSpan intradayTime = date.TimeOfDay;
            TimeSpan closeTime = new TimeSpan(15, 0, 0);
            var gap = closeTime.Subtract(intradayTime);
            double seconds = gap.TotalSeconds;
            double numOfintraday = 0;
            if (DateTimeExtension.DateUtils.IsTradeDay(date)==true)
            {
                if (seconds<=0)
                {
                    seconds = seconds * modifiedParameter;
                }
                else if (seconds<=3600*2)
                {
                    seconds = seconds*1;
                }
                else if (seconds<=3600*3.5)
                {
                    seconds = 7200 + (seconds - 7200) * modifiedParameter;
                }
                else if (seconds<=3600*5.5)
                {
                    seconds = 7200 + 1.5 * 3600 * modifiedParameter + (seconds - 3.5 * 3600);
                }
                else 
                {
                    seconds = 7200 + 1.5 * 3600 * modifiedParameter + 7200+(seconds-5.5*3600)*modifiedParameter;
                }
                numOfintraday = seconds / (4 * 1 + 20 * modifiedParameter)/3600;
            }
            else
            {
                numOfintraday = seconds * modifiedParameter/(4*1+20*modifiedParameter)/3600;
            }

            duration = numOfTradedays + numOfNonTradedays + numOfintraday;
            return duration;
        }

    }
}
