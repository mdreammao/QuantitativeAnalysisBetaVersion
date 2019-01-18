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
                        break;
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

        private void getGreekInformation(DateTime date, StockTransaction underlyingData, List<StockOptionInformationWithModified> optionListWithModified)
        {
            
        }

    }
}
