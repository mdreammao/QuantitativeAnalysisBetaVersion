using Autofac;
using NLog;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.Utilities;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static QuantitativeAnalysis.Utilities.DateTimeExtension;

namespace QuantitativeAnalysis.Monitor
{
    public class BasisFrontNext
    {
        private Logger logger = LogManager.GetCurrentClassLogger();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private string code;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private StockMinuteRepository stockMinutelyRepo;
        private TransactionDateTimeRepository dateRepo;
        private string indexCode;
        private Dictionary<DateTime, Dictionary<string, List<StockTransaction>>> allData = new Dictionary<DateTime, Dictionary<string, List<StockTransaction>>>();


        public BasisFrontNext(StockMinuteRepository stockMinutelyRepo, string code)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
            this.code = code;
            indexCode = code == "IF" ? "000300.SH" : "000905.SH";

        }

        public void parameterIteration(DateTime startDate, DateTime endDate,double slipPoints=5)
        {
            getAllData(startDate,endDate);
            for (int openStdMultiple = 2; openStdMultiple <=3; openStdMultiple++)
            {
                for (int closeStdMultiple = 0; closeStdMultiple <=3; closeStdMultiple++)
                {
                    var stopLossStdMultiple = (openStdMultiple + closeStdMultiple) / 2.0;
                    for (int numOfHistoricalDays = 1; numOfHistoricalDays <=8 ; numOfHistoricalDays++)
                    {
                        for (int KLineScale = 1; KLineScale <=5; KLineScale=KLineScale+4)
                        {
                            double winningRate = 0;
                            double winningRateAfterSlip = 0;
                            var performance = compute(startDate, endDate, openStdMultiple, closeStdMultiple, stopLossStdMultiple, numOfHistoricalDays, KLineScale, slipPoints,ref winningRate,ref winningRateAfterSlip);
                            var record = getParaPerformance(performance);
                            record.closeStdMultiple = closeStdMultiple;
                            record.KLineScale = KLineScale;
                            record.numOfHistoricalDays = numOfHistoricalDays;
                            record.openStdMultiple = openStdMultiple;
                            record.stopLossStdMultiple = stopLossStdMultiple;
                            record.slip = slipPoints;
                            record.rateOfWinning = winningRate;
                            record.rateOfWinningAfterSlip = winningRateAfterSlip;
                        }
                    }
                }
            }
        }

        private paraPerformance getParaPerformance(List<double> performance)
        {
            paraPerformance record = new paraPerformance();
            record.maxDrawdown = performance.Min(x => x);
            record.profit = performance.Last();
            return record;
        }

        private void getAllData(DateTime startDate, DateTime endDate)
        {
            var dataStartDate = DateUtils.PreviousTradeDay(startDate, 20);
            var tradedays = dateRepo.GetStockTransactionDate(dataStartDate, endDate);
            foreach (var date in tradedays)
            {
                var dataDaily = new Dictionary<string, List<StockTransaction>>();
                var list = getFutureList(date,4);
                var indexData= stockMinutelyRepo.GetStockTransactionWithRedis(indexCode, date, date);
                dataDaily.Add(indexCode, indexData);
                foreach (var item in list)
                {
                    var data = stockMinutelyRepo.GetStockTransactionWithRedis(item.Key, date, date);
                    data.RemoveAt(241);
                    data.RemoveAt(0);
                    dataDaily.Add(item.Key, data);
                }
                allData.Add(date, dataDaily);
            }
        }

        private List<double> compute(DateTime startDate, DateTime endDate,double openStdMultiple,double closeStdMultiple,double stopLossStdMultiple,int numOfHistoricalDays,int KLineScale,double slipPoints,ref double winningRate,ref double winningRateAfterSlip)
        {
            var pnlList = new List<double>();
            double pnl = 0;
            double total = 0;
            double winning = 0;
            double winningAfterSlip = 0;
            double position=0;//当月开空仓位-1，当月不开仓为0，当月开多仓位1
            double openBasis=0;//记录逐笔开仓的负基差位置
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {
                var codeList = getFutureList(date, 2);
                var codeKey = new List<string>(codeList.Keys);
                var allDataToday = allData[date];
                var dataList = getHistoricalData(date, numOfHistoricalDays, KLineScale);
                var mean = getMean(dataList);
                var std = getStd(dataList);
                var frontDataAvgPrice = getDataDaily(date, codeKey[0],1);
                var nextDataAvgPrice = getDataDaily(date, codeKey[1], 1);
                var frontData = allDataToday[codeKey[0]];
                var nextDate = allDataToday[codeKey[1]];
                bool label = true;
                var frontExpireDate = codeList[codeKey[0]].expireDate;
                if (date.Date.AddDays(7)>frontExpireDate.Date)
                {
                    label = false;
                }
                else
                {
                    label = true;
                }
                //逐分钟进行遍历，进行开平仓判断和止损判断
                for (int i = 5; i < 235; i++)
                {
                    double basisNow = nextDate[i].Close - frontData[i].Close;
                    double basisNowActual = nextDate[i].Close - frontData[i].Close;
                    //double basisNow = nextDataAvgPrice[i] - frontDataAvgPrice[i];
                    if (position==-1)
                    {
                        //止盈止损
                        if (basisNow<(openBasis-std*stopLossStdMultiple))//止损
                        {
                            position = 0;
                            var closeBasis = basisNowActual;
                            pnl += -slipPoints;
                            pnl += closeBasis - openBasis;
                            openBasis = 0;
                        }
                        else if (basisNow > (mean + std * closeStdMultiple)) //止盈
                        {
                            position = 0;
                            winning += 1;
                            var closeBasis = basisNowActual;
                            pnl += -slipPoints;
                            pnl += closeBasis - openBasis;
                            if (closeBasis - openBasis > 2 * slipPoints)
                            {
                                winningAfterSlip += 1;
                            }
                            openBasis = 0;
                        }
                    }
                    else if (position==0 && label==true)
                    {
                        if (basisNow<(mean-std*openStdMultiple))
                        {
                            position = -1;
                            total += 1;
                            openBasis = basisNowActual;
                            pnl += -slipPoints;
                        }
                        else if (basisNow > (mean + std * openStdMultiple))
                        {
                            position = 1;
                            total += 1;
                            openBasis= basisNowActual;
                            pnl += -slipPoints;
                        }
                    }
                    else if (position==1)
                    {
                        //止盈止损
                        if (basisNow > (openBasis + std * stopLossStdMultiple))//止损
                        {
                            position = 0;
                            var closeBasis = basisNowActual;
                            pnl += -slipPoints;
                            pnl +=-(closeBasis - openBasis);
                            openBasis = 0;
                        }
                        else if (basisNow < (mean - std * closeStdMultiple))//止盈
                        {
                            position = 0;
                            winning += 1;
                            var closeBasis = basisNowActual;
                            pnl += -slipPoints;
                            pnl += -(closeBasis - openBasis);
                            if (-(closeBasis - openBasis) > 2 * slipPoints)
                            {
                                winningAfterSlip += 1;
                            }
                            openBasis = 0;
                        }
                    }
                }
                //计算每日收盘时候的盈亏
                if (position!=0)
                {
                    var closeDailyBasis = nextDate[239].Close - frontData[239].Close;
                    if (position==-1)
                    {
                        pnlList.Add(pnl + (closeDailyBasis - openBasis));
                    }
                    else if (position==1)
                    {
                        pnlList.Add(pnl - (closeDailyBasis - openBasis));
                    }
                    //假设当月合约到期了，按当期日的收盘价强平
                    if (date.Date==codeList[codeKey[0]].expireDate.Date)
                    {
                        if (position==-1)
                        {
                            if ((closeDailyBasis - openBasis)>0)
                            {
                                winning += 1;
                            }
                            if ((closeDailyBasis - openBasis)>2*slipPoints)
                            {
                                winningAfterSlip += 1;
                            }
                        }
                        else if (position == 1)
                        {
                            if (-(closeDailyBasis - openBasis) > 0)
                            {
                                winning += 1;
                            }
                            if (-(closeDailyBasis - openBasis) > 2 * slipPoints)
                            {
                                winningAfterSlip += 1;
                            }
                        }
                        position = 0;
                        pnl = pnlList.Last();
                        
                        
                    }
                }
                else
                {
                    pnlList.Add(pnl);
                }
            }
            winningRate = winning / total;
            winningRateAfterSlip = winningAfterSlip / total;
            return pnlList;
        }

        private List<double> getHistoricalData(DateTime today,int numOfHistoricalDays,int KLineScale)
        {
            var dataStartDate = DateUtils.PreviousTradeDay(today, numOfHistoricalDays);
            var dataEndDdate = DateUtils.PreviousTradeDay(today, 1);
            var tradedays = dateRepo.GetStockTransactionDate(dataStartDate, dataEndDdate);
            var basisList = new List<double>();
            foreach (var date in tradedays)
            {
                var list = getFutureList(date, 2);
                var listKey = new List<string>(list.Keys);
                var dataToday = allData[date];
                var frontData = getDataDaily(date,listKey[0],KLineScale);
                var nextData = getDataDaily(date, listKey[1],KLineScale);
                for (int i = 0; i < frontData.Count(); i++)
                {
                    basisList.Add(nextData[i] - frontData[i]);
                }
            }
            return basisList;
        }

        private List<double> getDataDaily(DateTime today, string code,int KLineScale = 1)
        {
            var dataToday = allData[today];
            var data = dataToday[code];
            var dataDaily = new List<double>();
            for (int i = 0; i <= 240 - KLineScale; i = i + KLineScale)
            {
                double amount = 0;
                double volume = 0;
                double price = 0;
                for (int j = 0; j < KLineScale; j++)
                {
                    amount += data[i + j].Amount;
                    volume += data[i + j].Volume;
                }
                price = volume == 0 ? data[i + KLineScale - 1].Close : (amount / volume) / 200.0;
                dataDaily.Add(price);
            }
            return dataDaily;
        }

        private double getMean(List<double> list)
        {
            double mean = 0;
            for (int i = 0; i < list.Count(); i++)
            {
                mean += list[i];
            }
            return mean / list.Count();
        }


        private double getStd(List<double> list)
        {
            double variance = 0;
            double std = 0;
            double mean = getMean(list);
            for (int i = 0; i < list.Count(); i++)
            {
                variance += Math.Pow(list[i] - mean, 2)/list.Count();
            }
            std = Math.Sqrt(variance);
            return std;
        }

        /// <summary>
        /// 根据计算规则获取当日交易日的股指期货列表
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        private Dictionary<string, CFEFutures> getFutureList(DateTime date,int number=3)
        {
            List<CFEFutures> list = new List<CFEFutures>();
            List<DateTime> dateList = new List<DateTime>();
            Dictionary<string, CFEFutures> dic = new Dictionary<string, CFEFutures>();
            var expireDateOfThisMonth = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
            if (date > expireDateOfThisMonth)
            {
                date = DateUtils.GetFirstDateOfNextMonth(date);
            }
            dateList.Add(date);
            var date2 = DateUtils.GetFirstDateOfNextMonth(date);
            dateList.Add(date2);
            var date3 = DateUtils.GetLastDateOfThisSeason(date2);
            if (date3.Month == date2.Month)
            {
                date3 = DateUtils.GetFirstDateOfNextMonth(date3);
                date3 = DateUtils.GetLastDateOfThisSeason(date3);
            }
            dateList.Add(date3);
            var date4 = DateUtils.GetLastDateOfThisSeason(DateUtils.GetFirstDateOfNextMonth(date3));
            dateList.Add(date4);
            for (int i = 0; i < number; i++)
            {
                date = dateList[i];
                var future = new CFEFutures();
                string year = date.Year.ToString();
                year = year.Substring(year.Length - 2, 2);
                string month = "0" + date.Month.ToString();
                month = month.Substring(month.Length - 2, 2);
                future.code = code + year + month + ".CFE";
                future.expireDate = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
                list.Add(future);
            }
            foreach (var item in list)
            {
                dic.Add(item.code, item);
            }
            return dic;
        }

        /// <summary>
        /// 根据计算规则获取当日交易日的当月和季月的股指期货列表
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        private Dictionary<string, CFEFutures> getSpecialFutureList(DateTime date)
        {
            List<CFEFutures> list = new List<CFEFutures>();
            List<DateTime> dateList = new List<DateTime>();
            Dictionary<string, CFEFutures> dic = new Dictionary<string, CFEFutures>();
            var expireDateOfThisMonth = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
            if (date > expireDateOfThisMonth)
            {
                date = DateUtils.GetFirstDateOfNextMonth(date);
            }
            dateList.Add(date);
            var date2 = DateUtils.GetLastDateOfThisSeason(date);
            if (date.Month == date2.Month)
            {
                date2 = DateUtils.GetLastDateOfThisSeason(DateUtils.GetFirstDateOfNextMonth(date));
            }
            dateList.Add(date2);
            for (int i = 0; i < 2; i++)
            {
                date = dateList[i];
                var future = new CFEFutures();
                string year = date.Year.ToString();
                year = year.Substring(year.Length - 2, 2);
                string month = "0" + date.Month.ToString();
                month = month.Substring(month.Length - 2, 2);
                future.code = code + year + month + ".CFE";
                future.expireDate = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(date.Year, date.Month));
                list.Add(future);
            }
            foreach (var item in list)
            {
                dic.Add(item.code, item);
            }
            return dic;
        }
    }
}
