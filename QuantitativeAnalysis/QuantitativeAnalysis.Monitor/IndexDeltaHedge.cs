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
    public class IndexDeltaHedge
    {
        private Logger logger = LogManager.GetCurrentClassLogger();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private string code;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private StockMinuteRepository stockMinutelyRepo;
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private string indexCode;
        private Dictionary<DateTime, Dictionary<string, List<StockTransaction>>> allData = new Dictionary<DateTime, Dictionary<string, List<StockTransaction>>>();

        public IndexDeltaHedge(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, string code)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
            this.code = code;
            indexCode = code == "IF" ? "000300.SH" : "000905.SH";
        }


        public void deltaHedge(DateTime startDate, DateTime endDate)
        {
            getAllData(startDate, endDate);
            var volList = getVolList(startDate, endDate);
            var optionPrice = deltaHedgePerDate(startDate, endDate, volList);
        }


        //计算从开始日期到结束日期的对冲成本
        private double deltaHedgePerDate(DateTime startDate,DateTime endDate, Dictionary<DateTime, double> vol)
        {
            double option = 0;
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            int deltaIndex = 220;
            //计算历史波动率参数
            string hedgeCode = "";
            double deltaNow = 0;
            double pnl = 0;
            double cash = 0;
            //按第一天的开盘价确定期初价格和行权价
            var stock = stockDailyRepo.GetStockTransactionWithRedis(indexCode, startDate, endDate);
            double startPrice = stock[0].Open;
            double strike = startPrice;
            foreach (var date in tradedays)
            {
                //获取当日期货合约代码
                var list = getSpecialFutureList(date);
                //获取当日收盘前标的价格
               // var index = stockMinutelyRepo.GetStockTransactionWithRedis(indexCode, date, date);
                var index = allData[date][indexCode];
                var indexPrice = index[deltaIndex].Close;
                double duration = (DateUtils.GetSpanOfTradeDays(date,endDate)+1/12)/252.0;
                //按标的计算收盘前delta值
                double deltaTarget = ImpliedVolatilityExtension.ComputeOptionDelta(strike, duration, 0.04, 0, "认沽", vol[date], indexPrice);

                //对冲未开仓进行开仓
                if (hedgeCode=="")
                {
                    hedgeCode = list.Last().Value.code;
                    double futurePrice = allData[date][hedgeCode][deltaIndex+1].Close;
                    cash += -futurePrice*deltaTarget;
                    deltaNow = deltaTarget;
                }
                //如果对冲的合约快到期时，进行移仓操作，移仓到季月合约
                if (list.ContainsKey(hedgeCode) && list[hedgeCode].expireDate.Date.AddDays(-7)<=date.Date)
                {
                    double futurePriceFront = allData[date][hedgeCode][deltaIndex + 1].Close;
                    hedgeCode= list.Last().Value.code;
                    double futurePriceNext= allData[date][hedgeCode][deltaIndex + 1].Close;
                    cash += futurePriceFront * deltaNow - futurePriceNext * deltaTarget;
                    deltaNow = deltaTarget;
                }
                else if (list.ContainsKey(hedgeCode)) //对冲的合约未到期，继续对冲
                {
                    double futurePrice = allData[date][hedgeCode][deltaIndex + 1].Close;
                    cash += -futurePrice * (deltaTarget - deltaNow);
                    deltaNow = deltaTarget;
                }
                //错误情况
                if (list.ContainsKey(hedgeCode)==false  && hedgeCode!="")
                {
                    throw new ArgumentOutOfRangeException("对冲选取错误!!");
                }
            }
            //计算最后一天的PNL=cash+期货值的钱+付出去的期权收益
            var lastDate = tradedays.Last();
            double futureLastPrice = allData[lastDate][hedgeCode][239].Close;
            double indexLastPrice = stock.Last().Close;
            pnl =-(cash + deltaNow * futureLastPrice - Math.Max(startPrice - indexLastPrice, 0));
            option = pnl / startPrice;
            return option;
        }

        private Dictionary<DateTime,double> getVolList(DateTime startDate, DateTime endDate,int term=126)
        {
            Dictionary<DateTime, double> vol = new Dictionary<DateTime, double>();
            var originalDate = DateUtils.PreviousTradeDay(startDate, 252);
            var tradedays = dateRepo.GetStockTransactionDate(originalDate, endDate);
            var stock = stockDailyRepo.GetStockTransactionWithRedis(indexCode, originalDate, endDate);
            for (int i = term; i < tradedays.Count(); i++)
            {
                var data = new List<double>();
                for (int j = 0; j < term; j++)
                {
                    data.Add(stock[i - term + j].Close);
                }
                vol.Add(tradedays[i], getHistoricalVol(data));
            }
            return vol;
        }

        private double getHistoricalVol(List<double> data)
        {
            return HistoricalVolatilityExtension.getHistoricalVolatilityByClosePrice(data);
        }

       /// <summary>
       /// 获取每个交易日的全部期货
       /// </summary>
       /// <param name="startDate"></param>
       /// <param name="endDate"></param>
        private void getAllData(DateTime startDate, DateTime endDate)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {
                var dataDaily = new Dictionary<string, List<StockTransaction>>();
                var list = getFutureList(date, 4);
                var indexData = stockMinutelyRepo.GetStockTransactionWithRedis(indexCode, date, date);
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

        /// <summary>
        /// 根据计算规则获取当日交易日的股指期货列表
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        private Dictionary<string, CFEFutures> getFutureList(DateTime date, int number = 3)
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
