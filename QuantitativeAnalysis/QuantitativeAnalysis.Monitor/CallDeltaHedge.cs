using Autofac;
using NLog;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Monitor
{
    public class CallDeltaHedge
    {
        private double rate;
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private int duration;
        private Dictionary<DateTime, double> priceDic=new Dictionary<DateTime, double>();
        private List<StockTransaction> priceList;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockTickRepository stockRepo;
        private StockDailyRepository stockDailyRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;


        public CallDeltaHedge(StockTickRepository stockRepo, StockDailyRepository stockDailyRepo, string code,int duration)
        {
            this.stockRepo = stockRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Server84);
            this.code = code;
            this.duration = duration;
        }

        public void compute(DateTime startDate, DateTime endDate)
        {
            var startday=DateTimeExtension.DateUtils.PreviousTradeDay(startDate, duration + 1);
            var endday= DateTimeExtension.DateUtils.PreviousTradeDay(endDate, duration + 1);
            priceList = stockDailyRepo.GetStockTransaction(code, startday, endDate);
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            if (priceDic==null || priceDic.Count()==0)
            {
                foreach (var item in priceList)
                {
                    priceDic.Add(item.DateTime, item.Close);
                }
            }
            for (int i = 0; i < tradedays.Count()-duration; i++)
            {
                var call=-getHedgeCost(tradedays[i],tradedays[i+duration] , code);
                Console.WriteLine("call from {0} to {1} costs {2}!",tradedays[i], tradedays[i + duration], call);
            }
        }

        private double getHedgeCost(DateTime startDate, DateTime endDate, string code)
        {
            double cost = 0;
            double position = 0;
            double price = 0;
            double strike = 0;
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            for (int i = 0; i < tradedays.Count(); i++)
            {
                var date = tradedays[i];
                var duartionRemain = (tradedays.Count() - 1 - i) / 252.0;
                price = getStockPrice(date);
                if (strike == 0)
                {
                    strike = price;
                }
                double vol = getHisVol(date, duration);
                double targetDelta = getOptionDelta(strike, duartionRemain, 0.04,0,vol,price);
                cost = cost - (targetDelta - position) * price;
                if (targetDelta>position)
                {
                    cost = cost - (targetDelta - position) * price*1.0002;
                }
                else
                {
                    cost = cost - (targetDelta - position) * price * 1.0012;
                }
                position = targetDelta;
            }
            cost = cost + position * price-getPayoffOfCall(price,strike);
            return cost/strike;
        }

        private double getPayoffOfCall(double price,double strike)
        {
            double payoff = 0;
            if (price>strike)
            {
                payoff = price - strike;
            }
            return payoff;
        }

        private double getOptionDelta(double strike,double duration,double r,double dividend,double vol,double stockPrice)
        {
            double delta = 0;
            delta = ImpliedVolatilityExtension.ComputeOptionDelta(strike, duration, r, dividend, "认购", vol, stockPrice);
            return delta;
        }

        private double getHisVol(DateTime today,int duration)
        {
            int index=0;
            List<double> priceNowList = new List<double>();
            for (int i = 0; i < priceList.Count(); i++)
            {
                if (priceList[i].DateTime==today)
                {
                    index = i;
                    break;
                }
            }
            for (int i = index-duration-1; i < index; i++)
            {
                priceNowList.Add(priceList[i].Close);
            }
            double vol = HistoricalVolatilityExtension.getHistoricalVolatilityByClosePrice(priceNowList);
            return vol;
        }

        private double getStockPrice(DateTime today)
        {
            
            double price = 0;
            price = priceDic[today];
            return price;
        }
    }
}
