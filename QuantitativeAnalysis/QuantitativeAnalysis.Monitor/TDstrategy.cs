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
    public class TDstrategy
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private StockMinuteRepository stockMinutelyRepo;

        public TDstrategy(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, string code)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            dateRepo = new TransactionDateTimeRepository(ConnectionType.Default);
            this.code = code;
        }

        public void compute(DateTime startDate, DateTime endDate,int delaynum=4,int startnum=4,int buynum=4)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            var etfall= stockMinutelyRepo.GetStockTransaction(code, tradedays.First(),tradedays.Last());
            var netvalue = new double[etfall.Count()];
            var signal = new double[etfall.Count()];
            var buyStart = new double[etfall.Count()];
            var buyCalculator = new double[etfall.Count()];
            var buySignal = new double[etfall.Count()];
            var sellStart = new double[etfall.Count()];
            var sellCalculator = new double[etfall.Count()];
            var sellSignal = new double[etfall.Count()];
            int startnumNow = 0;
            int buynumNow = 0;
            int sellnum = buynum;
            int sellnumNow = 0;
            bool start = false;
            bool buy = false;
            double startPrice = 0;
            double stoplossPrice = 10000;

            //买入启动
            startnumNow = 0;
            for (int i = delaynum; i < etfall.Count() - delaynum; i++)
            {
                
                if (etfall[i].Close < etfall[i - delaynum].Close)
                {
                    startnumNow += 1;
                }
                else
                {
                    startnumNow = 0;
                }
                if (startnumNow>=startnum)
                {
                    buyStart[i] = 1;
                }
            }
            //买入计数
            buynumNow = 0;
            double buyCalculatorStartPrice = 0;
            bool buyCalculatorStart = false;
            for (int i = delaynum; i < etfall.Count() - delaynum; i++)
            {
                if (buyStart[i]==1)
                {
                    buyCalculatorStart = true;
                    buyCalculatorStartPrice = 0;
                    buynumNow = 0;
                }
                if (buyCalculatorStart==true && etfall[i].Close>etfall[i-2].High && etfall[i].High>=etfall[i-1].High)  
                {
                    if (buynumNow>0 && etfall[i].Close>=buyCalculatorStartPrice)
                    {
                        buynumNow += 1;
                        buyCalculator[i] = buynumNow;
                        if (buynumNow == buynum)
                        {
                            buySignal[i + 1] = 1;
                            buyCalculatorStart = false;
                        }
                    }
                    if (buynumNow==0)
                    {
                        buynumNow += 1;
                        buyCalculatorStartPrice = etfall[i].Close;
                    }
                }
                if (buynumNow>0)
                {
                    buyCalculator[i] = buynumNow;
                }
            }

            //卖出启动
            startnumNow = 0;
            for (int i = delaynum; i < etfall.Count() - delaynum; i++)
            {

                if (etfall[i].Close < etfall[i - delaynum].Close)
                {
                    startnumNow += 1;
                }
                else
                {
                    startnumNow = 0;
                }
                if (startnumNow >= startnum)
                {
                    sellStart[i] = 1;
                }
            }
            //买入计数
            buynumNow = 0;
            double sellCalculatorStartPrice = 99999999;
            bool sellCalculatorStart = false;
            for (int i = delaynum; i < etfall.Count() - delaynum; i++)
            {
                if (sellStart[i] == 1)
                {
                    sellCalculatorStart = true;
                    sellCalculatorStartPrice = 99999999;
                    sellnumNow = 0;
                }
                if (sellCalculatorStart ==true && etfall[i].Close < etfall[i - 2].Low && etfall[i].Low<= etfall[i - 1].Low)
                {
                    if (sellnumNow > 0 && etfall[i].Close <= sellCalculatorStartPrice)
                    {
                        sellnumNow += 1;
                        if (sellnumNow == sellnum)
                        {
                            sellSignal[i + 1] = -1;
                            sellCalculatorStart = false;
                        }
                    }
                    if (sellnumNow == 0)
                    {
                        sellnumNow += 1;
                        sellCalculatorStartPrice = etfall[i].Close;
                    }
                }
                if (sellnumNow > 0)
                {
                    sellCalculator[i] = sellnumNow;
                }
            }

        }
    }
}
