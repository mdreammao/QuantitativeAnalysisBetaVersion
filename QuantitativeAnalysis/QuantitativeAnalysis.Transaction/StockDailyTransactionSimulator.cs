using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.DataAccess.Infrastructure;

namespace QuantitativeAnalysis.Transaction
{
    public class StockDailyTransactionSimulator
    {
        IStockRepository daily_repo;
        private double slippage;
        private double ratio_of_market_volume;
        public StockDailyTransactionSimulator(IStockRepository daily_repo,double slippage, double ratio)
        {
            this.daily_repo = daily_repo;
            this.slippage = slippage;
            ratio_of_market_volume = ratio;
        }
        public TransactionResult TradeByAverage(Signal s)
        {
            var result = new TransactionResult() { Signal = s };
            var daily_trans = daily_repo.GetStockTransaction(s.Code, s.StartTradingTime.Date, s.StartTradingTime.Date);
            if (daily_trans != null && daily_trans.Count > 0)
            {
                var daily_item = daily_trans.First();
                var average = daily_item.Amount / daily_item.Volume / 100;
                switch (s.Type)
                {
                    case TradingType.Ask:
                        if (average <= s.Price)
                        {
                            result.TradedVolume = daily_item.Volume * ratio_of_market_volume < s.Volume ? daily_item.Volume * ratio_of_market_volume : s.Volume;
                            result.TradedAmount = result.TradedVolume * (1 + slippage) * average;
                        }
                        break;
                    case TradingType.Bid:
                        if (average >= s.Price)
                        {
                            result.TradedVolume = daily_item.Volume * ratio_of_market_volume < s.Volume ? daily_item.Volume * ratio_of_market_volume : s.Volume;
                            result.TradedAmount = result.TradedVolume * (1 + slippage) * average;
                        }
                        break;
                    default:
                        break;
                }
            }
            else
                throw new Exception("没有获取到数据！");
            return result;
        }
    }
}
