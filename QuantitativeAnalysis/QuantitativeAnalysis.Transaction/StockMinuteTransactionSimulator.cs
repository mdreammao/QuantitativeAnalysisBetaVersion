using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.Model;

namespace QuantitativeAnalysis.Transaction
{
    public class StockMinuteTransactionSimulator
    {
        private IStockRepository repo;
        private double simu_trading_ratio;
        public StockMinuteTransactionSimulator(IStockRepository repo,double simu_trading_ratio)
        {
            this.simu_trading_ratio = simu_trading_ratio;
            this.repo = repo;
        }

        public TransactionResult Trade(Signal s)
        {
            var result = new TransactionResult() { Signal = s };
            var history_transaction = repo.GetStockTransactionWithRedis(s.Code, s.StartTradingTime, s.StartTradingTime);
            double total_amount = 0;
            double total_volume = 0;
            foreach(var tr in history_transaction)
            {
                var average = tr.Amount / tr.Volume;
                switch(s.Type)
                {
                    case TradingType.Ask:
                        if( average>= s.Price)
                        {
                            total_volume += simu_trading_ratio * tr.Volume;
                            total_amount += simu_trading_ratio * tr.Amount;
                        }
                        break;
                    case TradingType.Bid:
                        if(average<=s.Price)
                        {
                            total_volume += simu_trading_ratio * tr.Volume;
                            total_amount += simu_trading_ratio * tr.Amount;
                        }
                        break;
                    default:
                        break;
                }
                if (total_volume >= tr.Volume)
                    break;
            }
            result.TradedAmount = total_amount;
            result.TradedVolume = total_volume;
            return result;
        }
    }
}
