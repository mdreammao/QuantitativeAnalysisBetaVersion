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
        public StockDailyTransactionSimulator(IStockRepository daily_repo)
        {
            this.daily_repo = daily_repo;
        }
        public TransactionResult Trade(Signal s)
        {
            var daily_trans = daily_repo.GetStockTransaction(s.Code, s.StartTradingTime.Date, s.StartTradingTime.Date);
            if(daily_trans != null && daily_trans.Count > 0)
            {
                var daily_item = daily_trans.First();
                switch(s.Type)
                {
                    case TradingType.Ask:

                        break;
                    case TradingType.Bid:

                        break;
                    default:
                        break;
                }
            }
            return null;
        }
    }
}
