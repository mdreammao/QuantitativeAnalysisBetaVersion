using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using StackExchange.Redis;

namespace QuantitativeAnalysis.DataAccess
{
    public class StockRepository //: IStockRepository
    {
        private const string DailyKeyFormat = "{0}-{1}";
        private RedisReader reader = new RedisReader();

        public StockTransaction GetStockDaily(string code, DateTime date)
        {
            var key = string.Format(DailyKeyFormat, code, date.ToString("yyyy-MM-dd"));
            var values = reader.HGetAll(key);
            return ConvertToStockTransaction(code,date,values);
        }

        private StockTransaction ConvertToStockTransaction(string code,DateTime date, HashEntry[] entries)
        {
            if (entries == null || entries.Length==0)
                return null;
            return new StockTransaction
            {
                Code = code,
                DateTime = date.Date,
                Open = entries.ConvertTo<double>(StockTransaction.OpenName),
                High = entries.ConvertTo<double>(StockTransaction.HighName),
                Low = entries.ConvertTo<double>(StockTransaction.LowName),
                Close = entries.ConvertTo<double>(StockTransaction.CloseName),
                Volume = entries.ConvertTo<double>(StockTransaction.VolumeName),
                Amount = entries.ConvertTo<double>(StockTransaction.AmountName),
                AdjFactor = entries.ConvertTo<double>(StockTransaction.AdjFactorName),
                TradeStatus = entries.ConvertTo<string>( StockTransaction.TradeStatusName)
            };
        }
        
        
        public StockTransaction GetStockMinuteTransaction(string code, DateTime dateTime)
        {
            throw new NotImplementedException();
        }
    }
}
