using QuantitativeAnalysis.DataAccess.Infrastructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class DefaultStockDailyDataSource : IDataSource
    {
        private WindReader windReader = new WindReader();
        public DataTable Get(string code, DateTime begin, DateTime end)
        {
            return windReader.GetDailyDataTable(code, "open,high,low,close,volume,amt,adjfactor,trade_status", begin,end);
        }

        public DataTable GetFromSpecializedSQLServer(string code, DateTime date, ConnectionType type)
        {
            throw new NotImplementedException();
        }
    }
}
