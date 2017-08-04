using QuantitativeAnalysis.DataAccess.Infrastructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class DefaultStockMinuteDataSource : IDataSource
    {
        private WindReader windReader = new WindReader();
        
        public DataTable Get(string code, DateTime begin,DateTime end)
        {
            return windReader.GetMinuteDataTable(code, "open,high,low,close,volume,amt", begin, end, "Fill=Previous");
        }
    }
}
