using QuantitativeAnalysis.DataAccess.Infrastructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.DataAccess.ETF
{
    public class DefaultETFConstituentDailyDataSource : IDataSource
    {
        private WindReader windReader = new WindReader();
        //public DataTable Get(string code, DateTime date)
        //{
        //    return windReader.GetETFConstituentByDate(code, date);
        //}

        public DataTable Get(string code, DateTime begin, DateTime end)
        {
            throw new NotImplementedException();
        }
    }
}
