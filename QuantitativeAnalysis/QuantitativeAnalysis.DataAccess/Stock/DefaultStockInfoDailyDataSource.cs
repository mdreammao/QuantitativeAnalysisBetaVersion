using QuantitativeAnalysis.DataAccess.Infrastructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class DefaultStockInfoDailyDataSource : IDataSource
    {
        private WindReader windReader = new WindReader();
        public DataTable Get(string code, DateTime begin, DateTime end)
        {
            return windReader.GetDailyDataTable(code, "sec_name,industry_sw,industry_swcode,ipo_date,mrg_long_bal,mrg_short_bal,mfd_buyamt_d,mfd_sellamt_d,mfd_volinflowproportion_m,mkt_cap_ard,pe_ttm,pb_lf,ps_ttm", begin, end);
        }

        public DataTable GetFromSpecializedSQLServer(string code, DateTime date, ConnectionType type)
        {
            throw new NotImplementedException();
        }
    }
}
