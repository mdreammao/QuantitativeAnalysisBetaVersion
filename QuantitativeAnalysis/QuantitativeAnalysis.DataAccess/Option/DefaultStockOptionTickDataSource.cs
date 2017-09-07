using QuantitativeAnalysis.DataAccess.Infrastructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.DataAccess.Option
{
    public class DefaultStockOptionTickDataSource : IDataSource
    {
        private SqlServerReader sqlReader;
        public DefaultStockOptionTickDataSource(ConnectionType type)
        {
            sqlReader = new SqlServerReader(type);
        }
        public DataTable Get(string code, DateTime begin, DateTime end)
        {
            if (begin.Date != end.Date)
                throw new ArgumentException("开始时间和结束时间必须是同一天");
            var sqlStr = string.Format(@"SELECT [stkcd],convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) as tdatetime
 ,[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt],[OpenInterest]
  FROM [WindFullMarket{0}].[dbo].[MarketData_{1}]
  where convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) >='{2}'
   and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) <='{3}'",
   begin.ToString("yyyyMM"),code.Replace('.','_'),begin,end);
 //           var sqlStr = string.Format(@"SELECT rtrim([stkcd]) as [stkcd],stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.') as tdatetime
 //,[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt],[OpenInterest]
 // FROM [TradeMarket{0}].[dbo].[MarketData_{1}]
 // where ((ttime>=91500000 and ttime%10000000<=5959999 and ttime%100000<=59999) or (ttime<240000 and ttime>0)) and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) >='{2}'
 //  and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) <='{3}'",
 // begin.ToString("yyyyMM"), code.Replace('.', '_'), begin, end);
            return sqlReader.GetDataTable(sqlStr);
        }
    }
}
