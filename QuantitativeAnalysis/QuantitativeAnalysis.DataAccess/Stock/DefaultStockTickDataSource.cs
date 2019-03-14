using QuantitativeAnalysis.DataAccess.Infrastructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class DefaultStockTickDataSource : IDataSource
    {
        private SqlServerReader sqlReader;
        public DefaultStockTickDataSource(ConnectionType type)
        {
            sqlReader = new SqlServerReader(type);
        }
        public DataTable Get(string code, DateTime begin, DateTime end)
        {

            if (begin.Date != end.Date)
                throw new ArgumentException("开始时间和结束时间必须是同一天");
            var sqlStr = string.Format(@"SELECT [stkcd],convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) as tdatetime
 ,[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt],[HighLimit],[LowLimit]
  FROM [WindFullMarket{0}].[dbo].[MarketData_{1}]
  where ((ttime>=91500000 and ttime<=113000000) or (ttime>=130000000 and ttime<=150100000)) and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) >='{2}'
   and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) <='{3}'",
   begin.ToString("yyyyMM"), code.Replace('.', '_'), begin, end);
            if (begin.Date<=new DateTime(2011,07,31)) //公司数据库在2011年8月1日开始改变了ttime字段的格式
            {
                sqlStr = string.Format(@"SELECT [stkcd],convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(rtrim(ttime),3,0,':'),6,0,':')) as tdatetime
 ,[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt]
  FROM [WindFullMarket{0}].[dbo].[MarketData_{1}]
  where ((ttime>=91500 and ttime<=113000) or (ttime>=130000 and ttime<=150100)) and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(rtrim(ttime),3,0,':'),6,0,':')) >='{2}'
   and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(rtrim(ttime),3,0,':'),6,0,':')) <='{3}'",
   begin.ToString("yyyyMM"), code.Replace('.', '_'), begin, end);
            }
            if (begin.Date >= new DateTime(2012, 03, 01) && begin.Date <= new DateTime(2012, 05, 31))
            {
                sqlStr = string.Format(@"SELECT [stkcd],convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(rtrim(substring(ttime,0,7)),3,0,':'),6,0,':')) as tdatetime
 ,[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt],[HighLimit],[LowLimit]
  FROM [WindFullMarket{0}].[dbo].[MarketData_{1}]
  where ((substring(ttime,0,7)>=91500 and substring(ttime,0,7)<=113000) or (substring(ttime,0,7)>=130000 and substring(ttime,0,7)<=150100)) and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(rtrim(substring(ttime,0,7)),3,0,':'),6,0,':')) >='{2}'
   and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(rtrim(substring(ttime,0,7)),3,0,':'),6,0,':')) <='{3}'",
   begin.ToString("yyyyMM"), code.Replace('.', '_'), begin, end);
            }
            return sqlReader.GetDataTable(sqlStr);
        }

        public DataTable GetFromSpecializedSQLServer(string code, DateTime date, ConnectionType type)
        {
            var sqlReader = new SqlServerReader(type);
            var sqlStr = string.Format(@"SELECT [stkcd],[tdatetime],[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt],[HighLimit],[LowLimit]
  FROM [StockTickTransaction{0}].[dbo].[{1}]",
   date.ToString("yyyy"), date.ToString("yyyy-MM-dd"));
            return sqlReader.GetDataTable(sqlStr);
        }
    }
}


//           var sqlStr = string.Format(@"SELECT (left(rtrim([code]),6)+'.SH') as [stkcd],stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+reverse(stuff(stuff(stuff(reverse(rtrim(ttime)),4,0,'.'),7,0,':'),10,0,':')) as tdatetime
//,[lastprice] as [cp],[ask1] as [S1],[ask2] as [S2],[ask3] as [S3],[ask4] as [S4],[ask5] as [S5],[bid1] as [B1],[bid2] as [B2],[bid3] as [B3],[bid4] as [B4],[bid5] as [B5],[askv1] as [SV1],[askv2] as [SV2],[askv3] as [SV3],[askv4] as [SV4],[askv5] as [SV5],[bidv1] as [BV1],[bidv2] as [BV2],[bidv3] as [BV3],[bidv4] as [BV4],[bidv5] as [BV5],[volume] as [ts],[amount] as [tt]
// FROM [TickData_50ETFOption].[dbo].[MarketData_{1}]
//   where ((ttime>=93000000 and ttime<=113000000 and ttime%10000000<=5959999 and ttime%100000<=59999 ) or (ttime>=130000000 and ttime<=150000000 and ttime%10000000<=5959999 and ttime%100000<=59999 )) and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+reverse(stuff(stuff(stuff(reverse(rtrim(ttime)),4,0,'.'),7,0,':'),10,0,':'))) >='{2}'
//            and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+reverse(stuff(stuff(stuff(reverse(rtrim(ttime)),4,0,'.'),7,0,':'),10,0,':'))) <='{3}'",
// begin.ToString("yyyyMM"), code.Replace('.', '_'), begin, end);
//           var sqlStr = string.Format(@"SELECT rtrim('{4}') as [stkcd],stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.') as tdatetime
//,[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt]
// FROM [TradeMarket{0}].[dbo].[MarketData_{1}]
// where ((ttime>=91500000 and ttime%10000000<=5959999 and ttime%100000<=59999) or (ttime<240000 and ttime>0)) and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) >='{2}'
//  and convert(datetime,stuff(stuff(rtrim(tdate),5,0,'-'),8,0,'-')+' '+stuff(stuff(stuff(rtrim(ttime),3,0,':'),6,0,':'),9,0,'.')) <='{3}'",
// begin.ToString("yyyyMM"), code.Replace('.', '_'), begin, end,code.ToUpper());