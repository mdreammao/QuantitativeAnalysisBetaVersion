using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using StackExchange.Redis;
using System.Data.SqlClient;
using QuantitativeAnalysis.Utilities;
using System.Data;

namespace QuantitativeAnalysis.DataAccess
{
    public class StockDailyRepository : IStockRepository
    {
        private const string DailyKeyFormat = "{0}-{1}";
        private RedisReader redisReader = new RedisReader();
        private SqlServerReader sqlReader = new SqlServerReader(Infrastructure.ConnectionType.Local);
        private WindReader windReader = new WindReader();
        private SqlServerWriter sqlWriter = new SqlServerWriter(Infrastructure.ConnectionType.Local);
        public List<StockTransaction> GetStockTransaction(string code, DateTime begin,DateTime end)
        {
            var stocks = new List<StockTransaction>();
            var tradingDates = windReader.GetTransactionDate(begin, end);
            if (tradingDates != null && tradingDates.Count > 0)
            {
                for (var start = tradingDates[0]; start.Date <= tradingDates[tradingDates.Count-1].Date; start = start.AddDays(1))
                {
                    var key = string.Format(DailyKeyFormat, code, start.ToString("yyyy-MM-dd"));
                    var values = redisReader.HGetAll(key);
                    var trans = ConvertToStockTransaction(code, start, values);
                    if (trans == null)//just run once 
                    {
                        var existedDateInSql = GetExistedDateInSql(code, tradingDates.First(), tradingDates.Last());
                        var nonExistedDateIntervalnInSql = Computor.GetNoExistedInterval<DateTime>(tradingDates, existedDateInSql);
                        LoadStockTransactionToSqlFromWind(code, nonExistedDateIntervalnInSql);
                        var existedDateInRedis = GetExistedDateInRedis(code, tradingDates.First(), tradingDates.Last());
                        var nonExistedDateIntervalnInRedis = Computor.GetNoExistedInterval<DateTime>(tradingDates, existedDateInRedis);
                        LoadStockTransactionToRedisFromSql(code, nonExistedDateIntervalnInRedis);
                    }
                }
            }
            return stocks;
        }

        private void LoadStockTransactionToRedisFromSql(string code, List<KeyValuePair<DateTime, DateTime>> nonExistedDateIntervalnInRedis)
        {
            if(nonExistedDateIntervalnInRedis !=null && nonExistedDateIntervalnInRedis.Count > 0)
            {
                string sqlStr = GenerateSqlString(code, nonExistedDateIntervalnInRedis);
                var dt = sqlReader.GetDataTable(sqlStr);
                WriteToRedis(code,dt);
            }
        }

        private void WriteToRedis(string code, DataTable dt)
        {
            throw new NotImplementedException();
        }

        private string GenerateSqlString(string code, List<KeyValuePair<DateTime, DateTime>> nonExistedDateIntervalnInRedis)
        {
            var sqlStr = "select DATETIME,OPEN,HIGH,LOW,CLOSE,VOLUME,AMT,ADJFACTOR,TRADE_STATUS from [DailyTransaction].[dbo].[Stock] where Code={0} and {1};";
            var dateConditions = new StringBuilder();
            foreach (var pair in nonExistedDateIntervalnInRedis)
            {
                string dateCondition;
                if(pair.Key==pair.Value)
                    dateCondition = string.Format("datetime='{0}'", pair.Value);
                else
                    dateCondition = string.Format("datetime>='{0}' and datetime<='{1}'", pair.Key, pair.Value);
                if (dateConditions.Length > 0)
                    dateConditions.Append(" or " + dateCondition);
                else
                    dateConditions.Append(dateCondition);
            }
            return string.Format(sqlStr,code,dateConditions.ToString());
        }

        private List<DateTime> GetExistedDateInRedis(string code, DateTime begin, DateTime end)
        {
            var allExistedInRedis = redisReader.HGetAllFields(code).ConvertTo<DateTime>();
            return allExistedInRedis.Where(c=>c>=begin && c<=end).ToList();
        }

        private void LoadStockTransactionToSqlFromWind(string code, List<KeyValuePair<DateTime, DateTime>> intervals)
        {
            foreach(var item in intervals)
            {
                var dt = windReader.GetDailyDataTable(code, "open,high,low,close,volume,amt,adjfactor,trade_status", item.Key, item.Value);
                sqlWriter.InsertBulk(dt, "[DailyTransaction].[dbo].[Stock]");
            }
        }

        private List<DateTime> GetExistedDateInSql(string code, DateTime start, DateTime end)
        {
            var sqlStr = "select DateTime from [DailyTransaction].[dbo].[Stock] where Code='@code' and DateTime>=@start and DateTime<=@end";
            var pars = new SqlParameter[]
            {
                new SqlParameter("@code",code),
                new SqlParameter("@start",start.Date),
                new SqlParameter("@end",end.Date)
            };
            return sqlReader.GetDataTable(sqlStr, pars).ToList<DateTime>();
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
        
        private void GetStockDailyTransactionFromSqlServer(string code,DateTime begin,DateTime end)
        {
            var sqlString = "select * from [DailyTransaction].[dbo].[Stock] where code='@code' and DateTime>=@begin and DateTime<=@end";
            var pars = new SqlParameter[]
            {
                new SqlParameter("@code",code),
                new SqlParameter("@begin",begin.Date),
                new SqlParameter("@end",end.Date)
            };
            var res=sqlReader.GetDataTable(sqlString,pars);
        }
        
    }
}
