using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Configuration;
using QuantitativeAnalysis.Utilities;
using System.IO;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class TransactionDateTimeRepository
    {
        public TransactionDateTimeRepository(ConnectionType type)
        {
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
        }
        private SqlServerReader sqlReader;
        private SqlServerWriter sqlWriter;
        private WindReader windReader = new WindReader();
        public List<DateTime> GetStockTransactionDate(DateTime start,DateTime end)
        {
            for(int year=start.Year; year <= end.Year;year++)
            {
                var existed = sqlReader.ExecuteScalar<int>(string.Format("select 1 from [Common].[dbo].[TransactionDate] where datetime >= '{0}-01-01' and datetime<='{0}-12-31'", year)) > 0;
                if(!existed)
                {
                    var res = windReader.GetTransactionDate(new DateTime(year,1,1), new DateTime(year,12,31)).ToDataTableWithSingleColum("DateTime");
                    sqlWriter.InsertBulk(res, "[Common].[dbo].[TransactionDate]");
                }

            }
            return FetchTransactionDateFromSql(start, end);
        }
        public DateTime GetLastTransactionDate(DateTime current,DateLevel level)
        {
            switch (level)
            {
                case DateLevel.Month:
                    var lastDayOfMonth = new DateTime(current.Year, current.Month + 1, 1).AddDays(-1);
                    return GetStockTransactionDate(lastDayOfMonth.AddDays(-10), lastDayOfMonth).Max();
                case DateLevel.Year:
                    var lastDayOfYear = new DateTime(current.Year + 1, 1, 1).AddDays(-1);
                    return GetStockTransactionDate(lastDayOfYear.AddDays(-10), lastDayOfYear).Max();
                default:
                    throw new Exception("参数错误！");
            }
        }

        public DateTime GetFirstTransactionDate(DateTime current,DateLevel level)
        {
            switch (level)
            {
                case DateLevel.Month:
                    var startOfMonth = new DateTime(current.Year, current.Month, 1);
                    return GetStockTransactionDate(startOfMonth, startOfMonth.AddDays(10)).Min();
                case DateLevel.Year:
                    var startOfYear = new DateTime(current.Year, 1, 1);
                    return GetStockTransactionDate(startOfYear, startOfYear.AddDays(10)).Min();
                default:
                    throw new Exception("参数错误！");
            }
        }

        public DateTime GetNextTransactionDate(DateTime current)
        {
            var start = current.AddDays(1);
            return GetStockTransactionDate(start, start.AddDays(10)).Min();
        }

        public DateTime GetPreviousTransactionDate(DateTime current)
        {
            var end = current.AddDays(-1);
            return GetStockTransactionDate(end.AddDays(-10), end).Max();
        }

        private List<DateTime> FetchTransactionDateFromSql(DateTime start, DateTime end)
        {
            var sqlStr = string.Format("select DateTime from [Common].[dbo].[TransactionDate] where datetime>='{0}'and datetime<='{1}'", start.ToShortDateString(), end.ToShortDateString());
            var res = sqlReader.GetDataTable(sqlStr);
            return res.ToList<DateTime>();
        }



    }

    public enum DateLevel
    {
        Month,
        Year
    }
}
