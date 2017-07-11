using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Utilities;

namespace QuantitativeAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            var connStr = "server=(local);uid=sa;pwd=maoheng0;";
            var reader = new SqlServerReader(connStr);
            var dt = reader.GetDataTable("SELECT TOP 1000 * FROM [optionTickData].[dbo].[MarketData_10000001_SH]");
            //var reader = new WindReader();
            //var data = reader.GetDailyDataTable("000001.SZ", "open,high,low,close,volume,amt,vwap,adjfactor,trade_status", "2017-06-10".ToDateTime(), "2017-07-09".ToDateTime(), "");

        }
    }
}
