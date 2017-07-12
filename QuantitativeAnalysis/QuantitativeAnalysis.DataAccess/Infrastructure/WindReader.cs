using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WAPIWrapperCSharp;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class WindReader
    {
        public DataTable GetDailyDataTable(string code, string fields, DateTime startTime, DateTime endTime, string options = "")
        {
             return WindClientSingleton.Instance.wsd(code, fields, startTime, endTime, options).ToDataTable();
        }

        public DataTable GetMinuteDataTable(string code, string fields, DateTime startTime, DateTime endTime, string options = "")
        {
            return WindClientSingleton.Instance.wsi(code, fields, startTime, endTime, options).ToDataTable();
        }

        public WindData GetDailyData(string code,string fields,DateTime startTime, DateTime endTime,string options = "")
        {
            return WindClientSingleton.Instance.wsd(code, fields, startTime, endTime, options);
        }

        public WindData GetMinuteData(string code,string fields,DateTime startTime,DateTime endTime,string options = "")
        {
            return WindClientSingleton.Instance.wsi(code, fields, startTime, endTime, options);
        }
    }
}
