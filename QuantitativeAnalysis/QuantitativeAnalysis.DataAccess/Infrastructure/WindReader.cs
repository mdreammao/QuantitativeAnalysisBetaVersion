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
        //public DataTable GetETFConstituentByDate(string code,DateTime date)
        //{
        //    var wData = WindClientSingleton.Instance.wset("etfconstituent", string.Format(@"date={0};windcode={1}",date.ToString("yyyy-MM-dd"),code));
        //    return wData.ToDataTable();
        //}


        public DataTable GetSectorconstituentByDate(string code, DateTime date)
        {
            var wData = WindClientSingleton.Instance.wset("sectorconstituent", string.Format(@"date={0};windcode={1}", date.ToString("yyyy-MM-dd"), code));
            return wData.ToDataTable();
        }

        public DataTable GetDailyDataTable(string code, string fields, DateTime startTime, DateTime endTime, string options = "")
        {
            if (startTime > endTime) throw new Exception("开始时间不能大于结束时间。");
            var rawData = GetDailyData(code, fields, startTime, endTime, options);
            var dt = rawData.ToDataTable();
            AppendTitle(dt, rawData);
            return dt;
        }

        private void AppendTitle(DataTable dt, WindData rawData)
        {
            dt.Columns.Add("Code", typeof(string)).SetOrdinal(0);
            dt.Columns.Add("DateTime", typeof(DateTime)).SetOrdinal(1);
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var row = dt.Rows[i];
                row[0] = rawData.codeList[0];
                row[1] = rawData.timeList[i];
            }
        }

        public DataTable GetMinuteDataTable(string code, string fields, DateTime startTime, DateTime endTime, string options = "")
        {
            if (startTime > endTime) throw new Exception("开始时间不能大于结束时间。");
            var rawData = GetMinuteData(code, fields, startTime, endTime, options);
            var dt =rawData.ToDataTable();
            AppendTitle(dt, rawData);
            return dt;
        }

        public WindData GetDailyData(string code,string fields,DateTime startTime, DateTime endTime,string options = "")
        {
            var wData= WindClientSingleton.Instance.wsd(code, fields, startTime, endTime, options);
            if (wData.errorCode < 0)
                throw new Exception(string.Format("不能从Wind获取数据，ErrorCode:{0},ErrorMsg:{1}", wData.errorCode, WindClientSingleton.Instance.getErrorMsg(wData.errorCode)));
            return wData;
        }

        public WindData GetMinuteData(string code,string fields,DateTime startTime,DateTime endTime,string options = "")
        {
            var wData = WindClientSingleton.Instance.wsi(code, fields, startTime, endTime, options);
            if (wData.errorCode < 0)
                throw new Exception(string.Format("不能从Wind获取数据，ErrorCode:{0},ErrorMsg:{1}", wData.errorCode, WindClientSingleton.Instance.getErrorMsg(wData.errorCode)));
            return wData;
        }

        public WindData GetTransactionDate(DateTime startDate,DateTime endDate,string options="")
        {
            return WindClientSingleton.Instance.tdays(startDate, endDate, options);
        }
        public WindData GetDataSet(string reportName, string options)
        {
            var wData = WindClientSingleton.Instance.wset(reportName, options);
            if (wData.errorCode < 0)
                throw new Exception(string.Format("不能从Wind获取数据，ErrorCode:{0},ErrorMsg:{1}", wData.errorCode, WindClientSingleton.Instance.getErrorMsg(wData.errorCode)));
            return wData;
        }

        public DataTable GetDataSetTable(string reportName,string options)
        {
            return GetDataSet(reportName, options).ToDataTable();
        }
    }
}
