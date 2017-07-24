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
            if (startTime > endTime) throw new Exception("开始时间不能大于结束时间。");
            return GetDailyData(code,fields,startTime,endTime,options).ToDataTable();
        }

        public DataTable GetMinuteDataTable(string code, string fields, DateTime startTime, DateTime endTime, string options = "")
        {
            if (startTime > endTime) throw new Exception("开始时间不能大于结束时间。");
            return GetMinuteData(code, fields, startTime, endTime, options).ToDataTable();
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
    }
}
