using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using WAPIWrapperCSharp;
using QuantitativeAnalysis.Utilities;
using StackExchange.Redis;

namespace QuantitativeAnalysis.DataAccess
{
    public static class Extensions
    {
        private static readonly string CodeColumnName = "Code";
        private static readonly string DateTimeColumnName = "DateTime";

        public static DataTable ToDataTable(this WindData wData)
        {
            var dt = new DataTable();
            if (wData != null && wData.GetDataLength()>0)
            {
                dt.Columns.AddRange(wData.GetDataColumns());
                dt.BeginLoadData();
                wData.GetRowData().ForEach(c => dt.LoadDataRow(c, false));
                dt.EndLoadData();
            }
            return dt;
        }

        public static DataColumn[] GetDataColumns(this WindData wData)
        {
            var colNames = wData.fieldList;
            var colLength = colNames.Length;
            var res = (object[])wData.data;
            var dc = new DataColumn[colLength+2];
            dc[0] = new DataColumn(CodeColumnName, typeof(string));
            dc[1] = new DataColumn(DateTimeColumnName, typeof(DateTime));
            for (int i = 0; i < colLength; ++i)
            {
                var re = res[i];
                dc[i+2] = new DataColumn(colNames[i], re.GetType());
            }
            return dc;
        }

        public static List<object[]> GetRowData(this WindData wData)
        {
            var rows = new List<object[]>();
            var source = (object[])wData.data;
            for(int i = 0; i < wData.GetTimeLength(); i++)
            {
                var row = new object[wData.GetFieldLength()+2];
                row[0] = wData.codeList[0];//需优化
                row[1] = wData.timeList[i];
                for(int j = 0; j < wData.GetFieldLength(); ++j)
                {
                    row[j+2] = source[j + i * wData.GetFieldLength()];
                }
                rows.Add(row);
            }
            return rows;
        }

        public static List<DateTime> ToDateTimes(this WindData wData)
        {
            var list = new List<DateTime>();
            var data = (object[])wData.data;
            foreach (var item in data)
                list.Add(item.ToString().ToDateTime());
            return list;
        }

        public static T ConvertTo<T>(this HashEntry[] entries,string fieldName)
        {
            var res =(entries.First(c => c.Name.ToString().Equals(fieldName)).Value);
            return (T)Convert.ChangeType(res, typeof(T));
        }

        public static List<T> ConvertTo<T>(this RedisValue[] values)
        {
            return values.Select(c => (T)Convert.ChangeType(c.ToString(), typeof(T))).ToList();
        }
    }

}
