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
        public static DataTable ToDataTableWithSingleColum(this WindData wData,string columnName)
        {
            var dt = new DataTable();
            dt.Columns.Add(columnName);
            var data = (object[])wData.data;
            data.ToList().ForEach(c => dt.Rows.Add(c));
            return dt;
        }

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
            dynamic arrayData = ConvertToArray(wData.data);
            //dc[0] = new DataColumn(CodeColumnName, typeof(string));
            //dc[1] = new DataColumn(DateTimeColumnName, typeof(DateTime));
            var dc = new DataColumn[colLength];
            for (int i = 0; i < colLength; ++i)
            {
                var re = arrayData[i];
                dc[i] = new DataColumn(colNames[i], re.GetType());
            }
            return dc;
        }

        private static dynamic ConvertToArray(object data)
        {
            var type = data.GetType();
            dynamic arrayData;
            if (type == typeof(object[]))
                arrayData = (object[])data;
            else if (type == typeof(double[]))
                arrayData = (double[])data;
            else if (type == typeof(int[]))
                arrayData = (int[])data;
            else throw new Exception("未实现类型转换！");
            return arrayData;
        }
        public static List<object[]> GetRowData(this WindData wData)
        {
            var rows = new List<object[]>();
            dynamic source = ConvertToArray(wData.data);
            var rowCount = wData.GetDataLength() / wData.GetFieldLength();
            for (int i = 0; i < rowCount; i++)
            {
                var row = new object[wData.GetFieldLength()];
                for(int j = 0; j < wData.GetFieldLength(); ++j)
                {
                    row[j] = source[j + i * wData.GetFieldLength()];
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
