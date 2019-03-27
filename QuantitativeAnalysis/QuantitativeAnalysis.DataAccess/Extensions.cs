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

        public static List<T> ToList<T>(this WindData wData)
        {
            var result = new List<T>();
            var data = (object[])wData.data;
            foreach(var item in data)
                result.Add((T)Convert.ChangeType(item, typeof(T)));
            return result;
        }
        public static DataTable ToDataTable(this WindData wData)
        {
            var dt = new DataTable();
            dynamic source = ConvertToArray(wData.data);
            if (wData != null && wData.GetDataLength()>0)
            {
                var columnTypes = wData.GetDataColumns();
                dt.Columns.AddRange(columnTypes);
                dt.BeginLoadData();
                wData.GetRowData(columnTypes).ForEach(c => dt.LoadDataRow(c, false));
                dt.EndLoadData();
            }

            return dt;
        }

        public static DataColumn[] GetDataColumns(this WindData wData)
        {
            var colNames = wData.fieldList;
            var colLength = colNames.Length;
            dynamic arrayData = ConvertToArray(wData.data);
            var dc = new DataColumn[colLength];
            bool success = true;
            int k = 0;
            try
            {
                while (k <= 10000)
                {
                    for (int i = k * colLength; i < (k + 1) * colLength; ++i)
                    {
                        success = true;
                        var re = arrayData[i];
                        if (re.GetType() == typeof(System.DBNull))
                        {
                            success = false;
                            break;
                        }
                    }
                    if (success == true)
                    {
                        for (int i = k * colLength; i < (k + 1) * colLength; ++i)
                        {
                            var re = arrayData[i];
                            dc[i - k * colLength] = new DataColumn(colNames[i - k * colLength], re.GetType());
                        }
                        break;
                    }
                    k++;
                }
            }
            catch (Exception e)
            {
                if (success == false)
                {
                    Console.WriteLine("数据缺失，存在DBNULL类型！");
                    for (int i = 0; i < colLength; ++i)
                    {
                        var re = arrayData[i];
                        if (re.GetType() == typeof(System.DBNull))
                        {
                            dc[i] = new DataColumn(colNames[i], typeof(String));
                        }
                        else
                        {
                            dc[i] = new DataColumn(colNames[i], re.GetType());
                        }
                    }
                }
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
        public static List<object[]> GetRowData(this WindData wData, DataColumn[] columnTypes)
        {
            var rows = new List<object[]>();
            dynamic source = ConvertToArray(wData.data);
            var rowCount = wData.GetDataLength() / wData.GetFieldLength();
            for (int i = 0; i < rowCount; i++)
            {
                var row = new object[wData.GetFieldLength()];
                for(int j = 0; j < wData.GetFieldLength(); ++j)
                {
                    if (source[j + i * wData.GetFieldLength()]==null)
                    {
                        if (columnTypes[j].DataType == typeof(String))
                        {
                            row[j] = "";
                        }
                        else if (columnTypes[j].DataType == typeof(double))
                        {
                            row[j] = 0;
                        }
                        else if (columnTypes[j].DataType == typeof(DateTime))
                        {
                            row[j] = new DateTime();
                        }
                    }
                    else
                    {
                        if (source[j + i * wData.GetFieldLength()].GetType() == typeof(System.DBNull))
                        {
                            if (columnTypes[j].DataType == typeof(String))
                            {
                                row[j] = "数据缺失";
                            }
                            else
                            {
                                row[j] = -1;
                            }
                        }
                        else
                        {
                            row[j] = source[j + i * wData.GetFieldLength()];
                        }
                    }
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
