using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using WAPIWrapperCSharp;

namespace QuantitativeAnalysis.DataAccess
{
    public static class Extensions
    {
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
            var dc = new DataColumn[colLength];
            for (int i = 0; i < colLength; ++i)
            {
                var re = res[i];
                dc[i] = new DataColumn(colNames[i], re.GetType());
            }
            return dc;
        }

        public static List<object[]> GetRowData(this WindData wData)
        {
            var rows = new List<object[]>();
            var source = (object[])wData.data;
            for(int i = 0; i < wData.GetTimeLength(); i++)
            {
                var row = new object[wData.GetFieldLength()];
                for(int j = 0; j < row.Length; ++j)
                {
                    row[j] = source[j + i * row.Length];
                }
                rows.Add(row);
            }
            return rows;
        }
    }
}
