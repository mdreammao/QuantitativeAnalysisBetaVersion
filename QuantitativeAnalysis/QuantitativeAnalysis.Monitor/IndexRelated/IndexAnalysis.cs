using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Utilities;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.DataAccess.Option;
using QuantitativeAnalysis;
using NLog;
using Autofac;
using QuantitativeAnalysis.Transaction;
using System.Data;
using System.Configuration;
using ExcelDataReader;
using System.IO;

namespace QuantitativeAnalysis.Monitor.IndexRelated
{
    public class IndexAnalysis
    {
        private TransactionDateTimeRepository dateRepo;
        private WindReader windReader;
        private DateTime date;

        public IndexAnalysis(TransactionDateTimeRepository dateRepo, DateTime date)
        {
            this.dateRepo = dateRepo;
            this.date = date;
            this.windReader = new WindReader();
        }



        public List<StockCode> differ(string etf,string index)
        {
            List<StockCode> list = new List<StockCode>();
            var etfList = getETFStocks(date, etf);
            var indexList = getIndexStocks(date, index);
            foreach (var etfName in etfList)
            {
                if (indexList.Contains(etfName.code)==false)
                {
                    list.Add(etfName);
                }
            }
            if (list.Count!=0)
            {
                var dt = DataTableExtension.ToDataTable(list);
                var etfStr = etf.Split('.');
                var indexStr = index.Split('.');
                string name = string.Format("E:\\result\\IndexAnalysis\\differ_{0}_{1}.csv", etfStr[0], indexStr[0]);
                saveToCSV(dt, name);
            }
            return list;
        }

        private void saveToCSV(DataTable dt,string filename)
        {
           
            DataTableExtension.SaveCSV(dt, filename);
        }



        private List<string> getIndexStocks(DateTime date, string index)
        {
            var rawData = windReader.GetDataSetTable("sectorconstituent", string.Format("date={0};windcode={1}", date.Date, index));
            List<string> codeList = new List<string>();
            foreach (DataRow dr in rawData.Rows)
            {
                codeList.Add(Convert.ToString(dr[1]));
            }
            return codeList;
        }


        private List<StockCode> getETFStocks(DateTime date, string index)
        {
            var rawData = windReader.GetDataSetTable("etfconstituent", string.Format("date={0};windcode={1};field=wind_code,sec_name", date.Date, index));
            List<StockCode> codeList = new List<StockCode>();
            foreach (DataRow dr in rawData.Rows)
            {
                StockCode stock = new StockCode();
                stock.code = Convert.ToString(dr[0]);
                stock.name = Convert.ToString(dr[1]);
                codeList.Add(stock);
            }
            return codeList;
        }

    }
}
