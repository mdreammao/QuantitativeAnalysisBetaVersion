using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Data;

namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class StockInfoRepository
    {
        private WindReader windReader;
        private SqlServerWriter sqlWriter;
        private TransactionDateTimeRepository dateRepo;
        public StockInfoRepository(ConnectionType type)
        {
            windReader = new WindReader();
            sqlWriter = new SqlServerWriter(type);
            dateRepo = new TransactionDateTimeRepository(type);
        }

        public void UpdateStockInfoToNow()
        {
            var codes =GetCodes();
            var latestTradingDate = dateRepo.GetPreviousTransactionDate(DateTime.Now.AddDays(1));
            var dt = CreateDataTable();
            foreach(string code in codes)
            {
                var rawData = windReader.GetDailyData(code, "sec_name,ipo_date,delist_date", latestTradingDate, latestTradingDate);
                var info = (object[])rawData.data;
                dt.Rows.Add(new object[] { code, info[0],info[1],info[2] });
            }
            InsertToSql(dt);
        }


        private void InsertToSql(DataTable dt)
        {
            ClearStockInfoInDB();
            sqlWriter.InsertBulk(dt, "[Common].[dbo].[StockInfo]");
        }

        private void ClearStockInfoInDB()
        {
            var sql = "delete from [Common].[dbo].[StockInfo]";
            sqlWriter.WriteChanges(sql);
        }

        private List<string> GetCodes()
        {
            var listingCodes = GetListingCodes();
            var delistedCodes = GetDelistedCodes();
            var allCodes = new List<string>(listingCodes.Count + delistedCodes.Count);
            allCodes.AddRange(listingCodes);
            allCodes.AddRange(delistedCodes);
            return allCodes;
        }

        private DataTable CreateDataTable()
        {
            var dt = new DataTable();
            dt.Columns.Add("Code", typeof(string));
            dt.Columns.Add("SecName", typeof(string));
            dt.Columns.Add("IPODate", typeof(DateTime));
            dt.Columns.Add("DelistDate", typeof(DateTime));
            return dt;
        }

        private List<string> GetDelistedCodes()
        {
            var latestTradingDate = dateRepo.GetPreviousTransactionDate(DateTime.Now.AddDays(1));
            var options = "date = " + latestTradingDate.ToString("yyyy-MM-dd") + ";sectorid=a001010m00000000; field = wind_code";
            var rawData = windReader.GetDataSet("sectorconstituent", options);
            return rawData.ToList<string>();
        }

        private List<string> GetListingCodes()
        {
            var latestTradingDate = dateRepo.GetPreviousTransactionDate(DateTime.Now.AddDays(1));
            var options = "date = "+latestTradingDate.ToString("yyyy-MM-dd")+";sectorid = a001010100000000; field = wind_code";
            var rawData = windReader.GetDataSet("sectorconstituent", options);
            return rawData.ToList<string>();
        }
    }
}
