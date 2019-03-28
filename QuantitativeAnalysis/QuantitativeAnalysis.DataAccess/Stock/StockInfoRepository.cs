using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Model;
using System.Data;
using NLog;


namespace QuantitativeAnalysis.DataAccess.Stock
{
    public class StockInfoRepository
    {
        private WindReader windReader;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private TransactionDateTimeRepository dateRepo;
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();
        public StockInfoRepository(Infrastructure.ConnectionType type)
        {
            windReader = new WindReader();
            sqlWriter = new SqlServerWriter(type);
            sqlReader = new SqlServerReader(type);
            dateRepo = new TransactionDateTimeRepository(type);
        }

        public void UpdateStockInfoToNow()
        {
            var codes =GetCodes();
            var latestTradingDate = dateRepo.GetPreviousTransactionDate(DateTime.Now.AddDays(1));
            var dt = CreateIPODataTable();
            var IPOInfoList = GetStockListInfoFromSql();
            foreach(string code in codes)
            {
                StockIPOInfo infoObsolete = new StockIPOInfo();
                foreach (var item in IPOInfoList)
                {
                    if (item.code==code)
                    {
                        infoObsolete = item;
                    }
                    if (infoObsolete.code!=null)
                    {
                        continue;
                    }
                }
                bool needUpdate = false;
                if (infoObsolete.code==null)
                {
                    needUpdate = true;
                }
                else
                {
                    var span = latestTradingDate - infoObsolete.updateTime;
                    if (span.TotalDays>100)
                    {
                        needUpdate = true;  
                    }
                }
                //Console.WriteLine("code:{0}, needupdate?{1}", code, needUpdate);            
                if (needUpdate == true)
                {
                    var rawData = windReader.GetDailyData(code, "sec_name,ipo_date,delist_date", latestTradingDate, latestTradingDate);
                    var info = (object[])rawData.data;
                    dt.Rows.Add(new object[] { code, info[0], info[1], info[2] });
                }
            }
            InsertToSql(dt);
        }

        //从本地数据库获取股票上市退市信息
        public List<StockIPOInfo> GetStockListInfoFromSql()
        {
            List<StockIPOInfo> stockInfos = new List<StockIPOInfo>();
            var sqlStr = "select [Code],[SecName],[IPODate],[DelistDate],[UpdateDateTime] from [Common].[dbo].[StockInfo]";
            DataTable dt = new DataTable();
            try
            {
                dt = sqlReader.GetDataTable(sqlStr);
            }
            catch
            {
                logger.Warn(string.Format("There is no IPO information from specialized sqlserver!!!"));
            }
            foreach (DataRow dr in dt.Rows)
            {
                StockIPOInfo info = new StockIPOInfo();
                info.code = Convert.ToString(dr[0]);
                info.name = Convert.ToString(dr[1]);
                info.IPODate = Convert.ToDateTime(dr[2]);
                if (dr[3]!=DBNull.Value)
                {
                    info.DelistDate = Convert.ToDateTime(dr[3]);
                }
                else
                {
                    info.DelistDate = new DateTime(2099,12,31);
                }
                info.updateTime = Convert.ToDateTime(dr[4]);
                stockInfos.Add(info);
            }
            return stockInfos;
        }

        public DataTable UpdateStockBonusDataFromWind(List<string> stockCode)
        {
            var list = GetStockListInfoFromSql();
            var dt = CreateBonusDataTable();
            var updateDt = CreateBonusDataTable();
            var bonusList = GetStockBonusListFromSql();
            int number = 0;
            foreach (var item in list)
            {
                if (stockCode.Contains(item.code)==false)
                {
                    continue;
                }
                DateTime startDate = item.IPODate.Date;
                DateTime endDate = DateTime.Now.Date.AddYears(1);
                bool needToUpdate = false;
                if (item.DelistDate != null && item.DelistDate < DateTime.Now.Date)
                {
                    endDate = item.DelistDate;
                }
                var bonusLatest = GetLatestStockBonusByCodeFromSql(bonusList, item.code);
                if (bonusLatest.code == null)
                {
                    needToUpdate = true;
                }
                else
                {
                    DateTime updateTime = bonusLatest.updateTime;
                    double lastUpdateDays = (DateTime.Now.Date - updateTime.Date).TotalDays;

                    double lastDividendDays = (DateTime.Now.Date - bonusLatest.exDividendDate.Date).TotalDays;
                    if (lastUpdateDays >= 1 && item.DelistDate > DateTime.Now.Date)
                    {
                        needToUpdate = true;
                    }
                    if (item.DelistDate <= DateTime.Now.Date && updateTime <= item.DelistDate)
                    {
                        needToUpdate = true;
                    }
                    startDate = bonusLatest.exDividendDate.AddDays(1);
                    if (startDate < updateTime)
                    {
                        startDate = updateTime;
                    }
                }
                if (endDate < startDate)
                {
                    endDate = startDate;
                }
                if (needToUpdate == true)
                {
                    var rawData = windReader.GetDataSet("corporationaction", string.Format("startdate={1};enddate={2};windcode={0}", item.code, startDate, endDate));
                    if (rawData.codeList != null)
                    {
                        int num = rawData.codeList.Length;
                        int column = rawData.fieldList.Length;
                        var bonus = (object[])rawData.data;

                        for (int i = 0; i < num; i++)
                        {
                            int index = i * column;
                            if (bonus[index + 4] == null)
                            {
                                continue;
                            }
                            dt.Rows.Add(new object[] { bonus[index], bonus[index + 1], bonus[index + 2], bonus[index + 3], bonus[index + 4], bonus[index + 5], bonus[index + 6], bonus[index + 7], bonus[index + 8], bonus[index + 9], bonus[index + 10], DateTime.Now });
                        }
                        number += num;
                    }
                    else
                    {
                        //if (bonusLatest.code != null)
                        //{
                        //    updateDt.Rows.Add(new object[] { bonusLatest.exDividendDate, bonusLatest.code, bonusLatest.secName, bonusLatest.cashPayoutRatio, bonusLatest.stockSplitRatio, bonusLatest.stockDividendRatio, bonusLatest.seoRatio, bonusLatest.seoPrice, bonusLatest.rightsIssuePrice, bonusLatest.rightsIssueRatio, bonusLatest.exDividendNote, DateTime.Now });
                        //}

                    }
                }
                //Console.WriteLine("update numbers:{0}", number);
            }
            InsertStockBonusDataToSql(dt);
            return dt;
        }



        //更新所有A股历史分红信息,每次更新需要更新表的更新时间
        public DataTable UpdateAllStockBonusDataFromWind()
        {
            UpdateStockInfoToNow();
            var list = GetStockListInfoFromSql();
            var dt = CreateBonusDataTable();
            var updateDt = CreateBonusDataTable();
            var bonusList = GetStockBonusListFromSql();
            int number = 0;
            foreach (var item in list)
            {
                DateTime startDate = item.IPODate.Date;
                DateTime endDate = DateTime.Now.Date;
                bool needToUpdate = false;
                if (item.DelistDate!=null && item.DelistDate<DateTime.Now.Date)
                {
                    endDate = item.DelistDate;
                }
                var bonusLatest = GetLatestStockBonusByCodeFromSql(bonusList, item.code);
                if (bonusLatest.code==null)
                {
                    needToUpdate = true;
                }
                else
                {
                    DateTime updateTime = bonusLatest.updateTime;
                    double lastUpdateDays = (DateTime.Now.Date - updateTime.Date).TotalDays;
                    
                    double lastDividendDays = (DateTime.Now.Date - bonusLatest.exDividendDate.Date).TotalDays;
                    if (lastUpdateDays > 100 && item.DelistDate > DateTime.Now.Date)
                    {
                        needToUpdate = true;
                    }
                    if (lastDividendDays> 180 && lastUpdateDays > 10 && item.DelistDate > DateTime.Now.Date)
                    {
                        needToUpdate = true;
                    }
                    if (item.DelistDate <= DateTime.Now.Date && updateTime<=item.DelistDate)
                    {
                        needToUpdate = true;
                    }
                    startDate = bonusLatest.exDividendDate.AddDays(1);
                    if (startDate<updateTime)
                    {
                        startDate = updateTime;
                    }
                }
                if (endDate<startDate)
                {
                    endDate = startDate;
                }
                if (needToUpdate==true)
                {
                    var rawData = windReader.GetDataSet("corporationaction", string.Format("startdate={1};enddate={2};windcode={0}",          item.code, startDate, endDate));
                    if (rawData.codeList != null)
                    {
                        int num = rawData.codeList.Length;
                        int column = rawData.fieldList.Length;
                        var bonus = (object[])rawData.data;

                        for (int i = 0; i < num; i++)
                        {
                            int index = i * column;
                            if (bonus[index + 4] == null)
                            {
                                continue;
                            }
                            dt.Rows.Add(new object[] { bonus[index], bonus[index + 1], bonus[index + 2], bonus[index + 3], bonus[index + 4], bonus[index + 5], bonus[index + 6], bonus[index + 7], bonus[index + 8], bonus[index + 9], bonus[index + 10], DateTime.Now });
                        }
                        number += num;
                    }
                    else
                    {
                        //if (bonusLatest.code == null)
                        //{
                        //    updateDt.Rows.Add(new object[] { bonusLatest.exDividendDate, bonusLatest.code, bonusLatest.secName, bonusLatest.cashPayoutRatio, bonusLatest.stockSplitRatio, bonusLatest.stockDividendRatio, bonusLatest.seoRatio, bonusLatest.seoPrice, bonusLatest.rightsIssuePrice, bonusLatest.rightsIssueRatio, bonusLatest.exDividendNote, DateTime.Now });
                        //}

                    }
                }
                Console.WriteLine("update numbers:{0}", number);
            }
            InsertStockBonusDataToSql(dt);
            return dt;
        }

        private StockBonusInfo GetLatestStockBonusByCodeFromSql(List<StockBonusInfo> list,string code)
        {
            StockBonusInfo info = new StockBonusInfo();
            info.code = null; 
            foreach (var item in list)
            {
                if (item.code==code)
                {
                    info = item;
                }
            }
            return info;
        }

        public List<StockBonusInfo> GetStockBonusListFromSql(string code="")
        {
            List<StockBonusInfo> list = new List<StockBonusInfo>();
            var sqlStr = "select * from [Common].[dbo].[StockBonusInfo]";
            if (code!="")
            {
                sqlStr =string.Format("select * from [Common].[dbo].[StockBonusInfo] where code='{0}'",code);
            }
            DataTable dt = new DataTable();
            try
            {
                dt = sqlReader.GetDataTable(sqlStr);
            }
            catch
            {
                logger.Warn(string.Format("There is no Stock Bonus information from specialized sqlserver!!!"));
            }
            foreach (DataRow dr in dt.Rows)
            {
                StockBonusInfo info = new StockBonusInfo();
                info.exDividendDate = Convert.ToDateTime(dr["ExDividendDate"]);
                info.code = Convert.ToString(dr["Code"]);
                info.secName = Convert.ToString(dr["SecName"]);
                info.cashPayoutRatio = Convert.ToDouble(dr["CashPayoutRatio"]);
                info.exDividendNote = Convert.ToString(dr["ExDividendNote"]);
                info.rightsIssuePrice = Convert.ToDouble(dr["RightsIssuePrice"]);
                info.rightsIssueRatio = Convert.ToDouble(dr["RightsIssueRatio"]);
                info.seoPrice = Convert.ToDouble(dr["SeoPrice"]);
                info.seoRatio = Convert.ToDouble(dr["SeoRatio"]);
                info.stockDividendRatio = Convert.ToDouble(dr["StockDividendRatio"]);
                info.stockSplitRatio = Convert.ToDouble(dr["StockSplitRatio"]);
                info.updateTime = Convert.ToDateTime(dr["UpdateTime"]);
                if (info.cashPayoutRatio>0)
                {
                    list.Add(info);
                }
            }
            return list;
        }


        ////先删除再添加
        //private void ModifyStockBonusDataToSql(DataTable dt)
        //{
        //    try
        //    {
        //        CreateStockBonusInfoInDB();
        //    }
        //    catch
        //    {
        //        logger.Warn("[Common].[dbo].[StockBonusInfo] create failed!");
        //    }
        //    foreach (DataRow dr in dt.Rows)
        //    {
        //        for (int i = 3; i <= 9; i++)
        //        {
        //            if (dr[i] == DBNull.Value)
        //            {
        //                dr[i] = 0;
        //            }
        //        }


        //    }

        //    sqlWriter.InsertBulk(dt, "[Common].[dbo].[StockBonusInfo]");
        //}

        private void InsertStockBonusDataToSql(DataTable dt)
        {
            try
            {
                CreateStockBonusInfoInDB();
            }
            catch
            {
                logger.Warn("[Common].[dbo].[StockBonusInfo] create failed!");
            }
            foreach (DataRow dr in dt.Rows)
            {
                for (int i = 3; i <=9; i++)
                {
                    if (dr[i]==DBNull.Value)
                    {
                        dr[i] = 0;
                    }
                }
                string code = Convert.ToString(dr[1]);
                DateTime time = Convert.ToDateTime(dr[0]).Date;
                var sql = string.Format("delete from [Common].[dbo].[StockBonusInfo] where code='{0}' and ExDividendDate='{1}'", code, time);
                sqlWriter.WriteChanges(sql);
            }
            sqlWriter.InsertBulk(dt, "[Common].[dbo].[StockBonusInfo]");
        }





        private void InsertToSql(DataTable dt)
        {
            ClearStockInfoInDB(dt);
            sqlWriter.InsertBulk(dt, "[Common].[dbo].[StockInfo]");
        }

        private void CreateStockBonusInfoInDB()
        {

            var sqlScript = string.Format(@"create table common.dbo.StockBonusInfo (
    [ExDividendDate] [datetime] NOT NULL,    
    [Code] [varchar](20) NOT NULL,
    [SecName] [varchar](20) NOT NULL,
	[CashPayoutRatio] [decimal](12, 4) NULL,
	[StockSplitRatio] [decimal](12, 4) NULL,
	[StockDividendRatio] [decimal](12, 4) NULL,
	[SeoRatio] [decimal](12, 4) NULL,
	[SeoPrice] [decimal](12, 4) NULL,
	[RightsIssuePrice] [decimal](12, 4) NULL,
    [RightsIssueRatio] [decimal](12, 4) NULL,
	[ExDividendNote] [varchar](200) NOT NULL,
    [UpdateTime] [datetime] NOT NULL)");
            sqlWriter.ExecuteSqlScript(sqlScript);
        }

        private void ClearStockInfoInDB(DataTable dt)
        {

            string codes="";
            foreach (DataRow dr in dt.Rows)
            {
                if (codes!="")
                {
                    codes = codes + ',';
                }
                codes = codes + "'" + Convert.ToString(dr[0]) + "'";
            }
            if (codes!="")
            {
                codes = "(" + codes + ")";
                var sql = "delete from [Common].[dbo].[StockInfo] where code in " + codes;
                sqlWriter.WriteChanges(sql);
            }
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

        private DataTable CreateBonusDataTable()
        {
            var dt = new DataTable();
            dt.Columns.Add("ExDividendDate", typeof(DateTime));
            dt.Columns.Add("Code", typeof(string));
            dt.Columns.Add("SecName", typeof(string));
            dt.Columns.Add("CashPayoutRatio", typeof(decimal));
            dt.Columns.Add("StockSplitRatio", typeof(decimal));
            dt.Columns.Add("StockDividendRatio", typeof(decimal));
            dt.Columns.Add("SeoRatio", typeof(decimal));
            dt.Columns.Add("SeoPrice", typeof(decimal));
            dt.Columns.Add("RightsIssuePrice", typeof(decimal));
            dt.Columns.Add("RightsIssueRatio", typeof(decimal));
            dt.Columns.Add("ExDividendNote", typeof(string));
            dt.Columns.Add("UpdateTime", typeof(DateTime));
            return dt;
        }


        private DataTable CreateIPODataTable()
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
