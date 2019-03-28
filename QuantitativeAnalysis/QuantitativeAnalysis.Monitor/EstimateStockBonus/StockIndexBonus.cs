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

namespace QuantitativeAnalysis.Monitor.EstimateStockBonus
{
    public class StockIndexBonus
    {
        private DateTime date;
        private StockInfoRepository stockInfoRepo;
        private TransactionDateTimeRepository dateRepo;
        private WindReader windReader;
        private StockDailyRepository stockDailyRepo;
        private List<StockIPOInfo> stockInfo;

        public StockIndexBonus(StockInfoRepository stockInfoRepo, StockDailyRepository stockDailyRepo,TransactionDateTimeRepository dateRepo,DateTime date)
        {
            this.date =DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(date);
            
            this.stockInfoRepo = stockInfoRepo;
            this.dateRepo = dateRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.windReader = new WindReader();
            this.stockInfo = stockInfoRepo.GetStockListInfoFromSql();
        }
        
        public void getBonusByIndex(string index)
        {
            var indexInfoList=getDocumentFromFtp(index);
            List<string> codeList = new List<string>();
            foreach (var item in indexInfoList)
            {
                codeList.Add(item.code);
            }
            stockInfoRepo.UpdateStockBonusDataFromWind(codeList);
            var list=getIndexBonus(date,index,indexInfoList);
            storeResults(list,index);
        }

        private string getFileName(string index,DateTime date,string name,string type)
        {
            string[] indexStr = index.Split('.');
            string todayStr = date.Year.ToString();
            if (date.Month <= 9)
            {
                todayStr += '0';
            }
            todayStr += date.Month.ToString();
            if (date.Day <= 9)
            {
                todayStr += '0';
            }
            todayStr += date.Day.ToString();
            string filename = indexStr[0] + name + todayStr + "."+type;
            return filename;
        }


       
        private List<indexStockInfo> getDocumentFromFtp(string index)
        {
            FtpHelper ftp = new FtpHelper("192.168.38.213", "index", "dfzq1234");
            List<indexStockInfo> list = new List<indexStockInfo>();
            string[] indexStr = index.Split('.');
            string todayStr = date.Year.ToString();
            string filename = getFileName(index, date, "weightnextday", "xls");
            string original = "\\hs300\\" + filename;
            string target = "E:\\result\\stockBonusEstimate\\" + indexStr[0] + "weightnextday" + todayStr + ".xls";
            ftp.Download(original, target);
            var set = DataTableExtension.getDataSetFromXLS(target);
            foreach (DataTable dt in set.Tables)
            {
                int num = 0;
                foreach (DataRow dr in dt.Rows)
                {
                    num = num + 1;
                    //去除表头
                    if (num<=1)
                    {
                        continue;
                    }
                    indexStockInfo info = new indexStockInfo();
                    string code = Convert.ToString(dr[4]);
                    string market = Convert.ToString(dr[7]);
                    string secName = Convert.ToString(dr[5]);
                    if (market=="Shanghai")
                    {
                        code = code + ".SH";
                    }
                    else
                    {
                        code = code + ".SZ";
                    }
                    double close = Convert.ToDouble(dr[12]);
                    double weight = Convert.ToDouble(dr[16]);
                    info.code = code;
                    info.secName = secName;
                    info.close = close;
                    info.weight = weight;
                    list.Add(info);
                }
            }
            return list;
        }

        private void storeResults(List<StockBonusEstimate> list,string index)
        {
            var dt = DataTableExtension.ToDataTable(list);
            dt.Columns["code"].ColumnName = "代码";
            dt.Columns["secName"].ColumnName = "股票";
            dt.Columns["dividend"].ColumnName = "分红利息";
            dt.Columns["dividendDate"].ColumnName = "分红时间";
            dt.Columns["shareRegisterDate"].ColumnName = "股权登记日";
            dt.Columns["points"].ColumnName = "分红点数";
            dt.Columns["status"].ColumnName = "备注";
            string filename = getFileName(index, date, "bonus", "csv");
            string nameStr = string.Format("E:\\result\\stockBonusEstimate\\{0}",filename);
            string remoteStr= "\\IndexBonus\\" + filename;;
            DataTableExtension.SaveCSV(dt, nameStr);
            FileInfo file = new FileInfo(nameStr);
            FtpHelper ftp = new FtpHelper("192.168.38.213", "index", "dfzq1234");
            ftp.Upload(file, remoteStr);
        }


        public List<StockBonusEstimate> getIndexBonus(DateTime date,string index,List<indexStockInfo> indexStockList)
        {
            var codeList=getIndexStocks(date, index);
            var bonusDic = getStockBonusList(codeList,date);
            var bonusDicLastYear = getStockBonusListByYear(bonusDic, date.Year - 1);
            var bonusDicThisYear = getStockBonusListByYear(bonusDic, date.Year);
            var bonusPlanDic=getStockBonusPlan(codeList, date);
            var epsDic=getStockEPSRatio(codeList, date);
            //获取当前日期
            var now = DateTime.Now.Date;
            double indexClose = stockDailyRepo.GetStockTransaction(index, date, date)[0].Close;
            DateTime lastYearEnd = new DateTime(date.Year - 1, 12, 31);
            DateTime thisYearMid = new DateTime(date.Year, 6, 30);
            List<StockBonusEstimate> estimateList = new List<StockBonusEstimate>();
            foreach (var code in codeList)
            {
                StockBonusEstimate estimate = new StockBonusEstimate();
                double epsRatio = 0;
                var bonusLastYear = new List<StockBonusInfo>();
                var bonusThisYear = new List<StockBonusInfo>();
                var bonusPlan = new List<StockBonusPlan>();
                int lastYearNumber = 0;
                int thisYearNumber = 0;
                if (epsDic.ContainsKey(code))
                {
                    epsRatio = epsDic[code];
                }
                if (bonusDicLastYear.ContainsKey(code))
                {
                    bonusLastYear = bonusDicLastYear[code];
                    lastYearNumber = bonusLastYear.Count();
                }
                if (bonusDicThisYear.ContainsKey(code))
                {
                    bonusThisYear = bonusDicThisYear[code];
                    thisYearNumber = bonusThisYear.Count();
                }
                if (bonusPlanDic.ContainsKey(code))
                {
                    bonusPlan = bonusPlanDic[code];
                }
                

                //去年无分红
                if (lastYearNumber==0)
                {
                    //按分红公告统计
                    foreach (var item in bonusThisYear)
                    {
                        estimate = new StockBonusEstimate();
                        estimate.code = item.code;
                        estimate.secName = item.secName;
                        estimate.dividend = item.cashPayoutRatio;
                        estimate.dividendDate = item.exDividendDate;
                        estimate.status = "去年无分红;分红公告明确";
                        estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                        if (estimate.dividendDate<=now)
                        {
                            estimate.status += "已分红";
                        }
                        estimateList.Add(estimate);
                    }
                    //分红无公告的，按分红预案统计
                    if (thisYearNumber==0 && bonusPlan.Count>0)
                    {
                        foreach (var item in bonusPlan)
                        {
                            estimate = new StockBonusEstimate();
                            estimate.code = item.code;
                            estimate.secName = item.name;
                            estimate.dividend = item.dividend;
                            estimate.status = "去年无分红;分红预案明确;分红日期未知";
                            estimate.dividendDate = new DateTime(date.Year, 12, 31);
                            estimate.shareRegisterDate = new DateTime(date.Year, 12, 31);
                            estimateList.Add(estimate);
                        }
                    }
                    //既没有分红公告也没有分红预案的，按不分红统计
                }
                //去年分红一次
                else if (lastYearNumber==1)
                {
                    //先按分红公告统计
                    foreach (var item in bonusThisYear)
                    {
                        estimate = new StockBonusEstimate();
                        estimate.code = item.code;
                        estimate.secName = item.secName;
                        estimate.dividend = item.cashPayoutRatio;
                        estimate.dividendDate = item.exDividendDate;
                        estimate.status = "去年分红1次;分红公告明确";
                        estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                        if (estimate.dividendDate <= now)
                        {
                            estimate.status += "已分红";
                        }
                        estimateList.Add(estimate);
                    }
                    //有分红公告分红1次的和分红预案比较是否有遗漏的，分红两次的不需要比较
                    if (thisYearNumber==1 && bonusPlan.Count>0)
                    {
                        //需要吗？
                    }
                    //没有分红公告但是有分红预案的公告，按照分红预案的信息
                    if (thisYearNumber == 0 && bonusPlan.Count > 0)
                    {
                        foreach (var item in bonusPlan)
                        {
                            estimate = new StockBonusEstimate();
                            estimate.code = item.code;
                            estimate.secName = item.name;
                            estimate.dividend = item.dividend;
                            estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[0].exDividendDate.AddYears(+1));
                            estimate.status = "去年分红1次;分红预案明确;分红日期未知";
                            estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                            if (estimate.dividendDate < now)
                            {
                                estimate.dividendDate = new DateTime(date.Year, 12, 31);
                                estimate.status = estimate.status + ";对应日期已过分红日期未知";
                                estimate.shareRegisterDate = new DateTime(date.Year, 12, 31);
                            }
                            estimateList.Add(estimate);
                        }
                    }
                    //没有分红公告也没有分红预案，按照EPS和去年分红信息来估计
                    if (thisYearNumber==0 && bonusPlan.Count==0 && epsRatio>0)
                    {
                        estimate = new StockBonusEstimate();
                        estimate.code = bonusLastYear[0].code;
                        estimate.secName = bonusLastYear[0].secName;
                        estimate.dividend = bonusLastYear[0].cashPayoutRatio*epsRatio;
                        estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[0].exDividendDate.AddYears(+1));
                        estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                        estimate.status = "去年分红1次;无公告无预案按eps估计";
                        if (estimate.dividendDate < now)
                        {
                            estimate.dividendDate = new DateTime(date.Year, 12, 31);
                            estimate.status = estimate.status + ";对应日期已过分红日期未知";
                            estimate.shareRegisterDate= new DateTime(date.Year, 12, 31);
                        }
                        estimateList.Add(estimate);
                    }

                }
                //去年分红两次
                else if (lastYearNumber>=2)
                {
                    int thisyear = 0;
                    //先按分红公告统计
                    foreach (var item in bonusThisYear)
                    {
                        estimate = new StockBonusEstimate();
                        estimate.code = item.code;
                        estimate.secName = item.secName;
                        estimate.dividend = item.cashPayoutRatio;
                        estimate.dividendDate = item.exDividendDate;
                        estimate.status = "去年分红2次;分红公告明确";
                        estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                        if (estimate.dividendDate <= now)
                        {
                            estimate.status += "已分红";
                        }
                        estimateList.Add(estimate);
                        thisyear += 1;
                    }
                    if (bonusThisYear.Count==0)
                    {
                        foreach (var item in bonusPlan)
                        {
                            estimate = new StockBonusEstimate();
                            estimate.code = item.code;
                            estimate.secName = item.name;
                            estimate.dividend = item.dividend;
                            estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[0].exDividendDate.AddYears(+1));
                            estimate.status = "去年分红2次;分红预案明确;分红时间未知";
                            if (thisyear==0)
                            {
                                estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[0].exDividendDate.AddYears(+1));
                                if (estimate.dividendDate<now)
                                {
                                    estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[1].exDividendDate.AddYears(+1));
                                    estimate.status = estimate.status + ":去年对应的第1次分红日期已过";
                                }
                                estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));

                            }
                            else
                            {
                                estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[1].exDividendDate.AddYears(+1));
                                estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                                
                            }
                            if (estimate.dividendDate < now)
                            {
                                estimate.dividendDate = new DateTime(date.Year, 12, 31);
                                estimate.status = estimate.status + ";对应日期已过分红日期未知";
                                estimate.shareRegisterDate = new DateTime(date.Year, 12, 31);
                            }
                            estimateList.Add(estimate);
                            thisyear += 1;
                        }
                    }
                    else if (bonusThisYear.Count==1)
                    {
                        foreach (var item in bonusPlan)
                        {
                            estimate = new StockBonusEstimate();
                            estimate.code = item.code;
                            estimate.secName = item.name;
                            estimate.dividend = item.dividend;
                            estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[0].exDividendDate.AddYears(+1));
                            estimate.status = "去年分红2次;分红预案明确;分红时间未明确";
                            estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                            if (estimate.dividendDate < now)
                            {
                                estimate.dividendDate = new DateTime(date.Year, 12, 31);
                                estimate.status = estimate.status + ";对应日期已过分红日期未知";
                                estimate.shareRegisterDate = new DateTime(date.Year, 12, 31);
                            }
                            estimateList.Add(estimate);
                            thisyear += 1;
                        }
                    }
                    else if (bonusThisYear.Count==2)
                    {
                        //搞定
                    }

                    //利用去年分红数据估计
                    if (thisyear==0 && epsRatio > 0)
                    {
                        estimate = new StockBonusEstimate();
                        estimate.code = bonusLastYear[0].code;
                        estimate.secName = bonusLastYear[0].secName;
                        estimate.dividend = bonusLastYear[0].cashPayoutRatio * epsRatio;
                        estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[0].exDividendDate.AddYears(+1));
                        estimate.status = "去年分红2次;按eps估计第1次分红";
                        estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                        if (estimate.dividendDate < now)
                        {
                            estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[1].exDividendDate.AddYears(+1));
                            estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                            estimate.status = estimate.status + ":去年对应的第1次分红日期已过";
                        }
                        if (estimate.dividendDate < now)
                        {
                            estimate.dividendDate = new DateTime(date.Year, 12, 31);
                            estimate.status = estimate.status + ";对应日期已过分红日期未知";
                            estimate.shareRegisterDate = new DateTime(date.Year, 12, 31);
                        }
                        estimateList.Add(estimate);
                        estimate = new StockBonusEstimate();
                        estimate.code = bonusLastYear[1].code;
                        estimate.secName = bonusLastYear[1].secName;
                        estimate.dividend = bonusLastYear[1].cashPayoutRatio * epsRatio;
                        estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[1].exDividendDate.AddYears(+1));
                        estimate.status = "去年分红2次;按eps估计第2次分红";
                        estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                        if (estimate.dividendDate < now)
                        {
                            estimate.dividendDate = new DateTime(date.Year, 12, 31);
                            estimate.status = estimate.status + ";对应日期已过分红日期未知";
                            estimate.shareRegisterDate = new DateTime(date.Year, 12, 31);
                        }
                        estimateList.Add(estimate);
                    }
                    else if (thisyear== 1 && epsRatio > 0)
                    {
                        estimate = new StockBonusEstimate();
                        estimate.code = bonusLastYear[1].code;
                        estimate.secName = bonusLastYear[1].secName;
                        estimate.dividend = bonusLastYear[1].cashPayoutRatio * epsRatio;
                        estimate.dividendDate = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(bonusLastYear[1].exDividendDate.AddYears(+1));
                        estimate.status = "去年分红2次;按eps估计第2次分红";
                        estimate.shareRegisterDate = DateTimeExtension.DateUtils.LatestTradeDay(estimate.dividendDate.AddDays(-1));
                        if (estimate.dividendDate < now)
                        {
                            estimate.dividendDate = new DateTime(date.Year, 12, 31);
                            estimate.status = estimate.status + ";对应日期已过分红日期未知";
                            estimate.shareRegisterDate = new DateTime(date.Year, 12, 31);
                        }
                        estimateList.Add(estimate);
                    }

                }
            }
            //计算具体的指数点
            foreach (var item in estimateList)
            {
                foreach (var stock in indexStockList)
                {
                    if (item.code==stock.code)
                    {
                        double points = stock.weight * item.dividend / stock.close * indexClose*0.01;
                        item.points = points;
                    }
                }
            }


            return estimateList;
        }

        private Dictionary<string, double> getStockEPSRatio(List<string> codeList,DateTime date)
        {
            Dictionary<string, double> epsDic = new Dictionary<string, double>();
            DateTime lastYear = DateTimeExtension.DateUtils.PreviousOrCurrentTradeDay(date.AddYears(-1));
            foreach (var code in codeList)
            {
                double epsThisYear = 0;
                double epsLastYear = 0;
                DateTime lastYearNow = lastYear;
                foreach (var item in stockInfo)
                {
                    if (item.code == code)
                    {
                        if (lastYear<item.IPODate)
                        {
                            lastYearNow = item.IPODate;
                        }
                    }
                }
                var info = stockInfo.Select(x => x.code == code);
                var rawData1 = windReader.GetDailyDataTable(code, "eps_ttm", lastYearNow, lastYearNow);
                var rawData2= windReader.GetDailyDataTable(code, "eps_ttm", date,date);
                foreach (DataRow dr in rawData1.Rows)
                {
                    epsLastYear = Convert.ToDouble(dr[2]);
                    break;
                }
                foreach (DataRow dr in rawData2.Rows)
                {
                    epsThisYear = Convert.ToDouble(dr[2]);
                    break;
                }

                if (epsThisYear<0)
                {
                    epsDic.Add(code, -1000);
                }
                else
                {
                    epsDic.Add(code, epsThisYear / epsLastYear);
                }
            }
            return epsDic;
        }
        


        private Dictionary<string, List<StockBonusPlan>> getStockBonusPlan(List<string> codeList,DateTime date)
        {
            Dictionary<string, List<StockBonusPlan>> planDic = new Dictionary<string, List<StockBonusPlan>>();
            DateTime lastYear = new DateTime(date.Year - 1, 1, 1);
            DateTime lastYearEnd = new DateTime(date.Year - 1, 12, 31);
            DateTime thisYearMid = new DateTime(date.Year, 6, 30);
            var rawData = windReader.GetDataSetTable("dividendproposal", string.Format("ordertype=1;startdate={0};enddate={1};sectorid=a001010100000000;field=wind_code,sec_name,report_date,progress,cash_dividend,fellow_preplandate", lastYear, date));
            foreach (DataRow dr in rawData.Rows)
            {
                string code = Convert.ToString(dr[0]);
                string name = Convert.ToString(dr[1]);
                DateTime reportDate = Convert.ToDateTime(dr[2]);
                string status = Convert.ToString(dr[3]);
                double dividend = 0;
                if (dr[4]!=DBNull.Value)
                {
                    dividend=Convert.ToDouble(dr[4]);
                }
                DateTime planDate = new DateTime();
                if (dr[5]!=DBNull.Value)
                {
                    planDate = Convert.ToDateTime(dr[5]);
                }
                if (codeList.Contains(code) && reportDate>=lastYearEnd && dividend>0)
                {
                    StockBonusPlan plan = new StockBonusPlan();
                    plan.code = code;
                    plan.status = status;
                    plan.dividend = dividend;
                    plan.name = name;
                    plan.reportDate = reportDate;
                    plan.planDate = planDate;
                    if (planDic.ContainsKey(code))
                    {
                        planDic[code].Add(plan);
                    }
                    else
                    {
                        List<StockBonusPlan> planList = new List<StockBonusPlan>();
                        planList.Add(plan);
                        planDic.Add(code, planList);
                    }
                }
            }
            return planDic;
        }


        private Dictionary<string, List<StockBonusInfo>> getStockBonusListByYear(Dictionary<string, List<StockBonusInfo>>bonusDic,int year)
        {
            Dictionary<string, List<StockBonusInfo>> dic = new Dictionary<string, List<StockBonusInfo>>();
            foreach (var item in bonusDic)
            {
                List<StockBonusInfo> list = new List<StockBonusInfo>();
                foreach (var bonusInfo in item.Value)
                {
                    if (bonusInfo.exDividendDate.Year==year)
                    {
                        if (dic.ContainsKey(item.Key))
                        {
                            dic[item.Key].Add(bonusInfo);
                        }
                        else
                        {
                            list.Add(bonusInfo);
                            dic.Add(item.Key, list);
                        }
                    }
                    
                }
            }
            return dic;
        }


        //第二年的1月1日开始预计
        private Dictionary<string, List<StockBonusInfo>> getStockBonusList(List<string> codeList,DateTime date)
        {
            Dictionary<string, List<StockBonusInfo>> bonusList = new Dictionary<string, List<StockBonusInfo>>();
            foreach (var code in codeList)
            {
                var list = stockInfoRepo.GetStockBonusListFromSql(code);
                var lastYearBonusList =new List<StockBonusInfo>();
                if (list.Count!=0)
                {
                    foreach (var item in list)
                    {
                        if (item.exDividendDate.Year>=date.AddYears(-1).Year)
                        {
                            lastYearBonusList.Add(item);
                        }
                    }
                    if (lastYearBonusList.Count!=0)
                    {
                        bonusList.Add(code, lastYearBonusList);
                    }
                }
            }
            return bonusList;
        }


        private List<string> getIndexStocks(DateTime date,string index)
        {
            var rawData = windReader.GetDataSetTable("sectorconstituent", string.Format("date={0};windcode={1}", date.Date, index));
            List<string> codeList = new List<string>();
            foreach (DataRow dr in rawData.Rows)
            {
                codeList.Add(Convert.ToString(dr[1]));
            }
            return codeList;
        }

        private List<StockIPOInfo> getStockListFromIndex(string index)
        {
            List<StockIPOInfo> list = new List<StockIPOInfo>();
            var allStock =stockInfoRepo.GetStockListInfoFromSql();

            return list;

        }


    }
}
