using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;

namespace QuantitativeAnalysis.Utilities
{
    public static class OptionUtilities
    {

        static double standardContractMultiplier = 10000;
        
        
        private static double getDuration(StockOptionInformation option,DateTime date)
        {
            TimeSpan span = option.expireDate - date;
            return span.Days + 1;
        }
        
        public static double getDurationByYear(DateTime start,DateTime end)
        {
            TimeSpan span = end - start;
            return (span.Days + 1)/365.0;
        }

        /// <summary>
        /// 根据合约代码，返回合约信息
        /// </summary>
        /// <param name="list"></param>
        /// <param name="code"></param>
        /// <returns></returns>
        public static StockOptionInformation getOptionByCode(List<StockOptionInformation> list, string code)
        {
            foreach (var item in list)
            {
                if (item.code == code)
                {
                    return item;
                }
            }
            return new StockOptionInformation();
        }


        /// <summary>
        /// 根据给定的条件，查找对应期权的合约代码
        /// </summary>
        /// <param name="list">期权合约列表</param>
        /// <param name="endDate">到期时间</param>
        /// <param name="type">认购还是认沽</param>
        /// <param name="strike">行权价格</param>
        /// <returns>满足条件的期权合约列表</returns>
        public static List<StockOptionInformation> getSpecifiedOption(List<StockOptionInformation> list, DateTime endDate, string type, double strike)
        {
            return list.FindAll(delegate (StockOptionInformation info)
            {
                if (info.type == type && info.strike == strike && info.expireDate == endDate)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            });
        }

        /// <summary>
        /// 将期权合约列表按到期时间升序排序
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public static List<DateTime> getEndDateListByAscending(List<StockOptionInformation> list)
        {
            List<DateTime> durationList = new List<DateTime>();
            foreach (var item in list)
            {
                if (durationList.Contains(item.expireDate) == false)
                {
                    durationList.Add(item.expireDate);
                }
            }
            return durationList.OrderBy(x => x).ToList();
        }

        /// <summary>
        /// 按期权合约列表按行权价升序排序
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public static List<double> getStrikeListByAscending(List<StockOptionInformation> list)
        {
            List<double> durationList = new List<double>();
            foreach (var item in list)
            {
                if (durationList.Contains(item.strike) == false)
                {
                    durationList.Add(item.strike);
                }
            }
            return durationList.OrderBy(x => x).ToList();
        }

        /// <summary>
        /// 将上市的期权合约到期时间按升序排序输出
        /// </summary>
        /// <param name="list">期权合约列表</param>
        /// <param name="today">今日日期</param>
        /// <returns>到日期列表(当月，下月，季月，下季月)</returns>
        public static List<double> getDurationStructure(List<StockOptionInformation> list, DateTime today)
        {
            List<double> durationList = new List<double>();
            foreach (var item in list)
            {
                if (item.listedDate <= today && item.expireDate >= today)
                {
                    double duration = getDuration(item,today);
                    if (durationList.Contains(duration) == false && duration >= 0)
                    {
                        durationList.Add(duration);
                    }
                }
            }
            return durationList.OrderBy(x => x).ToList();
        }

        ///// <summary>
        ///// 给定期权合约对应的IH合约代码
        ///// </summary>
        ///// <param name="info">期权合约信息</param>
        ///// <param name="date">当日日期</param>
        ///// <returns>IH合约代码，如果不存在对用的IH合约返回null</returns>
        //public static string getCorrespondingIHCode(StockOptionInformation info, int date)
        //{

        //    DateTime today = Kit.ToDate(date);
        //    if (info.endDate < today || date < 20150416)
        //    {
        //        return null;
        //    }
        //    if (Kit.ToInt_yyyyMMdd(info.endDate) <= 20150430 && Kit.ToInt_yyyyMMdd(info.endDate) >= 20150401)
        //    {
        //        return "IH1505.CFE";
        //    }
        //    DateTime IHExpirationDate = DateUtils.NextOrCurrentTradeDay(DateUtils.GetThirdFridayOfMonth(info.endDate));

        //    if (today <= IHExpirationDate)
        //    {
        //        return "IH" + IHExpirationDate.ToString("yyMM") + ".CFE";
        //    }
        //    else
        //    {
        //        return "IH" + IHExpirationDate.AddMonths(1).ToString("yyMM") + ".CFE";
        //    }

        //}

        /// <summary>
        /// 按期权是认购还是认沽来筛选合约列表
        /// </summary>
        /// <param name="list"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static List<StockOptionInformation> getOptionListByOptionType(List<StockOptionInformation> list, string type)
        {
            return list.FindAll(delegate (StockOptionInformation item)
            {
                if (item.type == type)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            );
        }
        /// <summary>
        /// 按期权行权价来筛选合约列表
        /// </summary>
        /// <param name="list"></param>
        /// <param name="strikeLower"></param>
        /// <param name="strikeUpper"></param>
        /// <returns></returns>
        public static List<StockOptionInformation> getOptionListByStrike(List<StockOptionInformation> list, double strikeLower, double strikeUpper)
        {
            return list.FindAll(delegate (StockOptionInformation item)
            {
                if (item.strike >= strikeLower && item.strike <= strikeUpper)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            );
        }

        /// <summary>
        /// 给定行权价，找到对应的期权合约信息
        /// </summary>
        /// <param name="list"></param>
        /// <param name="strike"></param>
        /// <returns></returns>
        public static List<StockOptionInformation> getOptionListByStrike(List<StockOptionInformation> list, double strike)
        {
            return list.FindAll(delegate (StockOptionInformation item)
            {
                if (item.strike == strike)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            );
        }

        /// <summary>
        /// 根据时间段来筛选上市的期权合约列表
        /// </summary>
        /// <param name="list"></param>
        /// <param name="firstDay">开始时间</param>
        /// <param name="lastDay">结束时间</param>
        /// <returns></returns>
        public static List<StockOptionInformation> getOptionListByDate(List<StockOptionInformation> list, int firstDay, int lastDay)
        {
            return list.FindAll(delegate (StockOptionInformation item)
            {
                if (Convert.ToInt32(item.listedDate.ToString("yyyyMMdd")) <= lastDay && Convert.ToInt32(item.listedDate.ToString("yyyyMMdd")) >= firstDay)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            );
        }

        /// <summary>
        /// 根据日期来筛选上市的期权合约列表
        /// </summary>
        /// <param name="list"></param>
        /// <param name="date"></param>
        /// <returns></returns>
        public static List<StockOptionInformation> getOptionListByDate(List<StockOptionInformation> list, int date)
        {
            return list.FindAll(delegate (StockOptionInformation item)
            {
                if (Convert.ToInt32(item.listedDate.ToString("yyyyMMdd")) <= date && Convert.ToInt32(item.listedDate.ToString("yyyyMMdd")) >= date)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            );
        }

        /// <summary>
        /// 根据到期时间来筛选期权合约
        /// </summary>
        /// <param name="list"></param>
        /// <param name="today"></param>
        /// <param name="duration"></param>
        /// <returns></returns>
        public static List<StockOptionInformation> getOptionListByDuration(List<StockOptionInformation> list, DateTime today, double duration)
        {
            return list.FindAll(delegate (StockOptionInformation item)
            {
                if (getDuration(item,today) == duration)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            );
        }


        /// <summary>
        /// 获取当日上市的合约信息
        /// </summary>
        /// <param name="list">期权合约列表</param>
        /// <param name="today">当日日期</param>
        /// <returns>期权合约列表</returns>
        public static List<StockOptionInformation> getUnmodifiedStockOptionInformationList(List<StockOptionInformation> list, DateTime today)
        {
            List<StockOptionInformation> listUnmodified = new List<StockOptionInformation>();
            foreach (var option in list)
            {
                var item = option;
                if (item.listedDate <= today && item.expireDate >= today)
                {
                    listUnmodified.Add(item);
                }
            }
            return listUnmodified;
        }


        /// <summary>
        /// 根据给定的期权，得到他对应的期权。看涨期权给出对应的看跌期权，看跌去期权给出其对应的看涨期权。
        /// </summary>
        /// <param name="list">期权备选列表</param>
        /// <param name="optionSelected">给定的期权</param>
        /// <returns></returns>
        public static StockOptionInformation getCallByPutOrPutByCall(List<StockOptionInformation> list, StockOptionInformation optionSelected)
        {
            StockOptionInformation optionChosen = new StockOptionInformation();
            foreach (var option in list)
            {
                if (option.expireDate == optionSelected.expireDate && option.strike == optionSelected.strike && option.unit == optionSelected.unit && option.type != optionSelected.type)
                {
                    optionChosen = option;
                    break;
                }
            }
            return optionChosen;
        }
    }

    public static class StockOptionExtension
    {
        public static StockOptionInformation GetParity(List<StockOptionInformation> list,StockOptionInformation option)
        {
            StockOptionInformation parity = new StockOptionInformation();
            foreach (var item in list)
            {
                if (item.strike==option.strike && item.expireDate==option.expireDate && item.underlying==option.underlying && item.type!=option.type && item.unit==option.unit)
                {
                    parity = item;
                    break;
                }
            }
            return parity;
        }

    }
}
