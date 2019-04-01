using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Monitor.Bond.ConvertibleBond
{
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
    using QuantitativeAnalysis.DataAccess.ETF;


    public class Intraday1
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string underlyingCode;
        private string indexCode;
        private List<DateTime> tradedays = new List<DateTime>();
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockDailyRepository stockDailyRepo;
        private List<OneByOneTransaction> transactionData=new List<OneByOneTransaction>();
        private StockMinuteRepository stockMinutelyRepo;
        private StockTickRepository tickRepo;
        private SqlServerWriter sqlWriter;
        private SqlServerReader sqlReader;
        private List<ConvertibleBondInfo> bondInfo;
        private WindReader windReader=new WindReader();
        private double slipRatio = 0.0001;
        private double feeRatioBuy = 0.0001;
        private double feeRatioSell = 0.0001;
        private double priceUnit = 0.001;
        //储存行情数据
        //tick数据
        Dictionary<DateTime, Dictionary<string, List<StockTickTransaction>>> tickData = new Dictionary<DateTime, Dictionary<string, List<StockTickTransaction>>>();
        //分数数据
        Dictionary<DateTime, Dictionary<string, List<StockTransaction>>> minuteData = new Dictionary<DateTime, Dictionary<string, List<StockTransaction>>>();
        //日线数据
        Dictionary<string, List<StockTransaction>> dailyData = new Dictionary<string, List<StockTransaction>>();
        //可转债日线转股价格数据
        Dictionary<string, List<ConvertibleBondDailyInfo>> bondDailyInfo = new Dictionary<string, List<ConvertibleBondDailyInfo>>();




        public Intraday1(StockMinuteRepository stockMinutelyRepo, StockDailyRepository stockDailyRepo, StockTickRepository tickRepo, TransactionDateTimeRepository dateRepo)
        {
            this.stockMinutelyRepo = stockMinutelyRepo;
            this.stockDailyRepo = stockDailyRepo;
            this.dateRepo = dateRepo;
            this.tickRepo = tickRepo;
            sqlWriter = new SqlServerWriter(ConnectionType.Server84);
            sqlReader = new SqlServerReader(ConnectionType.Local);
        }

        public void backtest(DateTime startDate, DateTime endDate)
        {
            dataPrepare(startDate, endDate);
            this.transactionData = new List<OneByOneTransaction>();
            foreach (var day in tradedays)
            {
                foreach (var info in bondInfo)
                {
                    if (day.Date >= info.startDate.Date && day.Date <= info.endDate.Date)
                    {
                        //var record = computeDailyWithRecordByMinute(day, info.code, info.stockCode, 0.01);
                        var record = computeDailyWithRecordByTick(day, info.code, info.stockCode, 0.03);
                        if (record.Count>0)
                        {
                            this.transactionData.AddRange(record);
                        }
                        
                    }
                }
            }
            var dt = DataTableExtension.ToDataTable(transactionData);
            var nowStr = DateTime.Now.ToString("yyyyMMddhhmm");
            string name = string.Format("E:\\result\\bond\\convertibleBond{0}.csv",nowStr);
            DataTableExtension.SaveCSV(dt, name);
        }


        private List<OneByOneTransaction> computeDailyWithRecordByTick(DateTime date,string bond,string stock,double stopLossRatio)
        {
            List<OneByOneTransaction> record = new List<OneByOneTransaction>();
            if (tickData.ContainsKey(date)==false || tickData[date].ContainsKey(bond)==false || tickData[date].ContainsKey(stock)==false)
            {
                return record;
            }
            
            double ceilPrice = getCeilingPrice(date, stock);
            double previousAmount = getPreviousAmount(date, stock);
            double previousBondPrice = getPreviousBondClose(date, bond);
            var bondData = tickData[date][bond];
            var stockData = tickData[date][stock];
            var bondDailyInfoNow = getBondDailyInfo(date, bond);
            double strike = bondDailyInfoNow.conversionPrice;
            double numbers = 100 / strike;
            double delta = getOptionDelta(date,bond,stock);
            delta =delta+0.2;
            double bondEstimatePrice = getEstimateBondPrice(date, bond, stock);
            if (bondEstimatePrice<previousBondPrice)
            {
                previousBondPrice = bondEstimatePrice;
            }
            //if (ceilPrice<strike)
            //{
            //    delta = 0.5;
            //}
            //else if (ceilPrice<1.5*strike)
            //{
            //    delta = 0.8;
            //}
            //else 
            //{
            //    delta = 1;
            //}
            double stockPriceChanged = ceilPrice / 11;
            //ceilPrice = stockData[0].HighLimit;
            var allData = getMergeData(bondData, stockData);
            double position = 0;
            double openPrice = 0;
            DateTime openTime = new DateTime();
            DateTime closeTime = new DateTime();
            double closePrice = 0;
            double maxOpenAmount = 0;
            double maxCloseAmount = 0;
            double longMaxPrice = 0;
            string status = "";
            TimeSpan lastOpenTime = new TimeSpan(14, 55, 00);
            TimeSpan firstOpenTime = new TimeSpan(9, 30, 00);
            var recordNow = new OneByOneTransaction();
            int index = 0;
            while (index<allData.Count()-1)
            {
                //正股涨停
                if (allData[index].Code == stock && allData[index].AskV1 == 0 && allData[index].Bid1 == ceilPrice && allData[index+1].Code==bond &&allData[index+1].AskV1>0 && position==0 && allData[index].TransactionDateTime.TimeOfDay<lastOpenTime && allData[index].TransactionDateTime.TimeOfDay >= firstOpenTime)
                {
                    double ceilAmount = allData[index].Bid1 * allData[index].BidV1;
                    if (ceilAmount>previousAmount*0.2  && allData[index+1].LastPrice<previousBondPrice+delta*numbers*stockPriceChanged)
                    {
                        
                        recordNow = new OneByOneTransaction();
                        var bondDataNow = allData[index + 1];
                        maxOpenAmount = bondDataNow.Ask1 * bondDataNow.AskV1;
                        double bondVolume = bondDataNow.AskV1;
                        if (maxOpenAmount>=10000 && (maxOpenAmount/ bondVolume)<1000)
                        {
                            position = 1;
                            openPrice = maxOpenAmount / bondVolume;
                            openTime = bondDataNow.TransactionDateTime;
                            recordNow.code = bond;
                            recordNow.maxOpenAmount = maxOpenAmount;
                            recordNow.openPrice = openPrice;
                            recordNow.openTime = openTime;
                            recordNow.date = date.Date;
                        }
                        index++;
                    }
                }
                //涨停打开或者将要打开卖出可转债
                if (position==1 && allData[index].Code == stock && (allData[index].Bid1 < ceilPrice || (allData[index].Bid1 == ceilPrice && allData[index].Bid1 * allData[index].BidV1<previousAmount*0.03)))
                {
                    for (int i = 1; i < allData.Count()-index; i++)
                    {
                        if (allData[index+i].Code==bond)//找到bond对应的数据
                        {
                            position = 0;
                            var bondDataNow = allData[index + i];
                            maxCloseAmount = bondDataNow.Bid1 * bondDataNow.BidV1;
                            double bondVolume = bondDataNow.BidV1;
                            closePrice = maxCloseAmount / bondVolume;
                            if (bondVolume == 0)
                            {
                                closePrice = bondDataNow.LastPrice;
                            }
                            closeTime = bondDataNow.TransactionDateTime;
                            recordNow.maxCloseAmount = maxCloseAmount;
                            recordNow.closePrice = closePrice;
                            recordNow.closeTime = closeTime;
                            recordNow.closeStatus = "涨停即将打开或已经打开";
                            recordNow.yield = (recordNow.closePrice - recordNow.openPrice) / recordNow.openPrice;
                            record.Add(recordNow);
                            index = index + i;
                            break;
                        }
                    }
                }

                //正股封涨停但是可转债跌幅过大，止损卖出
                if (position == 1 && allData[index].Code == bond && allData[index].LastPrice < (1-stopLossRatio) * openPrice)
                {
                    position = 0;
                    var bondDataNow = allData[index];
                    maxCloseAmount = bondDataNow.Bid1 * bondDataNow.BidV1;
                    double bondVolume = bondDataNow.BidV1;
                    closePrice = maxCloseAmount / bondVolume;
                    if (bondVolume == 0)
                    {
                        closePrice = bondDataNow.LastPrice;
                    }
                    closeTime = bondDataNow.TransactionDateTime;
                    recordNow.maxCloseAmount = maxCloseAmount;
                    recordNow.closePrice = closePrice;
                    recordNow.closeTime = closeTime;
                    recordNow.closeStatus = "涨停未打开但可转债下跌";
                    recordNow.yield = (recordNow.closePrice - recordNow.openPrice) / recordNow.openPrice;
                    record.Add(recordNow);
                    index = index ++;
                }

                //14点57分之后，强制平仓
                if (position == 1 && allData[index].TransactionDateTime.TimeOfDay>=lastOpenTime)
                {
                    for (int i = 1; i < allData.Count() - index; i++)
                    {
                        if (allData[index + i].Code == bond)//找到bond对应的数据
                        {
                            position = 0;
                            var bondDataNow = allData[index + i];
                            maxCloseAmount = bondDataNow.Bid1 * bondDataNow.BidV1;
                            double bondVolume = bondDataNow.BidV1;
                            closePrice = maxCloseAmount / bondVolume;
                            if (bondVolume==0)
                            {
                                closePrice=bondDataNow.LastPrice;
                            }
                            closeTime = bondDataNow.TransactionDateTime;
                            recordNow.maxCloseAmount = maxCloseAmount;
                            recordNow.closePrice = closePrice;
                            recordNow.closeTime = closeTime;
                            recordNow.closeStatus = "收盘强平";
                            recordNow.yield =(recordNow.closePrice - recordNow.openPrice) / recordNow.openPrice;
                            record.Add(recordNow);
                            index = index + i;
                            break;
                        }
                    }
                }
                index++;
            }
            return record;

        }

        private List<StockTickTransaction> getMergeData(List<StockTickTransaction> list1,List<StockTickTransaction> list2)
        {
            List<StockTickTransaction> list = new List<StockTickTransaction>();
            foreach (var item in list1)
            {
                StockTickTransaction trans = new StockTickTransaction();
                trans.Volume = item.Volume;
                trans.Amount = item.Amount;
                trans.Code = item.Code;
                trans.LastPrice = item.LastPrice;
                trans.TransactionDateTime = item.TransactionDateTime;
                trans.Ask1 = item.Ask1;
                trans.Ask2 = item.Ask2;
                trans.Ask3 = item.Ask3;
                trans.Ask4 = item.Ask4;
                trans.Ask5 = item.Ask5;
                trans.AskV1 = item.AskV1;
                trans.AskV2 = item.AskV2;
                trans.AskV3 = item.AskV3;
                trans.AskV4 = item.AskV4;
                trans.AskV5 = item.AskV5;
                trans.Bid1 = item.Bid1;
                trans.Bid2 = item.Bid2;
                trans.Bid3 = item.Bid3;
                trans.Bid4 = item.Bid4;
                trans.Bid5 = item.Bid5;
                trans.BidV1 = item.BidV1;
                trans.BidV2 = item.BidV2;
                trans.BidV3 = item.BidV3;
                trans.BidV4 = item.BidV4;
                trans.BidV5 = item.BidV5;
                list.Add(trans);
            }
            foreach (var item in list2)
            {
                StockTickTransaction trans = new StockTickTransaction();
                trans.Volume = item.Volume;
                trans.Amount = item.Amount;
                trans.Code = item.Code;
                trans.LastPrice = item.LastPrice;
                trans.TransactionDateTime = item.TransactionDateTime;
                trans.Ask1 = item.Ask1;
                trans.Ask2 = item.Ask2;
                trans.Ask3 = item.Ask3;
                trans.Ask4 = item.Ask4;
                trans.Ask5 = item.Ask5;
                trans.AskV1 = item.AskV1;
                trans.AskV2 = item.AskV2;
                trans.AskV3 = item.AskV3;
                trans.AskV4 = item.AskV4;
                trans.AskV5 = item.AskV5;
                trans.Bid1 = item.Bid1;
                trans.Bid2 = item.Bid2;
                trans.Bid3 = item.Bid3;
                trans.Bid4 = item.Bid4;
                trans.Bid5 = item.Bid5;
                trans.BidV1 = item.BidV1;
                trans.BidV2 = item.BidV2;
                trans.BidV3 = item.BidV3;
                trans.BidV4 = item.BidV4;
                trans.BidV5 = item.BidV5;
                list.Add(trans);
            }
            return list.OrderBy(x => x.TransactionDateTime).ToList();
        }

        private OneByOneTransaction computeDailyWithRecordByMinute(DateTime date, string bond, string stock,double stopLossRatio)
        {
            OneByOneTransaction record = new OneByOneTransaction();
            if (minuteData.ContainsKey(date) == false || minuteData[date].ContainsKey(bond) == false || minuteData[date].ContainsKey(stock) == false)
            {
                return record;
            }
            double ceilPrice = getCeilingPrice(date, stock);
            var bondData = minuteData[date][bond];
            var stockData = minuteData[date][stock];
            double position = 0;
            double openPrice = 0;
            DateTime openTime = new DateTime();
            DateTime closeTime = new DateTime();
            double closePrice = 0;
            double maxOpenAmount = 0;
            double maxCloseAmount = 0;
            double longMaxPrice = 0;
            string status="";
            for (int i = 0; i < stockData.Count()-5; i++)
            {
                if (stockData[i].High==ceilPrice && position==0 && bondData[i+1].Volume>0)
                {
                    position = 1;
                    openPrice = bondData[i + 1].Amount / bondData[i + 1].Volume;
                    maxOpenAmount = bondData[i + 1].Amount;
                    openTime = stockData[i + 1].DateTime;
                    longMaxPrice = openPrice;
                }
                if (position==1 && (bondData[i].Close-longMaxPrice)/longMaxPrice<-stopLossRatio && stockData[i].Close<ceilPrice*0.995)
                {
                    position = 0;
                    if (bondData[i+1].Volume>0)
                    {
                        closePrice = bondData[i + 1].Amount / bondData[i + 1].Volume;
                    }
                    else
                    {
                        closePrice = bondData[i + 1].Close;
                    }
                    maxCloseAmount = bondData[i + 1].Amount;
                    closeTime = bondData[i + 1].DateTime;
                    status = "追踪止损";
                    break;
                }
                else if (position==1 && bondData[i].Close>longMaxPrice)
                {
                    longMaxPrice = bondData[i].Close;
                }
            }
            //收盘之前3分钟平仓
            if (position==1)
            {
                position = 0;
                if (bondData[stockData.Count() - 3].Volume>0)
                {
                    closePrice = bondData[stockData.Count() - 3].Amount / bondData[stockData.Count() - 3].Volume;
                }
                else
                {
                    closePrice = bondData[stockData.Count() - 3].Close;
                }
                maxCloseAmount = bondData[stockData.Count() - 3].Amount;
                closeTime = bondData[stockData.Count() - 3].DateTime;
                status = "收盘强平";
            }
            
            record.openPrice = openPrice;
            record.openTime = openTime;
            record.closePrice = closePrice;
            record.closeTime = closeTime;
            record.maxOpenAmount = maxOpenAmount;
            record.maxCloseAmount = maxCloseAmount;
            record.position = position;
            record.yield = (closePrice - openPrice) / openPrice;
            record.date = date.Date;
            record.closeStatus = status;
            record.code = bond;
            return record;
        }


        private double getOptionDelta(DateTime date,string BondCode,string stockCode)
        {
            double delta = 0.5;
            var bondInfo = getBondDailyInfo(date, BondCode);
            int days = DateTimeExtension.DateUtils.GetSpanOfTradeDays(date, bondInfo.conversionEndDate);
            double duration = (double)days / 252.0;
            var stockList = getPreviousStockCloseList(date, stockCode, days);
            var stockLastClose = stockList[stockList.Count() - 1];
            double volatility = HistoricalVolatilityExtension.getHistoricalVolatilityByClosePrice(stockList);

            double strike = bondInfo.conversionPrice;
            delta = ImpliedVolatilityExtension.ComputeOptionDelta(strike, duration, 0.04, 0,"认购", volatility, stockLastClose);
            return delta;
        }

        private double getEstimateBondPrice(DateTime date, string BondCode, string stockCode)
        {
            double estimate = 0;
            var bondInfo = getBondDailyInfo(date, BondCode);
            int days = DateTimeExtension.DateUtils.GetSpanOfTradeDays(date, bondInfo.conversionEndDate);
            var stockList = getPreviousStockList(date, stockCode, days);
            double stockStartPrice = 0;
            double bondStartPrice = 0;
            var stockLastClose = stockList[stockList.Count() - 1].Close;
            for (int i = stockList.Count()-1; i >=0;  i--)
            {
                if (stockList[i].Close<stockList[i-1].Close*1.09)//当前推第i天未涨停，利用该天作为基准
                {
                    stockStartPrice = stockList[i].Close;
                    bondStartPrice = getTodayBondClose(stockList[i].DateTime.Date, bondInfo.code);
                    break;
                }
            }
            double delta = getOptionDelta(date, BondCode, stockCode);
            if (stockStartPrice!=0 && bondStartPrice!=0)
            {
                estimate = bondStartPrice + (100 / bondInfo.conversionPrice) * delta * (stockLastClose - stockStartPrice);
            }
            return estimate;
        }


        private List<StockTransaction> getPreviousStockList(DateTime date, string code, int days = 30)
        {
            List<StockTransaction> list = new List<StockTransaction>();
            if (dailyData.ContainsKey(code) == false)
            {
                return list;
            }
            var data = dailyData[code];

            for (int i = 1; i < data.Count(); i++)
            {
                if (data[i].DateTime == date)
                {
                    for (int j = Math.Max(0, i - days); j <= i - 1; j++)
                    {
                        list.Add(data[j]);
                    }
                    break;
                }
            }
            return list;
        }

        private List<double> getPreviousStockCloseList(DateTime date,string code,int days=30)
        {
            List<double> list = new List<double>();
            if (dailyData.ContainsKey(code) == false)
            {
                return list;
            }
            var data = dailyData[code];

            for (int i = 1; i < data.Count(); i++)
            {
                if (data[i].DateTime == date)
                {
                    for (int j = Math.Max(0,i-days); j <= i-1; j++)
                    {
                        list.Add(data[j].Close);
                    }
                    break;
                }
            }
            return list;
        }


        private double getTodayBondClose(DateTime date, string code)
        {
            double closePrice = 0;
            if (dailyData.ContainsKey(code) == false)
            {
                return closePrice;
            }
            var data = dailyData[code];

            for (int i = 1; i < data.Count(); i++)
            {
                if (data[i].DateTime == date)
                {
                    var dataToday = data[i];
                    var dataYesterday = data[i - 1];
                    closePrice = dataToday.Close;
                }
            }
            return closePrice;
        }

        private double getPreviousBondClose(DateTime date, string code)
        {
            double closePrice = 0;
            if (dailyData.ContainsKey(code) == false)
            {
                return closePrice;
            }
            var data = dailyData[code];

            for (int i = 1; i < data.Count(); i++)
            {
                if (data[i].DateTime == date)
                {
                    var dataToday = data[i];
                    var dataYesterday = data[i - 1];
                    closePrice = dataYesterday.Close;
                }
            }
            return closePrice;
        }

        private ConvertibleBondDailyInfo getBondDailyInfo(DateTime date,string code)
        {
            ConvertibleBondDailyInfo info = new ConvertibleBondDailyInfo();
            if (bondDailyInfo.ContainsKey(code)==false)
            {
                return info;
            }
            var data = bondDailyInfo[code];
            foreach (var item in data)
            {
                info = item;
                if (item.date.Date==date.Date)
                {
                    break;
                }
            }
            return info;
        }

        private double getPreviousAmount(DateTime date,string code)
        {
            double amount = 0;
            if (dailyData.ContainsKey(code) == false)
            {
                return amount;
            }
            var data = dailyData[code];

            for (int i = 1; i < data.Count(); i++)
            {
                if (data[i].DateTime == date)
                {
                    var dataToday = data[i];
                    var dataYesterday = data[i - 1];
                    amount = dataYesterday.Amount;
                }
            }
            return amount;
        }


        private double getCeilingPrice(DateTime date,string code)
        {
            double price = 0;
            if (dailyData.ContainsKey(code)==false)
            {
                return price;
            }
            var data = dailyData[code];

            for (int i = 1; i < data.Count(); i++)
            {
                if (data[i].DateTime==date)
                {
                    var dataToday = data[i];
                    var dataYesterday = data[i - 1];
                    price = Math.Round(dataYesterday.Close * dataYesterday.AdjFactor / dataToday.AdjFactor * 1.1, 2);
                }
            }
            return price;
        }


        private List<ConvertibleBondInfo> GetConvertibleBondInfos(DateTime date)
        {
            List<ConvertibleBondInfo> info = new List<ConvertibleBondInfo>();
            List<string> codeList = new List<string>();
            string dateStr = date.ToString("yyyy-MM-dd");
            string optionStr = string.Format("date={0};sectorid=a101020600000000", dateStr);
            var dt = windReader.GetDataSetTable("sectorconstituent", optionStr);
            foreach (DataRow dr in dt.Rows)
            {
                string code = Convert.ToString(dr["wind_code"]);
                string name = Convert.ToString(dr["sec_name"]);
                string[] strList = code.Split('.');
                string market = strList[1];
                if (market=="SH" || market=="SZ")
                {
                    if (codeList.Contains(code)==false)
                    {
                        try
                        {
                            codeList.Add(code);
                            var dt2 = windReader.GetDailyDataTable(code, "underlyingcode,ipo_date,delist_date", date, date);
                            foreach (DataRow dr2 in dt2.Rows)
                            {
                                string stockCode = Convert.ToString(dr2["UNDERLYINGCODE"]);
                                string stockMarket = stockCode.Split('.')[1];
                                if (stockMarket=="SH" || stockMarket=="SZ")
                                {
                                    DateTime startTime = Convert.ToDateTime(dr2["IPO_DATE"]);
                                    DateTime endTime = Convert.ToDateTime(dr2["DELIST_DATE"]);
                                    ConvertibleBondInfo infoNow = new ConvertibleBondInfo();
                                    infoNow.code = code;
                                    infoNow.endDate = endTime;
                                    infoNow.startDate = startTime;
                                    infoNow.stockCode = stockCode;
                                    infoNow.name = name;
                                    info.Add(infoNow);
                                }
                            }
                        }
                        catch (Exception)
                        {

                            Console.WriteLine("bond:{0},no information!", code);
                        }


                        
                    }
                }
            }
            return info;

        }

        private string getConvetibleCodeByStockCode(string stock,DateTime date,List<ConvertibleBondInfo> info)
        {
            string bond = "";
            foreach (var item in info)
            {
                if (item.stockCode==stock && date.Date>=item.startDate && date.Date<=item.endDate)
                {
                    bond = item.code;
                }
            }
            return bond;
        }

        private void dataPrepare(DateTime startDate, DateTime endDate)
        {
            //获取交易日信息
            this.tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            List<DateTime> ceilDate = new List<DateTime>();
            string bondCode;
            string underlyingCode;
            //获取可转债信息
            this.bondInfo=GetConvertibleBondInfos(endDate);
            //获取日线数据
            try
            {
                foreach (var info in bondInfo)
                {
                    underlyingCode = info.stockCode;
                    DateTime startTime = startDate;
                    DateTime endTime = endDate;
                    if (startTime<info.startDate)
                    {
                        startTime = info.startDate;
                    }
                    if (endTime>info.endDate)
                    {
                        endTime = info.endDate;
                    }
                    if (dailyData.ContainsKey(underlyingCode) == false)
                    {
                        var underlyingData = stockDailyRepo.GetStockTransaction(underlyingCode, startTime.AddDays(-10), endTime);
                        dailyData.Add(underlyingCode, underlyingData);
                        endTime = DateTimeExtension.DateUtils.PreviousTradeDay(info.endDate, 7);
                        if (endTime>endDate.Date)
                        {
                            endTime = endDate.Date;
                        }
                        if (startDate>endTime)
                        {
                            startDate = endTime;
                        }
                        var bondData = stockDailyRepo.GetStockTransaction(info.code, info.startDate,endTime);
                        dailyData.Add(info.code, bondData);
                        if (info.startDate>endTime)
                        {
                            continue;
                        }
                        var tempDataTable = windReader.GetDailyDataTable(info.code, "clause_conversion2_swapshareprice,underlyingcode,clause_conversion_2_swapsharestartdate,clause_conversion_2_swapshareenddate", info.startDate, endTime);
                        List<ConvertibleBondDailyInfo> bondDaily = new List<ConvertibleBondDailyInfo>();
                        foreach (DataRow dt in tempDataTable.Rows)
                        {
                            ConvertibleBondDailyInfo bondDailyInfoNow = new ConvertibleBondDailyInfo();
                            bondDailyInfoNow.code = info.code;
                            bondDailyInfoNow.name = info.name;
                            bondDailyInfoNow.startDate = info.startDate;
                            bondDailyInfoNow.endDate = info.endDate;
                            bondDailyInfoNow.stockCode = info.stockCode;
                            bondDailyInfoNow.conversionPrice = Convert.ToDouble(dt["clause_conversion2_swapshareprice"]);
                            //bondDailyInfoNow.forceConvertDate = Convert.ToDateTime(dt["clause_conversion_2_forceconvertdate"]);
                            bondDailyInfoNow.conversionStartDate = Convert.ToDateTime(dt["clause_conversion_2_swapsharestartdate"]);
                            bondDailyInfoNow.conversionEndDate = Convert.ToDateTime(dt["clause_conversion_2_swapshareenddate"]);
                            bondDailyInfoNow.date = Convert.ToDateTime(dt["datetime"]);
                            bondDaily.Add(bondDailyInfoNow);
                        }
                        bondDailyInfo.Add(info.code, bondDaily);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            int num = 0;
            foreach (var item in dailyData)
            {
                var data = item.Value;
                var code = item.Key;
                num = num + 1;
                Console.WriteLine(num);
                for (int i = 1; i < data.Count(); i++)
                {
                    if (data[i] == null || data[i].DateTime.Date < startDate)
                    {
                        continue;
                    }
                    DateTime day = data[i].DateTime.Date;
                    
                    //获取其对应的可转债
                    bondCode = getConvetibleCodeByStockCode(code, day, bondInfo);
                    //判断是否涨停
                    var dataToday = data[i];
                    var dataYesterday = data[i - 1];
                    double price = Math.Round(dataYesterday.Close * dataYesterday.AdjFactor / dataToday.AdjFactor * 1.1, 2);
                    //获取日内数据
                    try
                    {
                        if (data[i].High >= 0.99*price && bondCode != "")
                        {
                            //获取分钟数据
                            //var data1 = stockMinutelyRepo.GetStockTransaction(bondCode, day, day);
                            //var data2 = stockMinutelyRepo.GetStockTransaction(code, day, day);
                            //if (minuteData.ContainsKey(data[i].DateTime) == true)
                            //{
                            //    minuteData[data[i].DateTime].Add(bondCode, data1);
                            //    minuteData[data[i].DateTime].Add(code, data2);
                            //}
                            //else
                            //{
                            //    Dictionary<string, List<StockTransaction>> dataNow = new Dictionary<string, List<StockTransaction>>();
                            //    dataNow.Add(bondCode, data1);
                            //    dataNow.Add(code, data2);
                            //    minuteData.Add(day, dataNow);
                            //}
                            //获取tick数据
                            DateTime startTime = day.Date + new TimeSpan(9, 30, 0);
                            DateTime endTime = day.Date + new TimeSpan(15, 0, 0);
                            var data3 = tickRepo.GetStockTransaction(bondCode, startTime, endTime);
                            var data4 = tickRepo.GetStockTransaction(code, startTime, endTime);
                            if (tickData.ContainsKey(day.Date) == true)
                            {
                                tickData[day.Date].Add(bondCode, data3);
                                tickData[day.Date].Add(code, data4);
                            }
                            else
                            {
                                Dictionary<string, List<StockTickTransaction>> dataNow = new Dictionary<string, List<StockTickTransaction>>();
                                dataNow.Add(bondCode, data3);
                                dataNow.Add(code, data4);
                                tickData.Add(day.Date, dataNow);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        Console.WriteLine("code:{0} date:{1} No data!", bondCode, day);
                    }


                    
                }
            }
        }
    }
}
