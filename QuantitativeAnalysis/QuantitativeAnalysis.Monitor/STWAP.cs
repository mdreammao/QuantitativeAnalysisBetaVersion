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
using NLog;
using Autofac;




namespace QuantitativeAnalysis.Monitor
{
    public class STWAP
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockTickRepository stockRepo;
        private int tradeId = 0;
        private int orderId = 0;
        private int eachOrderVolumeLimit = 1000000;
        private int totalTime = 14222;
        private double tradingRate = 0.8;

        public STWAP(StockTickRepository stockRepo, TransactionDateTimeRepository dateRepo, string code)
        {
            this.code = code;
            this.stockRepo = stockRepo;
            this.dateRepo = dateRepo;
        }

        public void computeSTWAP(DateTime startDate, DateTime endDate, int totalVolume=10000000,int newOrderTimeInterval = 15,int oldOrderTimeInterval=15,int oldOrderFrequency=10,int newOrderPriceMode=5,int oldOrderPriceMode=5,int cancelTimeInterval=120)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {

                List<StockTickTransaction> data = new List<StockTickTransaction>();
                try
                {
                    data = stockRepo.GetStockTransaction(code, date, date);
                }
                catch (Exception)
                {
                    Console.WriteLine("code:{0},date:{1} No Data!!", code, date);
                }
                if (data.Count()==0)
                {
                    Console.WriteLine("code:{0},date:{1} No Data!!", code, date);
                    continue;
                }
                double mean = 0;
                double marketMean = 0;
                int seconds = 0;
                int dataIndex = 0;
                int dataSeconds;
                int untradeVolume = totalVolume;
                List<OrderBook> orderBook = new List<OrderBook>();
                List<TradeBook> tradeBook = new List<TradeBook>();
                //初始化每个周期的挂单量和补单量
                int period = totalTime / newOrderTimeInterval;
                int orderVolumePerTimeInterval = getVolumeOfTimeInterval(untradeVolume, period);
                int stockNeedToTradePerTimeInterval = untradeVolume / period;
                int oldOrderVolumePerTimeInterval = 0;
                int orderNeedToTrade = 0;
                int orderAlreadyTrade = 0;
                int counting = 0;
                //逐秒进行判断成交以及策略挂单
                for (seconds = 0; seconds <=14221; seconds++)
                {
                    DateTime time =date.Date+DataTimeStampExtension.GetStockSecondsTimeByIndex(seconds);
                    //获取市场数据部分
                    var lastTick = onTick(seconds, data);
                    if (lastTick.Bid1==0)
                    {
                        continue;
                    }
                    counting += 1;
                    marketMean = (marketMean * (counting - 1) + lastTick.LastPrice) / counting;//计算市场的平均价格
                    //根据最新盘口数据调整orderbook中waitingValume字段
                    modifyOrderBookByTickData(ref orderBook, lastTick);
                    //策略挂单撤单部分
                    double newOrderPrice = getOrderPrice(newOrderPriceMode, lastTick);
                    double oldOrderPrice = getOrderPrice(oldOrderPriceMode, lastTick);
                    if (counting % newOrderTimeInterval==1)//每个新挂单周期的第1秒进行挂单
                    {
                        orderNeedToTrade = orderNeedToTrade + stockNeedToTradePerTimeInterval;
                        if (orderVolumePerTimeInterval>0)
                        {
                            orderId += 1;
                            double price = getOrderPrice(newOrderPriceMode, lastTick);
                            placeAnOrder(ref orderBook, lastTick, price, time, orderVolumePerTimeInterval, orderId);
                        }
                    }
                    
                    if (counting % oldOrderTimeInterval == 5)//每个补单周期的第5秒进行补单
                    {

                        int activeOrderNumbers = getActivieOrderNumbers(orderBook);
                        int volumeNow = (int)(Math.Round((orderNeedToTrade - orderAlreadyTrade - activeOrderNumbers) / 100.0) * 100);
                        volumeNow = Math.Min(oldOrderVolumePerTimeInterval, volumeNow);
                        if (volumeNow > 0)
                        {
                            orderId += 1;
                            double price = getOrderPrice(oldOrderPriceMode, lastTick);
                            placeAnOrder(ref orderBook, lastTick, price, time, volumeNow, orderId);
                        }
                    }

                    
                    if (counting % cancelTimeInterval == 0) //每个撤单周期进行撤单，并计算需要补挂的单子
                    {
                        int activeOrderNumbers = getActivieOrderNumbers(orderBook);
                        cancelDeviateOrders(ref orderBook, lastTick);
                        if (orderAlreadyTrade / Convert.ToDouble(orderNeedToTrade) < tradingRate)
                        {
                            cancelPartialDeviateOrders(ref orderBook, lastTick);
                            activeOrderNumbers = getActivieOrderNumbers(orderBook);
                        }
                            //计算后续补单每次挂单量
                        int oldOrderUntrade = orderNeedToTrade - orderAlreadyTrade - activeOrderNumbers;
                        oldOrderVolumePerTimeInterval = getVolumeOfTimeInterval(oldOrderUntrade, oldOrderFrequency);
                    }
                    //判断成交部分
                    dataSeconds = DataTimeStampExtension.GetStockSecondsIndex(data[dataIndex].TransactionDateTime);
                    if (dataSeconds==seconds)
                    {
                        //根据该tick的盘口数据，进行成交判断
                        transactionSimulation(ref orderBook, ref tradeBook, data[dataIndex]);
                        //根据成交信息更新数据
                        orderAlreadyTrade = getTradedNumbers(tradeBook);
                        untradeVolume = totalVolume - orderAlreadyTrade;
                        //移到下一个tick的数据
                        if (dataIndex+1<data.Count())
                        {
                            dataIndex = dataIndex + 1;
                        }
                    }
                    //输出最后的交易情况
                    if (seconds == 14220)
                    {
                        mean = getTradeAveragePrice(tradeBook);
                        int orderVolume = getOrderBookVolume(orderBook);
                        Console.WriteLine("{0}: 市场均价 {1} 成交均价 {2} 成交量 {3} 需成交 {4} 挂单 {5}", time, marketMean, mean, orderAlreadyTrade, orderNeedToTrade, orderVolume);
                    }
                }
            }
        }

        private int getOrderBookVolume(List<OrderBook> orderBook)
        {
            int volume = 0;
            foreach (var item in orderBook)
            {
                volume += item.volume;
            }
            return volume;
        }

        private double getTradeAveragePrice(List<TradeBook> tradeBook)
        {
            double totalVolume = 0;
            double totalAmount = 0;
            foreach (var item in tradeBook)
            {
                totalVolume += item.volume;
                totalAmount += item.volume * item.price;
            }
            return totalAmount / totalVolume;
        }

        private void placeAnOrder(ref List<OrderBook> orderBook,StockTickTransaction tickData,double price,DateTime time,int volume,int orderId)
        {
            var marketSummary = getMarketWaitingVolumeSummary(tickData);
            var order = new OrderBook();
            order.time = time;
            order.price = price;
            order.orderId = orderId;
            order.volume = volume;
            order.code = tickData.Code;
            order.waitingVolume = 0;
            if (marketSummary.ContainsKey(price))
            {
                order.waitingVolume = marketSummary[price];
            }
            else if (marketSummary.Count()>0)
            {
                if (price>marketSummary.Keys.Max() && marketSummary.Keys.Max()>0)
                {
                    order.waitingVolume =2* marketSummary[marketSummary.Keys.Max()];
                }
            }
            orderBook.Add(order);
        }

        //根据盘口数据更新orderbook中的waitingvolume字段
        private void modifyOrderBookByTickData(ref List<OrderBook> orderBook,StockTickTransaction tickData)
        {
            var marketSummary = getMarketWaitingVolumeSummary(tickData);
            var orderSummary = getOrderBookSummary(orderBook);
            foreach (var market in marketSummary)
            {
                if (orderSummary.ContainsKey(market.Key) && orderSummary[market.Key]>0)
                {
                    //从后往前更新数据
                    int waitingVolume =market.Value;
                    for (int i = orderBook.Count()-1; i >= 0; i--)
                    {
                        var order = orderBook[i];
                        if (order.price==market.Key && order.volume>0)
                        {
                            order.waitingVolume = waitingVolume;
                            waitingVolume = Math.Max(waitingVolume-order.volume,0);
                        }
                    }

                }
            }

        }

        //撤单，撤掉一档行情之外的挂单
        private void cancelPartialDeviateOrders(ref List<OrderBook> orderBook, StockTickTransaction tickData)
        {
            foreach (var item in orderBook)
            {
                if (item.price > tickData.Ask1)
                {
                    item.volume = 0;
                    item.waitingVolume = 0;
                }
            }
        }

        //撤单，撤掉五档行情之外的挂单
        private void cancelDeviateOrders(ref List<OrderBook> orderBook,StockTickTransaction tickData)
        {
            foreach (var item in orderBook)
            {
                if (item.price>tickData.Ask5)
                {
                    item.volume = 0;
                    item.waitingVolume = 0;
                }
            }
        }

        private int getTradedNumbers(List<TradeBook> tradeBook)
        {
            var tradeBookSummary = getTradeBookSummary(tradeBook);
            int numbers = 0;
            foreach (var item in tradeBookSummary)
            {
                if (item.Value>0)
                {
                    numbers += item.Value;
                }
            }
            return numbers;
        }

        private int getActivieOrderNumbers(List<OrderBook> orderBook)
        {
            var orderBookSummary = getOrderBookSummary(orderBook);
            int numbers = 0;
            foreach (var item in orderBookSummary)
            {
                if (item.Value>0)
                {
                    numbers += item.Value;
                }
            }
            return numbers;
        }

        private int getVolumeOfTimeInterval(int totalVolume,int numberOfTimeInterval)
        {
            //四舍五入
            int volume = (int)(Math.Round(Convert.ToDouble( totalVolume) / (numberOfTimeInterval * 100.0)) * 100);
            volume = Math.Min(volume, eachOrderVolumeLimit);
            volume = getRound2Digital(volume);
            return volume;
        }


        private double getOrderPrice(int orderPriceMode,StockTickTransaction tickData)
        {
            double price = -1;
            switch (orderPriceMode)
            {
                case 5: 
                    price = tickData.Ask5;
                    break;
                case 4:
                    price = tickData.Ask4;
                    break;
                case 3:
                    price = tickData.Ask3;
                    break;
                case 2:
                    price = tickData.Ask2;
                    break;
                case 1:
                    price = tickData.Ask1;
                    break;
                case -1:
                    price = tickData.Bid1;
                    break;
                case -2:
                    price = tickData.Bid2;
                    break;           
                case -3:             
                    price = tickData.Bid3;
                    break;           
                case -4:             
                    price = tickData.Bid4;
                    break;           
                case -5:             
                    price = tickData.Bid5;
                    break;           
                default:
                    break;
            }
            if (price==0 && tickData.Bid1!=0)
            {
                price = tickData.Bid1;
            }
            return price;
        }


        private int getRound2Digital(int volume)
        {
            int roundVolume = 0;
            roundVolume = Convert.ToInt32((Math.Round(Convert.ToDouble(volume/100.0), 2)) * 100);
            return roundVolume;
        }

        private StockTickTransaction onTick(int seconds,List<StockTickTransaction> data)
        {
            StockTickTransaction tick = new StockTickTransaction();
            int secondsIndex = DataTimeStampExtension.GetStockSecondsIndex(data[0].TransactionDateTime);
            for (int i = 0; i < data.Count(); i++)
            {
                secondsIndex = DataTimeStampExtension.GetStockSecondsIndex(data[i].TransactionDateTime);
                if (secondsIndex<=seconds)
                {
                    tick = data[i];
                }
                else
                {
                    break;
                }
            }
            return tick;
        }

        //按盘口价格模拟成交
        private void transactionSimulation(ref List<OrderBook> orderBook,ref List<TradeBook> tradeBook,StockTickTransaction tickData)
        {
            //如果orderbook上的挂单时间早于tickData数据的时间，认为orderbook的挂单完全成交。
            for (int i = 0; i < orderBook.Count(); i++)
            {
                var order = orderBook[i];
                if (order.time<tickData.TransactionDateTime)
                {
                    if (order.price<=tickData.Bid1 && order.volume>0) //若order的卖价小于等于bid1的价格，认为其在该时刻已经成交了。
                    {
                        //记录成交信息
                        TradeBook trade = new TradeBook();
                        trade.tradeId = tradeId;
                        tradeId += 1;
                        trade.time = tickData.TransactionDateTime;
                        trade.price = order.price;
                        trade.volume = order.volume;
                        trade.orderId = order.orderId;
                        //刷新orderbook情况
                        UpdateOrderBookWaitingVolume(order.price, orderBook[i].waitingVolume + orderBook[i].volume, i + 1, ref orderBook);
                        order.waitingVolume = 0;
                        order.volume = 0;
                        tradeBook.Add(trade);
                        printTradingLog(trade, orderBook, tickData);
                    }
                }
            }
            //如果orderbook上的挂单时间等于或者晚于tickData数据的时间，按照盘口成交，并减少盘口的量。
            var summary = getOrderBookSummary(orderBook);//先汇总挂单价格
            //按挂单价格遍历
            foreach (var price in summary.Keys)
            {
                for (int i = 0; i < orderBook.Count(); i++)//按order挂单的先后顺序成交
                {
                    int tradableVolume = getTradableVolumeFromMarket(tickData, price);
                    int tradedVolume = 0;
                    if (tradableVolume<=0)
                    {
                        break;
                    }
                    var order = orderBook[i];
                    if (orderBook[i].volume==0 || orderBook[i].price!=price)
                    {
                        continue;
                    }
                    //先成交他人的订单
                    if (tradableVolume <= orderBook[i].waitingVolume)
                    {
                        //无法成交
                        UpdateOrderBookWaitingVolume(price, tradableVolume, i, ref orderBook);
                        tradedVolume = tradableVolume;
                        tradableVolume = 0;
                        UpdateTickData(ref tickData, tradedVolume);
                        break;
                    }
                    //再成交策略的挂单
                    else if (tradableVolume <= orderBook[i].waitingVolume + orderBook[i].volume)
                    {
                        //部分成交
                        UpdateOrderBookWaitingVolume(price, tradableVolume, i+1, ref orderBook);
                        TradeBook trade = new TradeBook();
                        trade.tradeId = tradeId;
                        tradeId += 1;
                        trade.time = tickData.TransactionDateTime;
                        trade.price = price;
                        trade.volume = tradableVolume - orderBook[i].waitingVolume;
                        trade.orderId = orderBook[i].orderId;
                        UpdateOrderBookWaitingVolume(price, tradableVolume, i + 1, ref orderBook);
                        orderBook[i].volume -= tradableVolume - orderBook[i].waitingVolume;
                        orderBook[i].waitingVolume = 0;
                        tradedVolume = tradableVolume;
                        UpdateTickData(ref tickData, tradedVolume);
                        tradableVolume = 0;
                        tradeBook.Add(trade);
                        printTradingLog(trade, orderBook, tickData);
                        break;
                    }
                    else
                    {
                        //完全成交
                        UpdateOrderBookWaitingVolume(price, orderBook[i].waitingVolume+orderBook[i].volume, i+1, ref orderBook);
                        
                        TradeBook trade = new TradeBook();
                        trade.code = code;
                        trade.tradeId = tradeId;
                        tradeId += 1;
                        trade.time = tickData.TransactionDateTime;
                        trade.price = price;
                        trade.volume = orderBook[i].volume;
                        trade.orderId = orderBook[i].orderId;
                        UpdateOrderBookWaitingVolume(price, tradableVolume, i + 1, ref orderBook);
                        tradedVolume = orderBook[i].waitingVolume + orderBook[i].volume;
                        orderBook[i].volume = 0;
                        orderBook[i].waitingVolume = 0;
                        UpdateTickData(ref tickData, tradedVolume);
                        tradableVolume -= tradedVolume;
                        tradeBook.Add(trade);
                        printTradingLog(trade, orderBook, tickData);
                    }
                }
            }
        }

        private void printTradingLog(TradeBook trade,List<OrderBook> orderBook,StockTickTransaction tickData)
        {
            //foreach (var order in orderBook)
            //{
            //    if (trade.orderId==order.orderId)
            //    {
            //        Console.WriteLine("成交时间:{0},挂单时间:{1},成交量:{2},成交价:{3},ask1:{4},bid1:{5},lp：{6}", trade.time, order.time, trade.volume, trade.price, tickData.Ask1, tickData.Bid1, tickData.LastPrice);
            //    }
            //}
        }


        private void UpdateOrderBookWaitingVolume(double price,int volume,int index,ref List<OrderBook> orderBook)
        {
            for (int i = index; i < orderBook.Count(); i++)
            {
                if (orderBook[i].price==price)
                {
                    orderBook[i].waitingVolume -= volume;
                    if (orderBook[i].waitingVolume<0)
                    {
                        orderBook[i].waitingVolume = 0;
                    }
                }
            }
        }

        private int getTradableVolumeFromMarket(StockTickTransaction tickData,double price)
        {
            int volume = 0;
            SortedDictionary<double, int> market = new SortedDictionary<double, int>();
            if (tickData.Bid1*tickData.BidV1>0)
            {
                market.Add(tickData.Bid1, Convert.ToInt32(tickData.BidV1));
            }
            if (tickData.Bid2 * tickData.BidV2 > 0)
            {
                market.Add(tickData.Bid2, Convert.ToInt32(tickData.BidV2));
            }
            if (tickData.Bid3 * tickData.BidV3 > 0)
            {
                market.Add(tickData.Bid3, Convert.ToInt32(tickData.BidV3));
            }
            if (tickData.Bid4 * tickData.BidV4 > 0)
            {
                market.Add(tickData.Bid4, Convert.ToInt32(tickData.BidV4));
            }
            if (tickData.Bid5 * tickData.BidV5 > 0)
            {
                market.Add(tickData.Bid5, Convert.ToInt32(tickData.BidV5));
            }
            foreach (var item in market)
            {
                if (item.Key>=price)
                {
                    volume = +item.Value;
                }
            }
            return volume;
        }
        
        private void UpdateTickData(ref StockTickTransaction tickData,int volume)
        {
            int residualVolume = volume;
            residualVolume -=Convert.ToInt32(Math.Min(volume, tickData.BidV1));
            tickData.BidV1 -= Math.Min(volume, tickData.BidV1);
            volume = residualVolume;
            residualVolume -= Convert.ToInt32(Math.Min(volume, tickData.BidV2));
            tickData.BidV2 -= Math.Min(volume, tickData.BidV2);
            volume = residualVolume;
            residualVolume -= Convert.ToInt32(Math.Min(volume, tickData.BidV3));
            tickData.BidV3 -= Math.Min(volume, tickData.BidV3);
            volume = residualVolume;
            residualVolume -= Convert.ToInt32(Math.Min(volume, tickData.BidV4));
            tickData.BidV4 -= Math.Min(volume, tickData.BidV4);
            volume = residualVolume;
            residualVolume -= Convert.ToInt32(Math.Min(volume, tickData.BidV5));
            tickData.BidV5 -= Math.Min(volume, tickData.BidV5);
            volume = residualVolume;
        }

        //根据挂单情况获取可成交汇总
        private SortedDictionary<double, int> getMarketWaitingVolumeSummary(StockTickTransaction tickData)
        {
            SortedDictionary<double, int> market = new SortedDictionary<double, int>();
            if (tickData.Ask1*tickData.AskV1>0)
            {
                market.Add(tickData.Ask1, Convert.ToInt32(tickData.AskV1));
            }
            if (tickData.Ask2*tickData.AskV2>0)
            {
                market.Add(tickData.Ask2, Convert.ToInt32(tickData.AskV2));
            }
            if (tickData.Ask3 * tickData.AskV3 > 0)
            {
                market.Add(tickData.Ask3, Convert.ToInt32(tickData.AskV3));
            }
            if (tickData.Ask4 * tickData.AskV4 > 0)
            {
                market.Add(tickData.Ask4, Convert.ToInt32(tickData.AskV4));
            }
            if (tickData.Ask5 * tickData.AskV5 > 0)
            {
                market.Add(tickData.Ask5, Convert.ToInt32(tickData.AskV5));
            }
            return market;
        }

        //根据orderbook获取挂单汇总
        private SortedDictionary<double,int> getOrderBookSummary(List<OrderBook> book)
        {
            SortedDictionary<double, int> summary = new SortedDictionary<double, int>();
            foreach (var order in book)
            {
                if (order.volume>0)
                {
                    if (summary.ContainsKey(order.price))
                    {
                        summary[order.price] += order.volume;
                    }
                    else
                    {
                        summary.Add(order.price, order.volume);
                    }
                }
            }

            return summary;
        }

        private Dictionary<double, int> getTradeBookSummary(List<TradeBook> book)
        {
            Dictionary<double, int> summary = new Dictionary<double, int>();
            foreach (var trade in book)
            {
                if (trade.volume > 0)
                {
                    if (summary.ContainsKey(trade.price))
                    {
                        summary[trade.price] += trade.volume;
                    }
                    else
                    {
                        summary.Add(trade.price, trade.volume);
                    }
                }
            }
            return summary;
        }


    }
}
