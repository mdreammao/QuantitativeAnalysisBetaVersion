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
    public class TWAP
    {
        private TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        private Logger logger = LogManager.GetCurrentClassLogger();
        private string code;
        private Logger mylog = NLog.LogManager.GetCurrentClassLogger();
        private TransactionDateTimeRepository dateRepo;
        private StockTickRepository stockRepo;
        private int tradeId = 0;
        private int orderId = 0;
        private int eachOrderVolumeLimit = 10000;
        private int totalTime = 14402;
        private double tradingRate = 0.95;

        public TWAP(StockTickRepository stockRepo, TransactionDateTimeRepository dateRepo, string code)
        {
            this.code = code;
            this.stockRepo = stockRepo;
            this.dateRepo = dateRepo;
        }

        public void computeSTWAP(DateTime startDate, DateTime endDate, int totalVolume,int newOrderTimeInterval = 15,int oldOrderTimeInterval=15,int oldOrderFrequency=10,int newOrderPriceMode=1,int oldOrderPriceMode=-1,int cancelTimeInterval=240)
        {
            var tradedays = dateRepo.GetStockTransactionDate(startDate, endDate);
            foreach (var date in tradedays)
            {
                var data = stockRepo.GetStockTransaction(code, date, date);
                double mean = 0;
                double marketMean = 0;
                int seconds = 0;
                int dataIndex = 0;
                int dataSeconds;
                int untradeVolume = totalVolume;
                List<OrderBook> orderBook = new List<OrderBook>();
                List<TradeBook> tradeBook = new List<TradeBook>();
                //初始化每个周期的挂单量和补单量
                int period = totalVolume / newOrderTimeInterval;
                int orderVolumePerTimeInterval = getVolumeOfTimeInterval(untradeVolume, period);
                int stockNeedToTradePerTimeInterval = untradeVolume / period;
                int oldOrderVolumePerTimeInterval = 0;
                int orderNeedToTrade = 0;
                int orderAlreadyTrade = 0;
                //逐秒进行判断成交以及策略挂单
                for (seconds = 0; seconds <14401; seconds++)
                {
                    DateTime time =date.Date+DataTimeStampExtension.GetStockSecondsTimeByIndex(seconds);
                    //获取市场数据部分
                    var lastTick = onTick(seconds, data);
                    //根据最新盘口数据调整orderbook中waitingValume字段
                    modifyOrderBookByTickData(ref orderBook, lastTick);
                    //策略挂单撤单部分
                    double newOrderPrice = getOrderPrice(newOrderPriceMode, lastTick);
                    double oldOrderPrice = getOrderPrice(oldOrderPriceMode, lastTick);
                    if (seconds%newOrderTimeInterval==1)//每个新挂单周期的第1秒进行挂单
                    {
                        orderNeedToTrade = orderNeedToTrade + stockNeedToTradePerTimeInterval;
                        if (orderVolumePerTimeInterval>0)
                        {
                            orderId += 1;
                            double price = getOrderPrice(newOrderPriceMode, lastTick);
                            placeAnOrder(ref orderBook, lastTick, price, time, orderVolumePerTimeInterval, orderId);
                        }
                    }
                    
                    if (seconds% oldOrderTimeInterval == 5)//每个补单周期的第5秒进行补单
                    {
                        if (oldOrderVolumePerTimeInterval>0)
                        {
                            orderId += 1;
                            double price = getOrderPrice(oldOrderPriceMode, lastTick);
                            placeAnOrder(ref orderBook, lastTick, price, time, oldOrderVolumePerTimeInterval, orderId);
                        }
                    }

                    
                    if (seconds% cancelTimeInterval == 0) //每个撤单周期进行撤单，并计算需要补挂的单子
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
                        //移到下一个tick的数据
                        if (dataIndex+1<data.Count())
                        {
                            dataIndex = dataIndex + 1;
                        }
                        
                    }


                }

                

            }
        }

        private void placeAnOrder(ref List<OrderBook> orderBook,StockTickTransaction tickData,double price,DateTime time,int volume,int orderId)
        {
            var marketSummary = getMarketTickSummary(tickData);
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
        }

        //根据盘口数据更新orderbook中的waitingvolume字段
        private void modifyOrderBookByTickData(ref List<OrderBook> orderBook,StockTickTransaction tickData)
        {
            var marketSummary = getMarketTickSummary(tickData);
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
                    if (order.time < tickData.TransactionDateTime)
                    {
                        Console.WriteLine("Error!!");
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
                        break;
                    }
                    else
                    {
                        //完全成交
                        UpdateOrderBookWaitingVolume(price, orderBook[i].waitingVolume+orderBook[i].volume, i+1, ref orderBook);
                        
                        TradeBook trade = new TradeBook();
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
                    }
                }
            }
        }


        private void UpdateOrderBookWaitingVolume(double price,int volume,int index,ref List<OrderBook> orderBook)
        {
            for (int i = index; i < orderBook.Count(); i++)
            {
                if (orderBook[i].price==price)
                {
                    orderBook[i].waitingVolume -= volume;
                }
            }
        }

        private int getTradableVolumeFromMarket(StockTickTransaction tickData,double price)
        {
            int volume = 0;
            SortedDictionary<double, int> market = new SortedDictionary<double, int>();
            market.Add(tickData.Bid1, Convert.ToInt32(tickData.BidV1));
            market.Add(tickData.Bid2, Convert.ToInt32(tickData.BidV2));
            market.Add(tickData.Bid3, Convert.ToInt32(tickData.BidV3));
            market.Add(tickData.Bid4, Convert.ToInt32(tickData.BidV4));
            market.Add(tickData.Bid5, Convert.ToInt32(tickData.BidV5));
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
        private SortedDictionary<double, int> getMarketTickSummary(StockTickTransaction tickData)
        {
            SortedDictionary<double, int> market = new SortedDictionary<double, int>();
            market.Add(tickData.Bid1, Convert.ToInt32(tickData.BidV1));
            market.Add(tickData.Bid2, Convert.ToInt32(tickData.BidV2));
            market.Add(tickData.Bid3, Convert.ToInt32(tickData.BidV3));
            market.Add(tickData.Bid4, Convert.ToInt32(tickData.BidV4));
            market.Add(tickData.Bid5, Convert.ToInt32(tickData.BidV5));
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
