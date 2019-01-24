using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public class ImpliedVolatilityExtension2
    {

        public static double[] erfList = new double[100000];

        //计算delta
        public static double ComputeOptionDelta(double strike, double duration, double modifiedDuration, double riskFreeRate, double StockRate, string optionType, double optionVolatility, double underlyingPrice)
        {
            double d1 = (Math.Log(underlyingPrice / strike) + (riskFreeRate + Math.Pow(optionVolatility, 2) / 2) * modifiedDuration) / (optionVolatility * Math.Sqrt(modifiedDuration));
            if (optionType == "认购")
            {
                return normcdf(d1);
            }
            else
            {
                return normcdf(d1) - 1;
            }
        }

        //计算gamma
        public static double ComputeOptionGamma(double strike, double duration, double modifiedDuration, double riskFreeRate, double StockRate, double optionVolatility, double underlyingPrice)
        {
            double d1 = (Math.Log(underlyingPrice / strike) + (riskFreeRate + Math.Pow(optionVolatility, 2) / 2) * modifiedDuration) / (optionVolatility * Math.Sqrt(modifiedDuration));
            double gamma = Math.Exp(-d1 * d1 / 2.0) / (Math.Sqrt(2 * Math.PI * modifiedDuration) * underlyingPrice * optionVolatility);
            return gamma;
        }


        //计算vega
        public static double ComputeOptionVega(double strike, double duration,double modifiedDuration,double riskFreeRate, double StockRate, double optionVolatility, double underlyingPrice)
        {
            double d1 = (Math.Log(underlyingPrice / strike) + (riskFreeRate + Math.Pow(optionVolatility, 2) / 2) * modifiedDuration) / (optionVolatility * Math.Sqrt(modifiedDuration));
            double vega = Math.Sqrt(modifiedDuration / 2.0 / Math.PI) * underlyingPrice * Math.Exp(-d1 * d1 / 2.0);
            return vega;
        }

        //计算theta
        public static double ComputeTheta(double strike, double duration, double modifiedDuration, double riskFreeRate, double StockRate, double optionVolatility, double underlyingPrice, string optionType)
        {
            double theta = ComputeThetaWithInterest(strike, duration, modifiedDuration, riskFreeRate, StockRate, optionVolatility, underlyingPrice, optionType) + ComputeThetaWithoutInterest(strike, duration, modifiedDuration, riskFreeRate, StockRate, optionVolatility, underlyingPrice, optionType);
            return theta;
        }


        //计算利息相关的theta
        public static double ComputeThetaWithInterest(double strike, double duration, double modifiedDuration, double riskFreeRate, double StockRate, double optionVolatility, double underlyingPrice, string optionType)
        {
            double d1 = (Math.Log(underlyingPrice / strike) + (riskFreeRate + Math.Pow(optionVolatility, 2) / 2) * modifiedDuration) / (optionVolatility * Math.Sqrt(modifiedDuration));
            double d2 = d1 - optionVolatility * Math.Sqrt(modifiedDuration);
            double theta = 0;
            if (optionType == "认购")
            {
                theta = -riskFreeRate * strike * Math.Exp(-riskFreeRate * duration) * normcdf(d2);
            }
            else
            {
                theta = riskFreeRate * strike * Math.Exp(-riskFreeRate * duration) * normcdf(-d2);
            }
            return theta;
        }


        //计算非利息相关的theta
        public static double ComputeThetaWithoutInterest(double strike, double duration, double modifiedDuration, double riskFreeRate, double StockRate, double optionVolatility, double underlyingPrice, string optionType)
        {
            double d1 = (Math.Log(underlyingPrice / strike) + (riskFreeRate + Math.Pow(optionVolatility, 2) / 2) * modifiedDuration) / (optionVolatility * Math.Sqrt(modifiedDuration));
            double d2 = d1 - optionVolatility * Math.Sqrt(modifiedDuration);
            double theta = 0;
            if (optionType == "认购")
            {
                theta = -underlyingPrice * optionVolatility * Math.Exp(-d1 * d1 / 2) / (2 * Math.Sqrt(modifiedDuration * 2 * Math.PI));
            }
            else
            {
                theta = -underlyingPrice * optionVolatility * Math.Exp(-d1 * d1 / 2) / (2 * Math.Sqrt(modifiedDuration * 2 * Math.PI));
            }
            return theta;
        }


        //计算rho
        public static double ComputeOptionRho(double strike, double duration, double modifiedDuration, double riskFreeRate, double StockRate, double optionVolatility, double underlyingPrice, string optionType)
        {
            double d1 = (Math.Log(underlyingPrice / strike) + (riskFreeRate + Math.Pow(optionVolatility, 2) / 2) * modifiedDuration) / (optionVolatility * Math.Sqrt(modifiedDuration));
            double d2 = d1 - optionVolatility * Math.Sqrt(modifiedDuration);
            double rho = 0;
            if (optionType=="认购")
            {
                rho = strike * duration * Math.Exp(-riskFreeRate * duration) * normcdf(d2);
            }
            else
            {
                rho = -strike * duration * Math.Exp(-riskFreeRate * duration) * normcdf(-d2);
            }
            return rho;
        }


        //计算期权价格
        public static double ComputeOptionPrice(double strike, double duration, double modifiedDuration,double riskFreeRate, double StockRate, string optionType, double optionVolatility, double underlyingPrice)
        {
            double etfPirce = underlyingPrice * Math.Exp(-StockRate * duration);
            return optionLastPrice(etfPirce, optionVolatility, strike, duration, modifiedDuration,riskFreeRate, optionType);
        }
        public static double ComputeImpliedVolatility(double strike, double duration, double modifiedDuration, double riskFreeRate, double StockRate, string optionType, double optionPrice, double underlyingPrice)
        {

            double etfPirce = underlyingPrice * Math.Exp(-StockRate * duration);
            return sigma(etfPirce, optionPrice, strike, duration,modifiedDuration,riskFreeRate, optionType);
        }
        public static double _StartPoint(double K, double T, double r, double call, double s)///K 是 执行价格 
        {
            double sigma = 0.1;
            double x = K * Math.Exp(-r * T); ///x是折现值
            double radicand = Math.Pow(call - (s - x) / 2, 2) - Math.Pow(s - x, 2) / Math.PI * (1 + x / s) / 2;
            if (radicand > 0)
            {
                sigma = 1 / Math.Sqrt(T) * Math.Sqrt(2 * Math.PI) / (s + x) * (call - (s - x) / 2 + Math.Sqrt(radicand));
            }
            return sigma;
        }
        /// <summary>
        /// 牛顿法计算看涨期权隐含波动率。利用简单的牛顿法计算期权隐含波动率。在计算中，当sigma大于3，认为无解并返回0
        /// </summary>
        /// <param name="callPrice">期权价格</param>
        /// <param name="spotPrice">标的价格</param>
        /// <param name="strike">期权行权价</param>
        /// <param name="duration">到期时间</param>
        /// <param name="modifiedDuration">根据交易日调整过的到期时间</param>
        /// <param name="r">无风险利率</param>
        /// <returns>返回隐含波动率</returns>
        public static double sigmaOfCall(double callPrice, double spotPrice, double strike, double duration, double modifiedDuration,double r)
        {
            double sigma = _StartPoint(strike, duration, r, callPrice, spotPrice), sigmaold = sigma;
            if (callPrice < spotPrice - strike * Math.Exp(-r * duration))
            {
                return 0;
            }
            for (int num = 0; num <= 50; num++)
            {
                sigmaold = sigma;
                double d1 = (Math.Log(spotPrice / strike) + (r + sigma * sigma / 2) * modifiedDuration) / (sigma * Math.Sqrt(modifiedDuration));
                double d2 = d1 - sigma * Math.Sqrt(modifiedDuration);
                double f_sigma = normcdf(d1) * spotPrice - normcdf(d2) * strike * Math.Exp(-r * duration);
                double df_sigma = spotPrice * Math.Sqrt(modifiedDuration) * Math.Exp(-d1 * d1 / 2) / (Math.Sqrt(2 * Math.PI));
                if (df_sigma <= 0.000001)
                {
                    break;
                }
                sigma = sigma + (callPrice - f_sigma) / df_sigma;
                if (Math.Abs(sigma - sigmaold) < 0.00001)
                {
                    break;
                }
            }
            if (sigma < 0)///sigma > 3 ||
            {
                sigma = 0;
            }

            return sigma;
        }

        /// <summary>
        /// 计算看跌期权隐含波动率。利用简单的牛顿法计算期权隐含波动率。在计算中，当sigma大于3，认为无解并返回0
        /// </summary>
        /// <param name="callPrice">期权价格</param>
        /// <param name="spotPrice">标的价格</param>
        /// <param name="strike">期权行权价</param>
        /// <param name="duration">到期时间</param>
        /// <param name="modifiedDuration">根据交易日调整过的到期时间</param>
        /// <param name="r">无风险利率</param>
        /// <returns>返回隐含波动率</returns>
        private static double sigmaOfPut(double putPrice, double spotPrice, double strike, double duration, double modifiedDuration,double r)
        {
            if ((putPrice + spotPrice - strike * Math.Exp(-r * duration)) < 0) // put价格太高，返回200%
            {
                return 2;
            }
            return sigmaOfCall(putPrice + spotPrice - strike * Math.Exp(-r * duration), spotPrice, strike, duration, modifiedDuration,r);
        }

        /// <summary>
        /// 二分法计算看涨期权隐含波动率。利用简单的牛顿法计算期权隐含波动率。在计算中，当sigma大于300%，认为无解并返回0
        /// </summary>
        /// <param name="optionPrice">期权价格</param>
        /// <param name="spotPrice">标的价格</param>
        /// <param name="strike">期权行权价</param>
        /// <param name="duration">期权到期日</param>
        /// <param name="r">无风险利率</param>
        /// <returns>返回隐含波动率</returns>
        public static double sigmaOfCallByBisection(double optionPrice, double spotPrice, double strike, double duration, double modifiedDuration,double r)
        {

            double low = 0, up = 3;
            double epsilon = 10000;
            double sigma = -1;
            if (optionPrice <= 0)
            {
                return sigma;
            }
            while (Math.Abs(up - low) > 0.000001)
            {
                double mid = (low + up) / 2;
                double callMid = callPrice(spotPrice, strike, mid, duration,modifiedDuration, r);
                epsilon = Math.Abs(callMid - optionPrice);
                if (callMid > optionPrice)
                {
                    up = mid;
                }
                else
                {
                    low = mid;
                }
                if (epsilon < 0.000001)
                {
                    sigma = mid;
                    break;
                }
                if (epsilon < 0.0001)
                {
                    sigma = mid;
                }
            }
            return sigma;
        }

        /// <summary>
        /// 二分法计算看跌期权隐含波动率。利用简单的牛顿法计算期权隐含波动率。在计算中，当sigma大于300%，认为无解并返回0
        /// </summary>
        /// <param name="optionPrice">期权价格</param>
        /// <param name="spotPrice">标的价格</param>
        /// <param name="strike">期权行权价</param>
        /// <param name="duration">期权到期日</param>
        /// <param name="r">无风险利率</param>
        /// <returns>返回隐含波动率</returns>
        public static double sigmaOfPutByBisection(double optionPrice, double spotPrice, double strike, double duration, double modifiedDuration,double r)
        {

            double low = 0, up = 3;
            double epsilon = 10000;
            double sigma = -1;
            if (optionPrice <= 0)
            {
                return sigma;
            }
            while (Math.Abs(up - low) > 0.000001)
            {
                double mid = (low + up) / 2;
                double putMid = putPrice(spotPrice, strike, mid, duration,modifiedDuration,r);
                epsilon = Math.Abs(putMid - optionPrice);
                if (putMid > optionPrice)
                {
                    up = mid;
                }
                else
                {
                    low = mid;
                }
                if (epsilon < 0.000001)
                {
                    sigma = mid;
                    break;
                }
                if (epsilon < 0.0001)
                {
                    sigma = mid;
                }
            }
            return sigma;
        }

        /// <summary>
        /// 利用期权价格等参数计算隐含波动率
        /// </summary>
        /// <param name="etfPrice">50etf价格</param>
        /// <param name="optionLastPrice">期权价格</param>
        /// <param name="strike">期权行权价</param>
        /// <param name="duration">到期时间</param>
        /// <param name="modifiedDuration">根据交易日调整过的到期时间</param>
        /// <param name="r">无风险利率</param>
        /// <param name="optionType">期权类型区分看涨还是看跌</param>
        /// <returns>返回隐含波动率</returns>
        public static double sigma(double etfPrice, double optionLastPrice, double strike, double duration, double modifiedDuration,double r, string optionType)
        {
            if (optionType.Equals("认购"))
            {
                return sigmaOfCall(optionLastPrice, etfPrice, strike, duration, modifiedDuration,r);
            }
            else if (optionType.Equals("认沽"))
            {
                return sigmaOfPut(optionLastPrice, etfPrice, strike, duration,modifiedDuration, r);
            }
            return 0;
        }

        /// <summary>
        /// 利用期权价格等参数计算隐含波动率
        /// </summary>
        /// <param name="futurePrice">50etf价格</param>
        /// <param name="optionLastPrice">期权价格</param>
        /// <param name="strike">期权行权价</param>
        /// <param name="duration">期权到日期</param>
        /// <param name="r">无风险利率</param>
        /// <param name="optionType">期权类型区分看涨还是看跌</param>
        /// <returns>返回隐含波动率</returns>
        public static double sigmaByFuture(double futurePrice, double optionLastPrice, double strike, double duration,double modifiedDuration, double r, string optionType)
        {
            if (optionType.Equals("认购"))
            {
                return sigmaOfCallByBisection(optionLastPrice, futurePrice * Math.Exp(-r * duration), strike, duration,modifiedDuration,r);
            }
            else if (optionType.Equals("认沽"))
            {
                return sigmaOfPutByBisection(optionLastPrice, futurePrice * Math.Exp(-r * duration), strike, duration,modifiedDuration,r);
            }
            return -1;
        }

        /// <summary>
        /// 根据隐含波动率计算期权价格
        /// </summary>
        /// <param name="etfPrice">50etf价格</param>
        /// <param name="sigma">隐含波动率</param>
        /// <param name="strike">期权行权价格</param>
        /// <param name="duration">期权到期日</param>
        /// <param name="r">无风险利率</param>
        /// <param name="optionType">期权类型看涨还是看跌</param>
        /// <returns>返回期权理论价格</returns>
        public static double optionLastPrice(double etfPrice, double sigma, double strike, double duration, double modifiedDuration,double r, string optionType)
        {
            if (optionType.Equals("认购"))
            {
                return callPrice(etfPrice, strike, sigma, duration,modifiedDuration,r);
            }
            else if (optionType.Equals("认沽"))
            {
                return putPrice(etfPrice, strike, sigma, duration, modifiedDuration,r);
            }
            return 0.0;
        }


        /// <summary>
        /// 计算看涨期权理论价格
        /// </summary>
        /// <param name="spotPrice">期权标的价格</param>
        /// <param name="strike">期权行权价</param>
        /// <param name="sigma">期权隐含波动率</param>
        /// <param name="duration">期权到期日</param>
        /// <param name="r">无风险利率</param>
        /// <returns>返回看涨期权理论价格</returns>
        private static double callPrice(double spotPrice, double strike, double sigma, double duration,double modifiedDuration,double r)
        {
            if (duration == 0)
            {
                return ((spotPrice - strike) > 0) ? (spotPrice - strike) : 0;
            }
            double d1 = (Math.Log(spotPrice / strike) + (r + sigma * sigma / 2) * modifiedDuration) / (sigma * Math.Sqrt(modifiedDuration));
            double d2 = d1 - sigma * Math.Sqrt(modifiedDuration);
            return normcdf(d1) * spotPrice - normcdf(d2) * strike * Math.Exp(-r * duration);
        }

        /// <summary>
        /// 计算看跌期权理论价格
        /// </summary>
        /// <param name="spotPrice">期权标的价格</param>
        /// <param name="strike">期权行权价</param>
        /// <param name="sigma">期权隐含波动率</param>
        /// <param name="duration">期权到期日</param>
        /// <param name="r">无风险利率</param>
        /// <returns>返回看跌期权理论价格</returns>
        private static double putPrice(double spotPrice, double strike, double sigma, double duration, double modifiedDuration,double r)
        {
            if (duration == 0)
            {
                return ((strike - spotPrice) > 0) ? (strike - spotPrice) : 0;
            }
            double d1 = (Math.Log(spotPrice / strike) + (r  + sigma * sigma / 2)*modifiedDuration) / (sigma * Math.Sqrt(modifiedDuration));
            double d2 = d1 - sigma * Math.Sqrt(modifiedDuration);
            return -normcdf(-d1) * spotPrice + normcdf(-d2) * strike * Math.Exp(-r * duration);
        }

        //erf近似计算
        private static double erfFast(double x)
        {
            double M = erfList.Count() / 10;
            if (erfList[erfList.Count() - 1] == 0) //初始化
            {

                for (int i = 0; i < erfList.Count(); i++)
                {
                    erfList[i] = erf(i / M);
                }
            }
            if (x >= 0)
            {
                return erfList[(int)Math.Round(Math.Min(x, 10) * M)];
            }
            else
            {
                return -erfList[-(int)Math.Round(Math.Max(x, -10) * M)];
            }
        }
        /// <summary>
        /// 辅助函数erf(x),利用近似的方法进行计算
        /// </summary>
        /// <param name="x">因变量x</param>
        /// <returns>返回etf(x)</returns>
        private static double erf(double x)
        {
            double tau = 0;
            double t = 1 / (1 + 0.5 * Math.Abs(x));
            tau = t * Math.Exp(-Math.Pow(x, 2) - 1.26551223 + 1.00002368 * t + 0.37409196 * Math.Pow(t, 2) + 0.09678418 * Math.Pow(t, 3) - 0.18628806 * Math.Pow(t, 4) + 0.27886807 * Math.Pow(t, 5) - 1.13520398 * Math.Pow(t, 6) + 1.48851587 * Math.Pow(t, 7) - 0.82215223 * Math.Pow(t, 8) + 0.17087277 * Math.Pow(t, 9));
            if (x >= 0)
            {
                return 1 - tau;
            }
            else
            {
                return tau - 1;
            }
        }

        /// <summary>
        /// 辅助函数normcdf(x)
        /// </summary>
        /// <param name="x">因变量x</param>
        /// <returns>返回normcdf(x)</returns>
        private static double normcdf(double x)
        {
            return 0.5 + 0.5 * erf(x / Math.Sqrt(2));
        }
    }
}
