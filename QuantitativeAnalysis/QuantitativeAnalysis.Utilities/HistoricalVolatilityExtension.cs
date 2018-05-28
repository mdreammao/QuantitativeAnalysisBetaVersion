using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public class HistoricalVolatilityExtension
    {
        public static double getHistoricalVolatilityByClosePrice(List<double> data)
        {
            double vol = 0;
            double[] rate = new double[data.Count() - 1];
            double mean = 0;
            //将收盘价数据转化为收益率
            for (int i = 1; i < data.Count(); i++)
            {
                rate[i - 1] =Math.Log(data[i] / data[i - 1]);
                mean += rate[i - 1];
            }
            mean /= data.Count() - 1;
            for (int i = 0; i < data.Count()-1; i++)
            {
                vol += Math.Pow(rate[i] - mean, 2);
            }
            vol = Math.Sqrt(252.0*vol / (data.Count() - 2));
            return vol;
        }
    }
}
