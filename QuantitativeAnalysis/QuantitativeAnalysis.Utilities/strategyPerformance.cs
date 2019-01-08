using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public class strategyPerformance
    {
        public static double sharpeRatioByDailyNetValue(List<double> netvalue, double annualizedCoefficient=250,double r=0)
        {
            List<double> yieldRate = new List<double>();
            for (int i = 1; i < netvalue.Count(); i++)
            {
                yieldRate.Add(netvalue[i] / netvalue[i - 1] - 1);
            }
            double AnnuliezdStd =  strategyPerformance.std(yieldRate)*Math.Sqrt(annualizedCoefficient);
            double mean = (netvalue[netvalue.Count() - 1] / netvalue[0]-1) / netvalue.Count() * annualizedCoefficient;
            double sharpe =(mean - r)/AnnuliezdStd;
            return sharpe;
        }

        public static double annulizedReturnByDailyNetValue(List<double> netvalue, double annualizedCoefficient = 250)
        {
            double annulizedReturn = (netvalue.Last() / netvalue.First() - 1) / netvalue.Count() * annualizedCoefficient;
            return annulizedReturn;
        }

        private static double std(List<double> list)
        {
            double avg = list.Average();
            double std = 0;
            foreach (var item in list)
            {
                std += Math.Pow(item - avg, 2);
            }
            std =Math.Sqrt(std / list.Count());
            return std;
        }
    }
}
