using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public class MathUtility
    {

        //计算两个序列之间的相关性
        public static double correlation(double[] x, double[] y)
        {
            int length = x.Length;
            double xmean = 0;
            double ymean = 0;
            double cov = 0;
            double xvar = 0;
            double yvar = 0;
            xmean = x.Sum() / length;
            ymean = y.Sum() / length;
            for (int i = 0; i < length; i++)
            {
                cov = cov + (x[i] - xmean) * (y[i] - ymean);
                xvar = xvar + Math.Pow((x[i] - xmean), 2);
                yvar = yvar + Math.Pow((y[i] - ymean), 2);
            }
            double corr = cov / Math.Sqrt(xvar * yvar);
            return corr;
        }

        //original least square方法
        //y=alpha+beta*x+epsilon
        public static bool OLS(double[] x,double[] y,ref double alpha,ref double beta)
        {
            if (x.Length!=y.Length)
            {
                return false;   
            }
            int n = x.Length;
            double xsum = x.Sum();
            double ysum = y.Sum();
            double xsquaresum = 0;
            double xysum = 0;
            for (int i = 0; i < n; i++)
            {
                xsquaresum += x[i] * x[i];
                xysum += x[i] * y[i];
            }
            alpha = ((xsquaresum)*(ysum)-(xsum)*(xysum))/(n*xsquaresum-(xsum*xsum));
            beta = (n * xysum - xsum * ysum) / (n * xsquaresum - (xsum * xsum));
            return true;
        }

        public static double std(List<double> list)
        {
            double avg = list.Average();
            double std = 0;
            foreach (var item in list)
            {
                std += Math.Pow(item - avg, 2);
            }
            std = Math.Sqrt(std / list.Count());
            return std;
        }
    }
}
