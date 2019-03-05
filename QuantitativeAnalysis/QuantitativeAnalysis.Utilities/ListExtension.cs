using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public class ListExtension
    {
        public static double mean(List<double> list)
        {
            double mymean = 0;
            if (list.Count()==0)
            {
                return mymean;
            }
            foreach (var item in list)
            {
                mymean += item;
            }
            mymean /= list.Count();
            return mymean;
        }

        public static double absoluteMean(List<double> list)
        {
            double mymean = 0;
            if (list.Count() == 0)
            {
                return mymean;
            }
            foreach (var item in list)
            {
                mymean +=Math.Abs(item);
            }
            mymean /= list.Count();
            return mymean;
        }

        //public static IList<T> Clone<T>(this IList<T> listToClone) where T : ICloneable
        //{
        //    return listToClone.Select(item => (T)item.Clone()).ToList();
        //}
    }
}
