using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public static class Computor
    {
        public static List<KeyValuePair<T,T>> GetNoExistedInterval<T>(List<T> expected,List<T> existed)
        {
            var nonExisted = expected.Except(existed).ToList();
            var dic = new Dictionary<int, KeyValuePair<T, T>>();//index begin end
            foreach (var nit in nonExisted)
            {
                var index = expected.IndexOf(nit);
                if (!dic.ContainsKey(index - 1))
                    dic.Add(index, new KeyValuePair<T, T>(nit, nit));
                else
                {
                    dic.Add(index, new KeyValuePair<T, T>(dic[index - 1].Key, nit));
                    dic.Remove(index - 1);
                }
            }
            return dic.Values.ToList();
        }
    }
}
