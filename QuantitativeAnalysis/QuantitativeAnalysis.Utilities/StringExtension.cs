using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.Utilities
{
    public static class StringExtension
    {
        public static DateTime ToDateTime(this string dtStr)
        {
            return DateTime.Parse(dtStr);
        }
        public static T ConvertTo<T>(this string val)
        {
            if (string.IsNullOrEmpty(val))
                return default(T);
            return (T)Convert.ChangeType(val, typeof(T));
        }
    }
}
