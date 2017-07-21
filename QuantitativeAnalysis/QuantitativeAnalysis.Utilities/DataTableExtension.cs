using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace QuantitativeAnalysis.Utilities
{
    public static class DataTableExtension
    {
        public static List<T> ToList<T>(this DataTable dt)
        {
            var res = new List<T>();
            if(dt!=null)
            {
                foreach (DataRow r in dt.Rows)
                {
                    var val =(T) Convert.ChangeType(r[0], typeof(T));
                    res.Add(val);
                }
            }
            return res;
        }
    }
}
