using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Model;

namespace QuantitativeAnalysis.Utilities
{
    public static class StockOptionExtension
    {
        public static StockOptionInformation GetParity(List<StockOptionInformation> list,StockOptionInformation option)
        {
            StockOptionInformation parity = new StockOptionInformation();
            foreach (var item in list)
            {
                if (item.strike==option.strike && item.expireDate==option.expireDate && item.underlying==option.underlying && item.type!=option.type && item.unit==option.unit)
                {
                    parity = item;
                    break;
                }
            }
            return parity;
        }

    }
}
