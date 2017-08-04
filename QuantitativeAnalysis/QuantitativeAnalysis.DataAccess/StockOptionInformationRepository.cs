using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Configuration;
using QuantitativeAnalysis.Utilities;
using System.IO;

namespace QuantitativeAnalysis.DataAccess
{
    public class StockOptionInformationRepository
    {
        public StockOptionInformationRepository(ConnectionType type)
        {
            sqlReader = new SqlServerReader(type);
            sqlWriter = new SqlServerWriter(type);
        }
        private SqlServerReader sqlReader;
        private SqlServerWriter sqlWriter;
        private WindReader windReader = new WindReader();

    }
}
