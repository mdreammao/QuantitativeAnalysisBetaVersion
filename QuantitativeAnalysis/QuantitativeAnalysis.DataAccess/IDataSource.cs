using QuantitativeAnalysis.DataAccess.Infrastructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuantitativeAnalysis.DataAccess
{
    public interface IDataSource
    {
        DataTable Get(string code, DateTime begin,DateTime end);

        DataTable GetFromSpecializedSQLServer(string code, DateTime date, ConnectionType type);
    }
}
