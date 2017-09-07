using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Utilities;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis.DataAccess;
using QuantitativeAnalysis.DataAccess.Stock;
using QuantitativeAnalysis.DataAccess.Option;
using NLog;
using Autofac;
using QuantitativeAnalysis.Transaction;


namespace QuantitativeAnalysis.Strategy
{
    public class PutCallParity
    {
        private DateTime startDate;
        private DateTime endDate;
        private double rate;
        static TypedParameter conn_type = new TypedParameter(typeof(ConnectionType), ConnectionType.Default);
        static Logger logger = LogManager.GetCurrentClassLogger();

        public PutCallParity(DateTime start, DateTime end, double rate = 0.04)
        {
            startDate = start;
            endDate = end;
            this.rate = rate;
        }

        public void statistics(DateTime date)
        {
            var repo = InstanceFactory.Get<OptionInfoRepository>(conn_type);

            repo.UpdateOptionInfo("510050.SH");
        }

    }
}

