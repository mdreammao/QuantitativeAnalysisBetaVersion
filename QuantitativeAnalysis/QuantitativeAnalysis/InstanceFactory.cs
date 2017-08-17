using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Autofac;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.DataAccess.Stock;
using Autofac.Core;
using QuantitativeAnalysis.Transaction;
namespace QuantitativeAnalysis
{
    public class InstanceFactory
    {
        private static IContainer container;

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="constructArgs">
        /// T对象的构造函数的参数，如果有就传入，没有就空
        /// 主要两种方法：
        /// 类型型参数：new TypedParameter(typeof(string), "sectionName")
        /// 参数名传入：new NamedParameter("configSectionName", "sectionName")
        /// </param>
        /// <returns></returns>
        public static T Get<T>(params Parameter[] constructArgs)
        {
            return container.Resolve<T>(constructArgs);
        }

        public static void Initialize()
        {
            var builder = new ContainerBuilder();
            var currentAssembly = AppDomain.CurrentDomain.GetAssemblies();
            builder.RegisterType<StockMinuteTransactionSimulator>();
            builder.RegisterAssemblyTypes(currentAssembly);
            container = builder.Build();
        }
    }
}
