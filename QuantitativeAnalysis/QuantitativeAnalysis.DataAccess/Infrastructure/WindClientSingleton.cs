using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WAPIWrapperCSharp;

namespace QuantitativeAnalysis.DataAccess.Infrastructure
{
    public class WindClientSingleton
    {
        private static readonly WindAPI instance = new WindAPI();

        public static WindAPI Instance
        {
            get
            {
                if (!instance.isconnected())
                {
                    try
                    {
                        int code = instance.start();
                        if (code != 0)
                            throw new Exception(string.Format("不能打开Wind，错误代码：{0},{1}", code, instance.getErrorMsg(code)));
                    }
                    catch (Exception e)
                    {

                        Console.WriteLine(string.Format("不能打开Wind，错误代码：{0}", e.Message));
                    }
                    
                }
                return instance;
            }
        }
        ~WindClientSingleton()
        {
            if (instance.isconnected())
                instance.stop();
        }
    }
}
