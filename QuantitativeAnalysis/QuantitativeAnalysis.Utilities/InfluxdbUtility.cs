using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.Utilities;
using QuantitativeAnalysis.Model;
using QuantitativeAnalysis;
using Autofac;
using System.Data;
using System.Configuration;
using static QuantitativeAnalysis.Utilities.DateTimeExtension;
using InfluxData.Net.Common.Enums;
using InfluxData.Net.InfluxDb;
using InfluxData.Net.InfluxDb.Models;

namespace QuantitativeAnalysis.Utilities
{
    public class InfluxdbUtility
    {
        //声明InfluxDbClient
        private InfluxDbClient clientDb;
        public InfluxdbUtility(string url= "http://localhost:8086/", string user= "sa", string pwd= "maoheng0")
        {
            //连接InfluxDb的API地址、账号、密码
            var infuxUrl = url;
            var infuxUser = user;
            var infuxPwd = pwd;
            //创建InfluxDbClient实例
            clientDb = new InfluxDbClient(infuxUrl, infuxUser, infuxPwd, InfluxDbVersion.Latest);
        }

        /// <summary>
        /// 从InfluxDB中读取数据
        /// </summary>
        public async void GetData()
        {
            //传入查询命令，支持多条
            var queries = new[]
            {
                " SELECT * FROM Reading WHERE time> now() -  24h "
            };
            var dbName = "test0";

            //从指定库中查询数据
            var response = await clientDb.Client.QueryAsync(queries, dbName);
            //得到Serie集合对象（返回执行多个查询的结果）
            var series = response.ToList();
            //取出第一条命令的查询结果，是一个集合
            var list = series[0].Values;
            //从集合中取出第一条数据
            var info_model = list.FirstOrDefault();
        }



        /// <summary>
        /// 往InfluxDB中写入数据
        /// </summary>
        public async void AddData()
        {
            //基于InfluxData.Net.InfluxDb.Models.Point实体准备数据
            var point_model = new Point()
            {
                Name = "Reading",//表名
                Tags = new Dictionary<string, object>()
                {
                    { "Id",  161}
                },
                Fields = new Dictionary<string, object>()
                {
                    { "Val", "webInfo" }
                },
                Timestamp = DateTime.UtcNow
            };
            var dbName = "test0";

            //从指定库中写入数据，支持传入多个对象的集合
            var response = await clientDb.Client.WriteAsync(point_model, dbName);
        }


    }
}
