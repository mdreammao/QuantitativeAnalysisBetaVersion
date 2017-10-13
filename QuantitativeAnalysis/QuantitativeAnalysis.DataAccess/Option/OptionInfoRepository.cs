using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using QuantitativeAnalysis.Model;
using System.Configuration;
using System.IO;
using QuantitativeAnalysis.DataAccess.Stock;
using NLog;

namespace QuantitativeAnalysis.DataAccess.Option
{
    public class OptionInfoRepository
    {
        private WindReader windReader = new WindReader();
        private SqlServerWriter sqlWriter;
        private const string RedisFieldFormat = "yyyy-MM-dd";
        private RedisReader redisReader = new RedisReader();
        private RedisWriter redisWriter = new RedisWriter();
        private SqlServerReader sqlReader;
        private TransactionDateTimeRepository dateRepo;
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public OptionInfoRepository(ConnectionType type)
        {
            sqlWriter = new SqlServerWriter(type);
            sqlReader = new SqlServerReader(type);
            dateRepo = new TransactionDateTimeRepository(type);
        }
       
        public List<StockOptionInformation> GetStockOptionInfo(string underlying,DateTime start,DateTime end)
        {
            logger.Info(string.Format("begin to fetch stock{0} Option Information from {1} to {2}...",underlying,start,end));
            var optionList=new List<StockOptionInformation>();
            var latestTradingDate = dateRepo.GetPreviousTransactionDate(DateTime.Now.AddDays(1));
            var dt=ReadFromSqlServer(underlying,start,end);
            var codeStr=underlying.Split('.');

            if (dt.Rows.Count==0 || Convert.ToDateTime(dt.Rows[0]["update_date_time"]).Date<latestTradingDate.Date)
            {
                UpdateOptionInfo(underlying);
                dt=ReadFromSqlServer(underlying,start,end);
            }
            foreach (DataRow item in dt.Rows)
            {
                var info=new StockOptionInformation();
                info.code=Convert.ToString(item["wind_code"])+"."+codeStr[1];
                info.name=Convert.ToString(item["sec_name"]);
                info.underlying=underlying;
                info.exerciseMode=Convert.ToString(item["exercise_mode"]);
                info.strike=Convert.ToDouble(item["exercise_price"]);
                info.type=Convert.ToString(item["call_or_put"]);
                info.unit=Convert.ToInt32(item["contract_unit"]);
                info.listedDate=Convert.ToDateTime(item["listed_date"]);
                info.expireDate=Convert.ToDateTime(item["expire_date"]);
                optionList.Add(info);
            }
            return optionList;
         }

        public void UpdateOptionInfo(string underlyingCode)
        {
            underlyingCode = underlyingCode.ToUpper();
            var exchange = underlyingCode.EndsWith(".SH") ? "sse" : throw new Exception("暂不支持");
            var dt = windReader.GetDataSetTable("optioncontractbasicinfo", string.Format("exchange={0};windcode={1};status=all;field=wind_code,sec_name,option_mark_code,call_or_put,exercise_mode,exercise_price,contract_unit,listed_date,expire_date",
                exchange, underlyingCode));
            WriteToSqlServer(underlyingCode,dt);
        }

        private void WriteToSqlServer(string underlyingCode,DataTable dt)
        {
            ClearExistedOptionInfo(underlyingCode);
            sqlWriter.InsertBulk(dt, "[Common].dbo.[OptionInfo]");
        }

        private DataTable ReadFromSqlServer(string underlyingCode,DateTime start,DateTime end)
        {
            var sqlStr = string.Format("select * from [Common].dbo.[OptionInfo] where option_mark_code='{0}' and  listed_date<='{2}' and expire_date>'{1}';",underlyingCode,start,end);
            return sqlReader.GetDataTable(sqlStr);

        }

        private void ClearExistedOptionInfo(string underlyingCode)
        {
            var sql = string.Format(@"delete [Common].dbo.[OptionInfo] where option_mark_code='{0}'",underlyingCode);
            sqlWriter.WriteChanges(sql);
        }
        
    }
}
