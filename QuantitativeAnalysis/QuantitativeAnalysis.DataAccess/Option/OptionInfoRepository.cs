using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuantitativeAnalysis.DataAccess.Infrastructure;
using System.Configuration;
using System.IO;

namespace QuantitativeAnalysis.DataAccess.Option
{
    public class OptionInfoRepository
    {
        private WindReader windReader = new WindReader();
        private SqlServerWriter sqlWriter;
        public OptionInfoRepository(ConnectionType type)
        {
            sqlWriter = new SqlServerWriter(type);
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

        private void ClearExistedOptionInfo(string underlyingCode)
        {
            var sql = string.Format(@"delete [Common].dbo.[OptionInfo] where option_mark_code='{0}'",underlyingCode);
            sqlWriter.WriteChanges(sql);
        }
        
    }
}
