using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Data;
using System.Collections;

namespace QuantitativeAnalysis.Utilities
{
    public class DataRowConverter : JsonConverter
    {
        private readonly HashSet<string> ExceptFields = new HashSet<string>();
        public void AddExceptFields(string field)
        {
            ExceptFields.Add(field.ToLower());
        }
        public override bool CanConvert(Type objectType)
        {
            return typeof(DataRow) == objectType;
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var row = (DataRow)value;
            writer.WriteStartObject();
            foreach(DataColumn column in row.Table.Columns)
            {
                if (ExceptFields.Contains(column.ColumnName.ToLower()))
                    continue;
                writer.WritePropertyName(column.ColumnName);
                serializer.Serialize(writer, row[column]);
            }
            writer.WriteEndObject();
        }
    }
}
