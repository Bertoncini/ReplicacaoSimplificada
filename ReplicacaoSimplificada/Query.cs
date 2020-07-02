using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace ReplicacaoSimplificada
{
    internal static class Query
    {
        public static string ReturnTableColumnNameType => $@"
select 
c.TABLE_CATALOG
,C.TABLE_SCHEMA
,C.TABLE_NAME
,COLUMN_NAME = UPPER(C.COLUMN_NAME)
,IS_NULLABLE = CASE WHEN C.IS_NULLABLE  = 'YES' THEN 1 ELSE 0 END
,C.DATA_TYPE
,[Se_PK] = case when pk.COLUMN_NAME is not null then 1 else 0 end 
,[IsIdentity] = COLUMNPROPERTY(object_id(c.TABLE_SCHEMA+'.'+c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') 
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN (
            SELECT 
                    ku.TABLE_CATALOG,
                    ku.TABLE_SCHEMA,
                    ku.TABLE_NAME,
                    ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
         )   pk 
ON  c.TABLE_CATALOG = pk.TABLE_CATALOG Collate Database_Default
            AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA Collate Database_Default
            AND c.TABLE_NAME = pk.TABLE_NAME Collate Database_Default
            AND c.COLUMN_NAME = pk.COLUMN_NAME Collate Database_Default
where c.table_schema = @schema and c.table_Name = @table
and data_Type != 'timestamp'
";

        public static string GenerateInsert(DataTable colunasOrigem, DataTable colunasDestino, DataRow item)
        {
            var index = 0;
            var listaItem = new List<string>();
            foreach(DataColumn column in item.Table.Columns)
            {
                try
                {
                    var seNull = Convert.ToBoolean(colunasDestino.Select($"COLUMN_NAME = '{column.ColumnName}'").First()["IS_NULLABLE"]);
                    var tem = item.ItemArray[index].ToString();

                    if(column.DataType == typeof(decimal) && !string.IsNullOrWhiteSpace(tem))
                        tem = tem.Replace(",", ".");
                    else if(column.DataType == typeof(DateTime) && !string.IsNullOrWhiteSpace(tem))
                        tem = ((DateTime)item.ItemArray[index]).ToString("yyyyMMdd HH:mm:ss.fff");
                    else if (column.DataType == typeof(byte[]))
                        tem = System.Convert.ToBase64String((byte[])item.ItemArray[index]);

                    index += 1;
                    if (column.DataType != typeof(byte[]))
                        listaItem.Add($@"{(seNull && string.IsNullOrWhiteSpace(tem) ? "null" : $"'{tem}'")}");
                    else
                        listaItem.Add($@"CAST('{tem}' AS VARBINARY(MAX))");

                }
                catch (Exception ex)
                {
                    Program.Mensagem = ex.Message;
                    Console.WriteLine(Program.Mensagem);
                }
              
            }

            return $"Insert into {colunasOrigem.Rows[0]["TABLE_SCHEMA"]}.{colunasOrigem.Rows[0]["TABLE_NAME"]} ({string.Join(", ", colunasOrigem.AsEnumerable().Select(s => s["COLUMN_NAME"]))}) values ({string.Join(",", listaItem)})";

        }

        public static string GenerateUpdate(DataTable colunasOrigem, DataTable colunasDestino, DataRow item, string columnPK)
        {
            var index = 0;
            var listaItem = new List<string>();

            foreach(DataColumn column in item.Table.Columns)
            {
                var seNull = Convert.ToBoolean(colunasDestino.Select($"COLUMN_NAME = '{column.ColumnName}'").First()["IS_NULLABLE"]);
                if(columnPK == column.ColumnName)
                {
                    index += 1;
                    continue;
                }

                var tem = item.ItemArray[index].ToString();

                if(column.DataType == typeof(decimal))
                    tem = tem.Replace(",", ".");
                else if(column.DataType == typeof(DateTime) && !string.IsNullOrWhiteSpace(tem))
                    tem = ((DateTime)item.ItemArray[index]).ToString("yyyyMMdd HH:mm:ss.fff");
                index += 1;

                listaItem.Add($@" {column.ColumnName} = {(seNull && string.IsNullOrWhiteSpace(tem) ? "null" : $"'{tem}'")} ");
            }

            return $"update {colunasOrigem.Rows[0]["TABLE_SCHEMA"]}.{colunasOrigem.Rows[0]["TABLE_NAME"]} set  {string.Join(",", listaItem)} where {columnPK} = '{item[columnPK].ToString()}'";
        }
    }
}
