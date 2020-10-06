using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace ReplicacaoSimplificada
{
    public static class Query
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

        public static string GenerateInsert(DataTable colunasDestino, DataRow item)
        {
            var index = 0;
            var listaItem = new List<string>();

            if (!colunasDestino.CheckColumnExists(item))
                throw new Exception("Não foi possivel localizar o nome da coluna na tabela de destino.");

            foreach (DataColumn column in item.Table.Columns)
            {

                var seNull = Convert.ToBoolean(colunasDestino.Select($"COLUMN_NAME = '{column.ColumnName}'").First()["IS_NULLABLE"]);
                var tem = item.ItemArray[index].ToString();

                if (column.DataType == typeof(decimal) && !string.IsNullOrWhiteSpace(tem))
                    tem = tem.Replace(",", ".");
                else if (column.DataType == typeof(DateTime) && !string.IsNullOrWhiteSpace(tem))
                    tem = ((DateTime)item.ItemArray[index]).ToString("yyyyMMdd HH:mm:ss.fff");
                else if (column.DataType == typeof(byte[]) && !string.IsNullOrWhiteSpace(tem))
                    tem = System.Convert.ToBase64String((byte[])item.ItemArray[index]);

                index += 1;
                if (seNull && string.IsNullOrWhiteSpace(tem))
                    listaItem.Add("null");
                else if (column.DataType == typeof(byte[]))
                    listaItem.Add($"CAST('{tem}' AS VARBINARY(MAX))");
                else
                    listaItem.Add($"'{tem}'");
            }

            return $"Insert into { colunasDestino.Rows[0]["TABLE_SCHEMA"]}.{ colunasDestino.Rows[0]["TABLE_NAME"]} ({ string.Join(", ", colunasDestino.AsEnumerable().Select(s => s["COLUMN_NAME"]))}) values ({ string.Join(",", listaItem)})";
        }

        public static string GenerateUpdate(DataTable colunasDestino, DataRow item, string columnPK)
        {
            var index = 0;
            var listaItem = new List<string>();

            if (!colunasDestino.CheckColumnExists(item))
                throw new Exception("Não foi possivel localizar o nome da coluna na tabela de destino.");

            foreach (DataColumn column in item.Table.Columns)
            {
                var seNull = Convert.ToBoolean(colunasDestino.Select($"COLUMN_NAME = '{column.ColumnName}'").First()["IS_NULLABLE"]);
                if (columnPK == column.ColumnName)
                {
                    index += 1;
                    continue;
                }

                var tem = item.ItemArray[index].ToString();

                if (column.DataType == typeof(decimal))
                    tem = tem.Replace(",", ".");
                else if (column.DataType == typeof(DateTime) && !string.IsNullOrWhiteSpace(tem))
                    tem = ((DateTime)item.ItemArray[index]).ToString("yyyyMMdd HH:mm:ss.fff");
                else if (column.DataType == typeof(byte[]) && !string.IsNullOrWhiteSpace(tem))
                    tem = System.Convert.ToBase64String((byte[])item.ItemArray[index]);
                index += 1;

                if (column.DataType != typeof(byte[]))
                    listaItem.Add($@"{column.ColumnName} = {(seNull && string.IsNullOrWhiteSpace(tem) ? "null" : $"'{tem}'")}");
                else
                    listaItem.Add($@"{column.ColumnName} = {(seNull && string.IsNullOrWhiteSpace(tem) ? "null" : $"CAST('{tem}' AS VARBINARY(MAX))")}");

            }

            return $"update {colunasDestino.Rows[0]["TABLE_SCHEMA"]}.{colunasDestino.Rows[0]["TABLE_NAME"]} set {string.Join(", ", listaItem)} where {columnPK} = '{item[columnPK].ToString()}'";
        }

        public static string GenerateMerge(string sourceTable, string destinationSchema, string destinationTable, string columnPK, DataTable colunasDestino)
        {
            var listTypeString = new List<String>() { "nvarchar", "varchar", "varbinary" };
            var typePkIsString = colunasDestino.AsEnumerable().Where(x => x["COLUMN_NAME"].ToString() == columnPK).Any(s => listTypeString.Any(x => x == s["DATA_TYPE"].ToString()));
            var setIdentityInsertOn = string.Empty;
            var setIdentityInsertOff = string.Empty;
            
            if (!typePkIsString && colunasDestino.AsEnumerable().Any(x => x["IsIdentity"].ToString() == "1"))
            {
                setIdentityInsertOn = $"SET IDENTITY_INSERT {destinationSchema}.{destinationTable} ON";
                setIdentityInsertOff = $"SET IDENTITY_INSERT {destinationSchema}.{destinationTable} OFF";
            }

            var queryMerge = $@"
{setIdentityInsertOn}
MERGE {destinationSchema}.{destinationTable} AS TargetTable
    USING {sourceTable} AS SourceTable
    ON (TargetTable.[{columnPK}] {(typePkIsString ? "COLLATE DATABASE_DEFAULT " : "")}= SourceTable.[{columnPK}])
    WHEN NOT MATCHED BY TARGET
        THEN INSERT ({string.Join(", ", colunasDestino.AsEnumerable().Select(s => s["COLUMN_NAME"]))})
            VALUES ({string.Join(", ", colunasDestino.AsEnumerable().Select(s => "SourceTable." + s["COLUMN_NAME"]))})
    WHEN MATCHED
        THEN UPDATE SET
           {string.Join(", ", colunasDestino.AsEnumerable().Where(x => x["COLUMN_NAME"].ToString() != columnPK).Where(xx => xx["IsIdentity"].ToString() != "1").Select(s => "TargetTable." + s["COLUMN_NAME"] + " = " + "SourceTable." + s["COLUMN_NAME"]))}
;{setIdentityInsertOff}
";

            return queryMerge;
        }
    }
}
