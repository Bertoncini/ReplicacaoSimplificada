using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace ReplicacaoSimplificada
{
    class Conexao : IDisposable
    {
        private string _connectionString;

        private SqlConnection _connection;

        public Conexao(string server, string database, string user, string password)
        {
            if (string.IsNullOrWhiteSpace(user) || string.IsNullOrWhiteSpace(password))
                _connectionString = $"Server={server};Database={database};Trusted_Connection=True;Application Name=ReplicacaoSimplificada";
            else
                _connectionString = $"Server={server};Database={database};User Id={user};Password={password};Application Name=ReplicacaoSimplificada";
        }

        private void validaConexao()
        {
            if (_connection == null || _connection.State != System.Data.ConnectionState.Open)
            {
                try
                {
                    _connection?.Close();
                }
                catch (Exception)
                {
                }
                _connection = new SqlConnection(_connectionString);

                if (_connection.State != System.Data.ConnectionState.Open)
                    _connection.Open();
            }
        }

        private DataTable returnDataTable(string query, SqlParameter[] parameters)
        {
            validaConexao();

            var command = _connection.CreateCommand();

            command.CommandText = query;
            command.Parameters.AddRange(parameters);

            var result = command.ExecuteReader();

            var dataTable = new DataTable();
            dataTable.Load(result);

            return dataTable;
        }

        private DataTable returnDataTable(string query)
        {
            validaConexao();

            var command = _connection.CreateCommand();

            command.CommandText = query;
            command.CommandTimeout = 0;

            var result = command.ExecuteReader();

            var dataTable = new DataTable();
            dataTable.Load(result);

            return dataTable;
        }

        public DataTable RetornaNomeColunas(string schema, string tabela) => returnDataTable(Query.ReturnTableColumnNameType, new SqlParameter[] {
                        new SqlParameter("schema", schema),
                        new SqlParameter("table", tabela)
                    });

        public DataTable RetornaDados(string schema, string key, string tabela, DataTable colunas, List<string> pks)
        {
            var querySelect = $@"
select {string.Join(", ", colunas.AsEnumerable().Select(x => x["COLUMN_NAME"]))} from {schema}.{tabela} 
{(pks.Count > 0 ? $@"
where {key} in (SELECT * FROM (values ({string.Join("),(", pks)})) as x (col1))
" : "")}
order by {key}
";
            return returnDataTable(querySelect);
        }

        public DataTable RetornaKeyCheckSum(string schema, string key, string tabela, DataTable colunas, List<string> pks, string where)
        {
            var querySelect = $@"
select {key}
    , cast(checksum({string.Join(", ", colunas.AsEnumerable().OrderBy(x => x["COLUMN_NAME"]).Select(x => x["COLUMN_NAME"]))}) as bigint) CheckValue 
from {schema}.{tabela} 
{(pks.Count > 0 ? $@"
where {key} in (SELECT * FROM (values ('{string.Join("'),('", pks)}')) as x (col1)) {(string.IsNullOrEmpty(where) ? string.Empty : " and " + where)}
" : (string.IsNullOrEmpty(where) ? string.Empty : " where " + where))} 
 order by {key} 
";
            return returnDataTable(querySelect);
        }

        internal void AtualizarDadosDestino(string destinationSchema, string destinationTable, string columnPK, bool pkIdentity, DataTable colunasDestino, DataRowCollection rows)
        {
            var tempGuid = Guid.NewGuid().ToString().Replace("-", "");
            var listTypeString = new List<String>() { "nvarchar", "varchar", "varbinary" };

            var queryMerge = $"Select {string.Join(", ", colunasDestino.AsEnumerable().Select(s => "Cast(" + s["COLUMN_NAME"] + " as " + s["DATA_TYPE"] + $"{(listTypeString.Any(x => x == s["DATA_TYPE"].ToString()) ? "(max)" : string.Empty)}) " + s["COLUMN_NAME"]))} into #{tempGuid} from ( values";

            foreach (DataRow row in rows)
            {
                var listaItem = new List<string>();
                foreach (DataColumn column in row.Table.Columns)
                {
                    try
                    {
                        var seNull = Convert.ToBoolean(colunasDestino.Select($"COLUMN_NAME = '{column.ColumnName}'").First()["IS_NULLABLE"]);
                        var tem = row[column.ColumnName].ToString().Replace("'", "''");

                        if (column.DataType == typeof(decimal) && !string.IsNullOrWhiteSpace(tem))
                            tem = tem.Replace(",", ".");
                        else if (column.DataType == typeof(DateTime) && !string.IsNullOrWhiteSpace(tem))
                            tem = ((DateTime)row[column.ColumnName]).ToString("yyyyMMdd HH:mm:ss.fff");
                        else if (column.DataType == typeof(byte[]))
                        {
                            var byteString = BitConverter.ToString((byte[])row[column.ColumnName]).Replace("-", "");
                            var byteSize = byteString.Length;
                            tem = $"0x{byteString.Substring(0, byteSize)}";
                        }

                        if (column.DataType == typeof(byte[]))
                            listaItem.Add($@"{(seNull && string.IsNullOrWhiteSpace(tem) ? "null" : $"{tem}")}");
                        else
                            listaItem.Add($@"{(seNull && string.IsNullOrWhiteSpace(tem) ? "null" : $"'{tem}'")}");
                    }
                    catch (Exception ex)
                    {
                        Program.Mensagem = ex.Message;
                        Console.WriteLine(Program.Mensagem);
                    }

                }
                queryMerge += "(" + string.Join(",", listaItem) + "),";
            }

            queryMerge = queryMerge.Substring(0, queryMerge.Length - 1);
            queryMerge += $") as x({string.Join(", ", colunasDestino.AsEnumerable().Select(s => s["COLUMN_NAME"]))})";

            queryMerge += $@"
{(colunasDestino.AsEnumerable().Where(x => x["IsIdentity"].ToString() == "1").Any() ? $"SET IDENTITY_INSERT {destinationSchema}.{destinationTable} ON" : string.Empty)} 
MERGE {destinationSchema}.{destinationTable} AS TargetTable
    USING #{tempGuid} AS SourceTable
    ON (TargetTable.[{columnPK}] = SourceTable.[{columnPK}])
    WHEN NOT MATCHED BY TARGET                                
        THEN INSERT (
            {string.Join(", ", colunasDestino.AsEnumerable().Select(s => s["COLUMN_NAME"]))}
		)
            VALUES(
			{string.Join(", ", colunasDestino.AsEnumerable().Select(s => "SourceTable." + s["COLUMN_NAME"]))}
		)
    WHEN MATCHED                                        
        THEN UPDATE SET
           {string.Join(", ", colunasDestino.AsEnumerable().Where(x => x["COLUMN_NAME"].ToString() != columnPK).Where(xx => xx["IsIdentity"].ToString() != "1").Select(s => "TargetTable." + s["COLUMN_NAME"] + " = " + "SourceTable." + s["COLUMN_NAME"]))}
;

{(colunasDestino.AsEnumerable().Where(x => x["IsIdentity"].ToString() == "1").Any() ? $"SET IDENTITY_INSERT {destinationSchema}.{destinationTable} OFF" : string.Empty)} 

";

            validaConexao();

            var command = _connection.CreateCommand();

            command.CommandText = queryMerge;
            command.CommandTimeout = 0;

            command.ExecuteNonQuery();

        }

        public void Dispose()
        {
            if (_connection.State == System.Data.ConnectionState.Open)
                _connection.Close();

            _connection.Dispose();
        }
    }
}
