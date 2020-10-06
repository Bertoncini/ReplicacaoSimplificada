using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace ReplicacaoSimplificada
{
    public interface IDbConnectionFactory
    {
        IDbConnection CreateConnection();

        void BulkCopy(string DestinationTableName, DataTable dt);
    }

    public class SqlConnectionFactory : IDbConnectionFactory
    {
        private readonly string dbConnectionString;

        public SqlConnectionFactory(string server, string database, string user = "", string password = "")
        {
            if (string.IsNullOrWhiteSpace(server))
                throw new ArgumentException("O parametro server é óbrigatório!");

            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("O parametro database é óbrigatório!");

            if (string.IsNullOrWhiteSpace(user) || string.IsNullOrWhiteSpace(password))
                this.dbConnectionString = $"Server={server};Database={database};Trusted_Connection=True;Application Name=ReplicacaoSimplificada";
            else
                this.dbConnectionString = $"Server={server};Database={database};User Id={user};Password={password};Application Name=ReplicacaoSimplificada";
        }

        public void BulkCopy(string DestinationTableName, DataTable dt)
        {
            using (var bulkcopy = new SqlBulkCopy(dbConnectionString))
            {
                bulkcopy.BulkCopyTimeout = 0;
                bulkcopy.DestinationTableName = DestinationTableName;
                bulkcopy.BatchSize = 1000;
                bulkcopy.NotifyAfter = 1000;
                bulkcopy.SqlRowsCopied += (objc, EventArgs) =>
                {
                    //Program.AtualizarStatus(server, $"(temp) Qtd de linhas já inserida {EventArgs.RowsCopied} de {dt.Rows.Count}", null);
                };
                bulkcopy.WriteToServer(dt);
                bulkcopy.Close();
            }
        }

        public IDbConnection CreateConnection() => new SqlConnection(this.dbConnectionString);
    }

    public class Conexao : IDisposable
    {
        private readonly IDbConnectionFactory _connectionFactory;

        private string server;
        private string _connectionString;

        private IDbConnection _connection;

        public Conexao(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException("connectionFactory");
            _connectionString = _connectionFactory.CreateConnection().ConnectionString;
        }

        public IDbConnection validaConexao(IDbConnection connection)
        {
            if (connection == null || connection.State != System.Data.ConnectionState.Open)
            {
                connection?.Close();

                connection = _connectionFactory.CreateConnection();


                server = connection.Database;
                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();
            }

            return connection;
        }

        private DataTable returnDataTable(string query, SqlParameter[] parameters)
        {
            _connection = validaConexao(_connection);

            var command = _connection.CreateCommand();

            command.CommandText = query;
            parameters.ToList().ForEach(param => command.Parameters.Add(param));

            var result = command.ExecuteReader();

            var dataTable = new DataTable();
            dataTable.Load(result);

            return dataTable;
        }

        private DataTable returnDataTable(string query)
        {
            _connection = validaConexao(_connection);

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
            var nomeTempTable = $"##{(Guid.NewGuid().ToString().Replace("-", ""))}";
            var listTypeString = new List<String>() { "nvarchar", "varchar", "varbinary" };
            var typePkIsString = colunas.AsEnumerable().Where(x => x["COLUMN_NAME"].ToString() == key).Any(s => listTypeString.Any(x => x == s["DATA_TYPE"].ToString()));
            if (pks != null && pks.Count > 0)
            {
                var table = new DataTable();
                table.Columns.Add("col1", typeof(string));
                foreach (var row in pks)
                    table.Rows.Add(row);

                var queryTempTable = $@"
CREATE TABLE {nomeTempTable}(
col1 varchar(max)
)
";
                _connection = validaConexao(_connection);
                var command = _connection.CreateCommand();
                command.CommandText = queryTempTable;
                command.CommandTimeout = 0;
                command.ExecuteNonQuery();

                Program.AtualizarStatus(server, $"Inserindo as pks na tabela temporaria para recuperar informacoes {nomeTempTable}", null);
                using (var bulkcopy = new SqlBulkCopy(_connectionString))
                {
                    bulkcopy.BulkCopyTimeout = 0;
                    bulkcopy.DestinationTableName = nomeTempTable;
                    bulkcopy.BatchSize = 1000;
                    bulkcopy.NotifyAfter = 1000;
                    bulkcopy.SqlRowsCopied += (objc, EventArgs) =>
                    {
                        Program.AtualizarStatus(server, $"(temp) Qtd de linhas já inserida para recuperar dados {EventArgs.RowsCopied} de {table.Rows.Count}", null);
                    };
                    bulkcopy.WriteToServer(table);
                    bulkcopy.Close();
                    Program.AtualizarStatus(server, $"Inserção na tabela temporaria finalizada", null);
                }
            }

            Program.AtualizarStatus(server, $"Recuperando dados para ser inserido no destino", null);
            var querySelect = $@"
select {string.Join(", ", colunas.AsEnumerable().Select(x => x["COLUMN_NAME"]))} from {schema}.{tabela} 
{(pks.Count > 0 ? $@"
where {key} {(typePkIsString ? "COLLATE DATABASE_DEFAULT" : "")} in (SELECT col1 FROM {nomeTempTable} )
" : "")}
order by {key}
";
            return returnDataTable(querySelect);
        }

        public DataTable RetornaKeyCheckSum(string schema, string key, string tabela, DataTable colunas, DataTable dtPks = null, string where = "")
        {
            var nomeTempTable = $"##{(Guid.NewGuid().ToString().Replace("-", ""))}";
            var listTypeString = new List<String>() { "nvarchar", "varchar", "varbinary" };
            var typePkIsString = colunas.AsEnumerable().Where(x => x["COLUMN_NAME"].ToString() == key).Any(s => listTypeString.Any(x => x == s["DATA_TYPE"].ToString()));

            if (dtPks != null && dtPks.Rows.Count > 0)
            {
                var queryTempTable = $@"
CREATE TABLE {nomeTempTable}(
col1 varchar(max),
CheckValue bigint
)
";
                _connection = validaConexao(_connection);
                var command = _connection.CreateCommand();
                command.CommandText = queryTempTable;
                command.CommandTimeout = 0;
                command.ExecuteNonQuery();

                using (var bulkcopy = new SqlBulkCopy(_connectionString))
                {
                    bulkcopy.BulkCopyTimeout = 0;
                    bulkcopy.DestinationTableName = nomeTempTable;
                    bulkcopy.WriteToServer(dtPks);
                    bulkcopy.Close();
                }
            }

            var querySelect = $@"
select {key}
    , cast(checksum({string.Join(", ", colunas.AsEnumerable().OrderBy(x => x["COLUMN_NAME"]).Select(x => x["COLUMN_NAME"]))}) as bigint) CheckValue 
from {schema}.{tabela} 
{((dtPks != null && dtPks.Rows.Count > 0) ? $@"
where {key} {(typePkIsString ? "COLLATE DATABASE_DEFAULT" : "")} in (SELECT col1 FROM {nomeTempTable}) {(string.IsNullOrEmpty(where) ? string.Empty : " and " + where)}
" : (string.IsNullOrEmpty(where) ? string.Empty : " where " + where))} 
 order by {key} 
";
            return returnDataTable(querySelect);
        }

        internal void AtualizarDadosDestino(string destinationSchema, string destinationTable, string columnPK, bool pkIdentity, DataTable colunasDestino, DataTable dados)
        {
            var time = new System.Diagnostics.Stopwatch();
            var tempGuid = "##" + Guid.NewGuid().ToString().Replace("-", "");
            var listTypeString = new List<String>() { "nvarchar", "varchar", "varbinary" };

            _connection = validaConexao(_connection);
            var command = _connection.CreateCommand();
            var camposTable = string.Join(", ", colunasDestino.AsEnumerable().Select(s => s["COLUMN_NAME"] + " " + (s["DATA_TYPE"].ToString() == "decimal" ? "money" : s["DATA_TYPE"]) + $"{(listTypeString.Any(x => x == s["DATA_TYPE"].ToString()) ? "(max)" : string.Empty)}").ToList());

            var queryTempTable = $@"
CREATE TABLE {tempGuid}(
{camposTable}
)
";
            Program.AtualizarStatus(server, $"Criando tabela Temporaria {tempGuid}", null);
            time.Start();
            command.CommandText = queryTempTable;
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            time.Stop();
            Program.AtualizarStatus(server, $"tabela Temporaria {tempGuid} criada", time.Elapsed);
            time = new System.Diagnostics.Stopwatch();
            Program.AtualizarStatus(server, $"Inserindo dados na tabela {tempGuid}, qtd de registro {dados.Rows.Count}", null);
            time.Start();

            _connectionFactory.BulkCopy(tempGuid, dados);

            time.Stop();
            Program.AtualizarStatus(server, $"Dados inserido na tabela {tempGuid}", time.Elapsed);
            time = new System.Diagnostics.Stopwatch();

            Program.AtualizarStatus(server, $"Iniciando os insert e update na tabela  {destinationSchema}.{destinationTable}", null);
            time.Start();
            var queryMerge = Query.GenerateMerge(tempGuid, destinationSchema, destinationTable, columnPK, colunasDestino);

            _connection = validaConexao(_connection);
            command = _connection.CreateCommand();
            command.CommandText = queryMerge;
            command.CommandTimeout = 0;
            command.ExecuteNonQuery();
            time.Stop();
            Program.AtualizarStatus(server, $"Atualização finalizada", time.Elapsed);
            time = new System.Diagnostics.Stopwatch();
        }

        public void Dispose()
        {
            if (_connection.State == System.Data.ConnectionState.Open)
                _connection.Close();

            _connection.Dispose();
        }
    }
}
