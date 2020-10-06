using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;

namespace ReplicacaoSimplificada
{
    public static class Program
    {
        public static bool SeAtualizado = false;
        public static string Mensagem;

        public static void AtualizarStatus(string destination, string message, TimeSpan? timeComplete) => new NotImplementedException();

        public static void Main(string[] args)
        {
            SeAtualizado = false;
            var argumentsDic = new Dictionary<string, string>();

            var parametersLine = Environment.CommandLine.Replace($"\"{Environment.GetCommandLineArgs()[0].ToString()}\"", "").Trim().Split("--");

            if (showHelp(args))
            {
                helpMessage();
                return;
            }

            if (parametersLine.Count() > 1 && args.Count() > 1)
                argumentsDic = returnDicParametrsCommandLine(parametersLine);
            else
                argumentsDic = returnDicParametrsArgs(args);


            var parameterValid = validationParameter(argumentsDic);

            if (!parameterValid)
                return;

            try
            {
                DataTable colunasOrigem, colunasDestino, DadosOrigemKey, DadosDestinoKey, DadosOrigem;

                colunasOrigem = RetornarColunas(argumentsDic["sourceserver"], argumentsDic["sourcedatabase"], argumentsDic["sourceuser"], argumentsDic["sourcepassword"], argumentsDic["sourceschema"], argumentsDic["sourcetable"], true);

                colunasDestino = RetornarColunas(argumentsDic["destinationserver"], argumentsDic["destinationdatabase"], argumentsDic["destinationuser"], argumentsDic["destinationpassword"], argumentsDic["destinationschema"], argumentsDic["destinationtable"], false);


                AtualizarStatus(argumentsDic["destinationserver"], $"Verificando Coluna na tabela de destino", null);
                var time = new System.Diagnostics.Stopwatch();
                time.Start();
                var result = !colunasOrigem.CheckColumnExists(colunasDestino);
                time.Stop();

                if (result)
                {
                    Mensagem = $"Existe colunas na tabela de destino que não existe na de origem\n{string.Join(", ", colunasOrigem.NameColumnNotExists(colunasDestino))}";
                    AtualizarStatus(argumentsDic["destinationserver"], Mensagem, time.Elapsed);
                    return;
                }

                time = new System.Diagnostics.Stopwatch();
                result = !colunasDestino.CheckColumnExists(colunasOrigem);
                time.Stop();
                if (result)
                {
                    Mensagem = $"Existe colunas na tabela de origem que não existe na de destino\n{string.Join(",", colunasDestino.NameColumnNotExists(colunasOrigem))}";
                    AtualizarStatus(argumentsDic["destinationserver"], Mensagem, time.Elapsed);
                    return;
                }

                var columnPKOrigem = colunasOrigem.Select("SE_PK = 1").First()["COLUMN_NAME"].ToString();
                var columnPKDestino = colunasDestino.Select("SE_PK = 1").First()["COLUMN_NAME"].ToString();
                var columnPKIdentity = Convert.ToBoolean(colunasDestino.Select("SE_PK = 1").First()["IsIdentity"]);

                DadosOrigemKey = RetornarKeyChecksum(argumentsDic["sourceserver"], argumentsDic["sourcedatabase"], argumentsDic["sourceuser"], argumentsDic["sourcepassword"], argumentsDic["sourceschema"], argumentsDic["sourcetable"], argumentsDic["sourcewhere"], columnPKOrigem, colunasOrigem, true);

                DadosDestinoKey = RetornarKeyChecksum(argumentsDic["destinationserver"], argumentsDic["destinationdatabase"], argumentsDic["destinationuser"], argumentsDic["destinationpassword"], argumentsDic["destinationschema"], argumentsDic["destinationtable"], argumentsDic["destinationwhere"], columnPKDestino, colunasOrigem, true);

                var listaPk = VerificarLinhasParaInsertUpdate(DadosOrigemKey, DadosDestinoKey, columnPKDestino);

                var listaQuery = new List<string>();

                if (listaPk.Count > 0)
                {
                    DadosOrigem = RetornarDados(argumentsDic["sourceserver"], argumentsDic["sourcedatabase"], argumentsDic["sourceuser"], argumentsDic["sourcepassword"], argumentsDic["sourceschema"], argumentsDic["sourcetable"], string.Empty, columnPKOrigem, colunasOrigem, listaPk.Select(x => x.GetType().GetProperty("PK").GetValue(x, null).ToString()).ToList());

                    if (argumentsDic["destinationexecute"].Trim() != "1")
                    {
                        foreach (DataRow item in DadosOrigem.Rows)
                        {
                            var item2 = listaPk.First(x => x.GetType().GetProperty("PK").GetValue(x, null).ToString() == item[columnPKDestino].ToString());
                            if (Convert.ToBoolean(item2.GetType().GetProperty("SeInsert").GetValue(item2, null)))
                                listaQuery.Add(Query.GenerateInsert(colunasDestino, item));
                            else //Update
                                listaQuery.Add(Query.GenerateUpdate(colunasDestino, item, columnPKDestino));
                        }

                        if (File.Exists($"{argumentsDic["destinationtable"]}.sql"))
                            File.Delete($"{argumentsDic["destinationtable"]}.sql");

                        using (var sw = File.CreateText($"{argumentsDic["destinationtable"]}.sql"))
                            sw.WriteLine($"{(columnPKIdentity ? $"SET IDENTITY_INSERT { argumentsDic["destinationschema"]}.{ argumentsDic["destinationtable"]} OFF " : string.Empty)} " + string.Join("\n", listaQuery) + $"{(columnPKIdentity ? $"SET IDENTITY_INSERT { argumentsDic["destinationschema"]}.{ argumentsDic["destinationtable"]} ON " : string.Empty)} ");

                        Mensagem = $"Geração de arquivo gerado com sucesso {argumentsDic["destinationtable"]}.sql";
                        AtualizarStatus(argumentsDic["destinationserver"], Mensagem, null);
                        SeAtualizado = true;
                    }
                    else
                    {
                        using (var con = new Conexao(new SqlConnectionFactory(argumentsDic["destinationserver"], argumentsDic["destinationdatabase"], argumentsDic["destinationuser"], argumentsDic["destinationpassword"])))
                            con.AtualizarDadosDestino(argumentsDic["destinationschema"], argumentsDic["destinationtable"], columnPKDestino, columnPKIdentity, colunasOrigem, DadosOrigem);
                        SeAtualizado = true;
                    }
                }
                else
                {
                    SeAtualizado = true;
                    Mensagem = "Não existe dados diveregente";
                    AtualizarStatus(argumentsDic["destinationserver"], Mensagem, null);
                }

            }
            catch (Exception ex)
            {
                Mensagem = ex.Message;
                AtualizarStatus(argumentsDic["destinationserver"], Mensagem, null);
            }
        }

        public static List<object> VerificarLinhasParaInsertUpdate(DataTable DadosOrigemKey, DataTable DadosDestinoKey, string columnPKDestino)
        {
            //AtualizarStatus(server, $"Verificando linhas para realização de insert ou update (separando chaves), Qtd Linhas para verificar {DadosOrigemKey.Rows.Count}", null);
            var time = new System.Diagnostics.Stopwatch();
            time.Start();
            var listaPk = new List<object>();

            foreach (DataRow item in DadosOrigemKey.Rows)
            {
                var item2 = DadosDestinoKey.Select($"{columnPKDestino} = '{item[columnPKDestino]}'");
                var value = item[columnPKDestino];

                if (item2.Count() == 0)
                    listaPk.Add(new { PK = value, SeInsert = true });
                else if (item[1].ToString() != item2[0][1].ToString())
                    listaPk.Add(new { PK = value, SeInsert = false });
            }
            time.Stop();
            //AtualizarStatus(server, $"Separação finalizada, qtd de linhas separadas {listaPk.Count}", time.Elapsed);
            return listaPk;
        }

        private static DataTable RetornarColunas(string server, string database, string user, string password, string schema, string table, bool seOrigem)
        {
            AtualizarStatus(server, $"Recuperando Colunas de {(seOrigem ? "Origem" : "Destino")}", null);
            var time = new System.Diagnostics.Stopwatch();
            time.Start();
            DataTable colunas;
            using (var con = new Conexao(new SqlConnectionFactory(server, database, user, password)))
                colunas = con.RetornaNomeColunas(schema, table);
            time.Stop();
            AtualizarStatus(server, $"Recuperação de colunas completado", time.Elapsed);
            return colunas;
        }

        private static DataTable RetornarKeyChecksum(string server, string database, string user, string password, string schema, string table, string where, string namePk, DataTable dtColunas, bool seOrigem)
        {
            AtualizarStatus(server, $"Recuperando dados de {(seOrigem ? "Origem" : "Destino")}", null);
            var time = new System.Diagnostics.Stopwatch();
            time.Start();
            DataTable Dados;
            using (var con = new Conexao(new SqlConnectionFactory(server, database, user, password)))
                Dados = con.RetornaKeyCheckSum(schema, namePk, table, dtColunas, null, where);
            time.Stop();
            AtualizarStatus(server, $"Recuperação de dados completado", time.Elapsed);
            return Dados;
        }

        private static DataTable RetornarDados(string server, string database, string user, string password, string schema, string table, string where, string namePk, DataTable dtColunas, List<string> pks)
        {
            AtualizarStatus(server, $"Recuperando dados para enviar para o destino", null);
            var time = new System.Diagnostics.Stopwatch();
            time.Start();
            DataTable Dados;
            using (var con = new Conexao(new SqlConnectionFactory(server, database, user, password)))
                Dados = con.RetornaDados(schema, namePk, table, dtColunas, pks);
            time.Stop();
            AtualizarStatus(server, $"Recuperação de dados completado", time.Elapsed);
            return Dados;
        }

        public static Dictionary<string, string> returnDicParametrsCommandLine(string[] parametersLine)
        {
            var argumentsDic = new Dictionary<string, string>();

            foreach (var parameter in parametersLine)
            {
                if (string.IsNullOrWhiteSpace(parameter) || parameter.IndexOf(" ") < 0)
                    continue;

                var key = parameter.Substring(0, parameter.IndexOf(" "));
                var value = parameter.Substring(parameter.IndexOf(" ") + 1);

                argumentsDic.Add(key.Trim(), value.Trim());
            }

            return argumentsDic;
        }

        public static Dictionary<string, string> returnDicParametrsArgs(string[] parametersLine)
        {
            var argumentsDic = new Dictionary<string, string>();

            for (var i = 0; i < parametersLine.Count(); i += 2)
            {
                var key = parametersLine[i].Replace("--", string.Empty);
                var value = parametersLine[i + 1];

                argumentsDic.Add(key.Trim(), value.Trim());
            }

            return argumentsDic;
        }

        public static bool validationParameter(Dictionary<string, string> argumentsDic)
        {
            var listParameterRequired = new List<string>
            {
                "sourceserver", "sourcedatabase", "sourceschema", "sourcetable",
                "destinationserver", "destinationdatabase", "destinationschema", "destinationtable",
            };

            var listParameterOptinal = new List<string> { "sourceuser", "sourcepassword", "sourcewhere", "destinationuser", "destinationpassword", "destinationexecute", "destinationwhere" };

            var parametersOptional = listParameterOptinal.Except(argumentsDic.Keys);
            if (parametersOptional.Any())
                parametersOptional.ToList().ForEach(o => argumentsDic.Add(o, string.Empty));

            return !listParameterRequired.Except(argumentsDic.Keys).Any();
        }

        public static string helpMessage()
        {
            var message = $@"
COMANDO HELPER

--sourceserver IP/SeverName de origem para se conectar junto com a porta
--sourceuser Usuario para se conectar ao banco de dados de Origem DEfault Impersonate
--sourcepassword Senha do usuario para se conectar a base de dados de Origem obrigatório se informar o usuario
--sourcedatabase Nome do banco de dados de Origem
--sourceschema Nome do schema Origem default ""dbo""
--sourcetable Nome da tabela Origem
--sourcewhere filtro para realizacao da comparacao

--destinationserver  IP/SeverName de Destino para se conectar junto com a porta
--destinationuser Usuario para se conectar ao banco de dados de Destino DEfault Impersonate
--destinatiopassword Senha do usuario para se conectar a base de dados de Destino obrigatório se informar o usuario
--destinationdatabase Nome do banco de dados de Destino
--destinationschema Nome do schema Origem default ""dbo""
--destinationtable Nome da tabela Destino
--destinationwhere filtro para realizacao da comparacao

--destinationexecute 1 = true 0 = false
";

            return message;
        }

        public static bool showHelp(IEnumerable<string> args) => args.Where(s => s != string.Empty).Select(s => s.ToLowerInvariant())
                .Intersect(new[] { "--help", "-help", "--h", "-h", "h", "help" }).Any();

    }
}
