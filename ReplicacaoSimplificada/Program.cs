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

        public static void Main(string[] args)
        {
            SeAtualizado = false;
            var argumentsDic = new Dictionary<string, string>();

            var parametersLine = Environment.CommandLine.Replace($"\"{Environment.GetCommandLineArgs()[0].ToString()}\"", "").Trim().Split("--");

            if (showHelp(args))
            {
                help();
                return;
            }

            if (parametersLine.Count() > 1 && args.Count() > 1)
                setValueDicParametrs(argumentsDic, parametersLine);
            else
                setValueDicParametrsArgs(argumentsDic, args);


            var parameterValid = validationParameter(argumentsDic);

            if (!parameterValid)
                return;

            try
            {
                DataTable colunasOrigem, colunasDestino, DadosOrigemKey, DadosDestinoKey, DadosOrigem;

                using (var con = new Conexao(argumentsDic["sourceserver"], argumentsDic["sourcedatabase"], argumentsDic["sourceuser"], argumentsDic["sourcepassword"]))
                    colunasOrigem = con.RetornaNomeColunas(argumentsDic["sourceschema"], argumentsDic["sourcetable"]);

                using (var con = new Conexao(argumentsDic["destinationserver"], argumentsDic["destinationdatabase"], argumentsDic["destinationuser"], argumentsDic["destinationpassword"]))
                    colunasDestino = con.RetornaNomeColunas(argumentsDic["destinationschema"], argumentsDic["destinationtable"]);

                if (colunasOrigem.CheckColumnNameType(colunasDestino))
                {
                    Console.WriteLine($"Existe colunas na tabela de destino que não existe na de origem\n{string.Join(", ", Util.ColumnsNotFind)}");
                    return;
                }

                if (colunasDestino.CheckColumnNameType(colunasOrigem))
                {
                    Console.WriteLine($"Existe colunas na tabela de origem que não existe na de destino\n{string.Join(",", Util.ColumnsNotFind)}");
                    return;
                }

                var columnPKOrigem = colunasOrigem.Select("SE_PK = 1").First()["COLUMN_NAME"].ToString();
                var columnPKDestino = colunasDestino.Select("SE_PK = 1").First()["COLUMN_NAME"].ToString();
                var columnPKIdentity = Convert.ToBoolean(colunasDestino.Select("SE_PK = 1").First()["IsIdentity"].ToString());

                using (var con = new Conexao(argumentsDic["sourceserver"], argumentsDic["sourcedatabase"], argumentsDic["sourceuser"], argumentsDic["sourcepassword"]))
                    DadosOrigemKey = con.RetornaKeyCheckSum(argumentsDic["sourceschema"], columnPKOrigem, argumentsDic["sourcetable"], colunasOrigem, new List<string>(), argumentsDic["sourcewhere"]);

                using (var con = new Conexao(argumentsDic["destinationserver"], argumentsDic["destinationdatabase"], argumentsDic["destinationuser"], argumentsDic["destinationpassword"]))
                    DadosDestinoKey = con.RetornaKeyCheckSum(argumentsDic["destinationschema"], columnPKDestino, argumentsDic["destinationtable"], colunasDestino,
                         DadosOrigemKey.AsEnumerable().Select(x => x[columnPKDestino].ToString()).ToList(), string.Empty
                        );

                var listaPk = new List<object>();

                foreach (DataRow item in DadosOrigemKey.Rows)
                {
                    var item2 = DadosDestinoKey.Select($"{columnPKDestino} = '{item[columnPKDestino]}'");

                    if (item2.Count() == 0)
                        listaPk.Add(new { PK = $"'{item[columnPKDestino]}'", SeInsert = true });
                    else if (item[1].ToString() != item2[0][1].ToString())
                        listaPk.Add(new { PK = $"'{item[columnPKDestino]}'", SeInsert = false });
                }

                using (var con = new Conexao(argumentsDic["sourceserver"], argumentsDic["sourcedatabase"], argumentsDic["sourceuser"], argumentsDic["sourcepassword"]))
                    DadosOrigem = con.RetornaDados(argumentsDic["sourceschema"], columnPKOrigem, argumentsDic["sourcetable"], colunasOrigem, listaPk.Select(x => x.GetType().GetProperty("PK").GetValue(x, null).ToString()).ToList());

                var listaQuery = new List<string>();

                if (listaPk.Count > 0)
                {
                    if (argumentsDic["destinationexecute"].Trim() != "1")
                    {
                        foreach (DataRow item in DadosOrigem.Rows)
                        {
                            var item2 = listaPk.First(x => x.GetType().GetProperty("PK").GetValue(x, null).ToString() == "'" + item[columnPKDestino].ToString() + "'");
                            if (Convert.ToBoolean(item2.GetType().GetProperty("SeInsert").GetValue(item2, null)))
                                listaQuery.Add(Query.GenerateInsert(colunasOrigem, colunasDestino, item));
                            else //Update
                                listaQuery.Add(Query.GenerateUpdate(colunasOrigem, colunasDestino, item, columnPKDestino));
                        }


                        if (File.Exists($"{argumentsDic["destinationtable"]}.sql"))
                            File.Delete($"{argumentsDic["destinationtable"]}.sql");

                        using (var sw = File.CreateText($"{argumentsDic["destinationtable"]}.sql"))
                            sw.WriteLine($"{(columnPKIdentity ? $"SET IDENTITY_INSERT { argumentsDic["destinationschema"]}.{ argumentsDic["destinationtable"]} OFF " : string.Empty)} " + string.Join("\n", listaQuery) + $"{(columnPKIdentity ? $"SET IDENTITY_INSERT { argumentsDic["destinationschema"]}.{ argumentsDic["destinationtable"]} ON " : string.Empty)} ");
                    }
                    else
                    {
                        using (var con = new Conexao(argumentsDic["destinationserver"], argumentsDic["destinationdatabase"], argumentsDic["destinationuser"], argumentsDic["destinationpassword"]))
                            con.AtualizarDadosDestino(argumentsDic["destinationschema"], argumentsDic["destinationtable"], columnPKDestino, columnPKIdentity, colunasOrigem, DadosOrigem.Rows);
                        SeAtualizado = true;
                    }
                }
                else
                {
                    Console.WriteLine("Não existe dados diveregente");
                    SeAtualizado = true;
                }

                Console.WriteLine(SeAtualizado);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static void setValueDicParametrs(Dictionary<string, string> argumentsDic, string[] parametersLine)
        {
            foreach (var parameter in parametersLine)
            {
                if (string.IsNullOrWhiteSpace(parameter) || parameter.IndexOf(" ") < 0)
                    continue;

                var key = parameter.Substring(0, parameter.IndexOf(" "));
                var value = parameter.Substring(parameter.IndexOf(" ") + 1);

                argumentsDic.Add(key, value);
            }
        }

        private static void setValueDicParametrsArgs(Dictionary<string, string> argumentsDic, string[] parametersLine)
        {
            for (var i = 0; i < parametersLine.Count(); i += 2)
            {
                var key = parametersLine[i].Replace("--", string.Empty);
                var value = parametersLine[i + 1];

                argumentsDic.Add(key, value);
            }


        }

        private static bool validationParameter(Dictionary<string, string> argumentsDic)
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

        static bool help()
        {
            Console.WriteLine("COMANDO HELPER");
            Console.WriteLine();
            Console.WriteLine("--sourceserver IP/SeverName de origem para se conectar junto com a porta");
            Console.WriteLine("--sourceuser Usuario para se conectar ao banco de dados de Origem DEfault Impersonate");
            Console.WriteLine("--sourcepassword Senha do usuario para se conectar a base de dados de Origem obrigatório se informar o usuario");
            Console.WriteLine("--sourcedatabase Nome do banco de dados de Origem");
            Console.WriteLine("--sourceschema Nome do schema Origem default \"dbo\" ");
            Console.WriteLine("--sourcetable Nome da tabela Origem");
            Console.WriteLine("--sourcewhere filtro para realizacao da comparacao");

            Console.WriteLine("--destinationserver  IP/SeverName de Destino para se conectar junto com a porta");
            Console.WriteLine("--destinationdatabase Nome do banco de dados de Destino");
            Console.WriteLine("--destinationschema Nome do schema Origem default \"dbo\" ");
            Console.WriteLine("--destinationtable Nome da tabela Destino");
            Console.WriteLine("--destinationuser Usuario para se conectar ao banco de dados de Destino DEfault Impersonate");
            Console.WriteLine("--destinatiopassword Senha do usuario para se conectar a base de dados de Destino obrigatório se informar o usuario");

            Console.WriteLine("--destinationexecute 1 = true 0 = false");


            return true;
        }

        static bool showHelp(IEnumerable<string> args) => args.Where(s => s != string.Empty).Select(s => s.ToLowerInvariant())
                .Intersect(new[] { "--help", "-help", "-h", "h", "help" }).Any();

    }
}
