using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Xunit;

namespace ReplicacaoSimplificadaTest
{
    public class ProgramTest
    {
        [Fact]
        public void VerificarMensagemHelp()
        {
            var expect = $@"
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

            Assert.Equal(expect, ReplicacaoSimplificada.Program.helpMessage());
        }

        [Theory]
        [InlineData("--help", true)]
        [InlineData("-help", true)]
        [InlineData("help", true)]
        [InlineData("--h", true)]
        [InlineData("-h", true)]
        [InlineData("h", true)]
        [InlineData("", false)]
        [InlineData("qualquerCoisa", false)]
        public void VerificaShowHelp(string parametro, bool expect)
        {
            var args = new List<string> { parametro };
            Assert.Equal(expect, ReplicacaoSimplificada.Program.showHelp(args));

        }

        [Fact]
        public void VerificaParametrosCommandLine()
        {
            var commandLine = new string[] {
                @"C:\Sources\ReplicacaoSimplificada\ReplicacaoSimplificada\bin\Debug\netcoreapp2.1\ReplicacaoSimplificada.dll",
                @"sourceserver servidorOrigem",
                @"sourceuser usuarioOrigem ",
                @"sourcepassword senhaorigem ",
                @"sourcedatabase dbOrigem",
                @"sourceschema schemaOrigem ",
                @"sourcetable tabelaOrigem ",
                @"sourcewhere whereOrigem ",
                @"destinationserver servidorDestino ",
                @"destinationuser usuarioDestino ",
                @"destinationpassword senhaDestino",
                @"destinationdatabase bancoDestino ",
                @"destinationschema schemaDestino ",
                @"destinationtable tabelaDestino ",
                @"destinationwhere whereDestino ",
                @"destinationexecute 1 ",
                @"    ",
            };
            var expectDic = new Dictionary<string, string>();
            expectDic.Add("sourceserver", "servidorOrigem");
            expectDic.Add("sourceuser", "usuarioOrigem");
            expectDic.Add("sourcepassword", "senhaorigem");
            expectDic.Add("sourcedatabase", "dbOrigem");
            expectDic.Add("sourceschema", "schemaOrigem");
            expectDic.Add("sourcetable", "tabelaOrigem");
            expectDic.Add("sourcewhere", "whereOrigem");

            expectDic.Add("destinationserver", "servidorDestino");
            expectDic.Add("destinationuser", "usuarioDestino");
            expectDic.Add("destinationpassword", "senhaDestino");
            expectDic.Add("destinationdatabase", "bancoDestino");
            expectDic.Add("destinationschema", "schemaDestino");
            expectDic.Add("destinationtable", "tabelaDestino");
            expectDic.Add("destinationwhere", "whereDestino");
            expectDic.Add("destinationexecute", "1");

            var paramsDic = ReplicacaoSimplificada.Program.returnDicParametrsCommandLine(commandLine);
            Assert.Equal(expectDic, paramsDic);
        }

        [Fact]
        public void VerificaParametrosArgs()
        {
            var commandLine = new string[] {
                "--sourceserver","servidorOrigem",
                "--sourceuser","usuarioOrigem ",
                "--sourcepassword","senhaorigem ",
                "--sourcedatabase","dbOrigem",
                "--sourceschema","schemaOrigem ",
                "--sourcetable","tabelaOrigem ",
                "--sourcewhere","whereOrigem ",
                "--destinationserver","servidorDestino ",
                "--destinationuser","usuarioDestino ",
                "--destinationpassword","senhaDestino",
                "--destinationdatabase","bancoDestino ",
                "--destinationschema","schemaDestino ",
                "--destinationtable","tabelaDestino ",
                "--destinationwhere","whereDestino ",
                "--destinationexecute","1 ",
            };
            var expectDic = new Dictionary<string, string>();
            expectDic.Add("sourceserver", "servidorOrigem");
            expectDic.Add("sourceuser", "usuarioOrigem");
            expectDic.Add("sourcepassword", "senhaorigem");
            expectDic.Add("sourcedatabase", "dbOrigem");
            expectDic.Add("sourceschema", "schemaOrigem");
            expectDic.Add("sourcetable", "tabelaOrigem");
            expectDic.Add("sourcewhere", "whereOrigem");

            expectDic.Add("destinationserver", "servidorDestino");
            expectDic.Add("destinationuser", "usuarioDestino");
            expectDic.Add("destinationpassword", "senhaDestino");
            expectDic.Add("destinationdatabase", "bancoDestino");
            expectDic.Add("destinationschema", "schemaDestino");
            expectDic.Add("destinationtable", "tabelaDestino");
            expectDic.Add("destinationwhere", "whereDestino");
            expectDic.Add("destinationexecute", "1");
            var paramsDic = ReplicacaoSimplificada.Program.returnDicParametrsArgs(commandLine);
            Assert.Equal(expectDic, paramsDic);
        }

        [Fact]
        public void validationDicParams()
        {
            var expectDicFalse = new Dictionary<string, string>();
            expectDicFalse.Add("sourceserver", "servidorOrigem");
            expectDicFalse.Add("sourcedatabase", "dbOrigem");
            expectDicFalse.Add("sourceschema", "schemaOrigem");
            expectDicFalse.Add("sourcetable", "tabelaOrigem");

            Assert.False(ReplicacaoSimplificada.Program.validationParameter(expectDicFalse));

            var expectDicTrue = new Dictionary<string, string>();
            expectDicTrue.Add("sourceserver", "servidorOrigem");
            expectDicTrue.Add("sourcedatabase", "dbOrigem");
            expectDicTrue.Add("sourceschema", "schemaOrigem");
            expectDicTrue.Add("sourcetable", "tabelaOrigem");
            expectDicTrue.Add("destinationserver", "servidorDestino");
            expectDicTrue.Add("destinationdatabase", "bancoDestino");
            expectDicTrue.Add("destinationschema", "schemaDestino");
            expectDicTrue.Add("destinationtable", "tabelaDestino");

            Assert.True(ReplicacaoSimplificada.Program.validationParameter(expectDicTrue));
        }

        [Theory]
        [MemberData(nameof(DataTables.TabelaDadosLinhasParaInserUpdade), MemberType = typeof(DataTables))]
        public void verificarLinhaParaInsertUpdade(DataTable dadosOrigem, DataTable dadosDestino, string columnPK, List<object> expect)
        {
            var actual = ReplicacaoSimplificada.Program.VerificarLinhasParaInsertUpdate(dadosOrigem, dadosDestino, columnPK);
            Assert.True(expect.All(e => actual.Any(a => a.GetType().GetProperty("PK").GetValue(a, null).ToString() == e.GetType().GetProperty("PK").GetValue(e, null).ToString()
            && a.GetType().GetProperty("SeInsert").GetValue(a, null).ToString() == e.GetType().GetProperty("SeInsert").GetValue(e, null).ToString())));
        }


        class DataTables
        {
            public static IEnumerable<object[]> TabelaDadosLinhasParaInserUpdade =>
               new List<object[]>
               {
            new object[] { dadosOrigem(), dadosDestino(), "COD", (new List<object> { new { PK = 1, SeInsert = false }, new { PK = 2, SeInsert = true } })  },
            new object[] { dadosDestino(), dadosOrigem(), "COD", (new List<object> { new { PK = 1, SeInsert = false }})  }
               };

            public static DataTable dadosOrigem()
            {
                var dt = new DataTable();
                dt.Columns.Add("COD", typeof(int));
                dt.Columns.Add("DESCR", typeof(string));

                var dr1 = dt.NewRow();
                dr1["COD"] = 1;
                dr1["DESCR"] = "PrimeiraLinha-Update";
                dt.Rows.Add(dr1);

                var dr2 = dt.NewRow();
                dr2["COD"] = 2;
                dr2["DESCR"] = "SegundaLinha";
                dt.Rows.Add(dr2);

                return dt;
            }

            public static DataTable dadosDestino()
            {
                var dt = new DataTable();
                dt.Columns.Add("COD", typeof(int));
                dt.Columns.Add("DESCR", typeof(string));

                var dr1 = dt.NewRow();
                dr1["COD"] = 1;
                dr1["DESCR"] = "PrimeiraLinha";
                dt.Rows.Add(dr1);

                return dt;
            }
        }
    }
}
