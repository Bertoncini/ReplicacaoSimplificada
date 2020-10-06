using ReplicacaoSimplificada;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace ReplicacaoSimplificadaTest
{
    public class UtilTest
    {
        [Theory]
        [MemberData(nameof(DataTables.TabelasIdenticas), MemberType = typeof(DataTables))]
        public void VerificaSeColunasTabelaAExistemTabelaB(DataTable tabelaA, DataTable tabelaB)
        {

            Assert.True(Util.CheckColumnExists(tabelaA, tabelaA));
            Assert.True(Util.CheckColumnExists(tabelaB, tabelaB));
            Assert.True(Util.CheckColumnExists(tabelaA, tabelaB));
            Assert.True(Util.CheckColumnExists(tabelaB, tabelaA));
        }

        [Theory]
        [MemberData(nameof(DataTables.TabelasColunaFaltando), MemberType = typeof(DataTables))]
        public void VerificaColunaFaltandoNaTabela(DataTable tabelaA, DataTable tabelaB)
        {
            var ColunaNaoTemTabelaA = new HashSet<string> { "Nr_Tabela" };
            var ColunaNaoTemTabelaB = new HashSet<string> { "Nr_Tabela", "Descr_Tabela" };
            Assert.Equal(Util.NameColumnNotExists(tabelaA, tabelaA), new HashSet<string>());
            Assert.True(Util.NameColumnNotExists(tabelaA, tabelaB).All(coluna => ColunaNaoTemTabelaA.Contains(coluna)));
            Assert.True(Util.NameColumnNotExists(tabelaB, tabelaA).All(coluna => ColunaNaoTemTabelaB.Contains(coluna)));
        }

        [Theory]
        [MemberData(nameof(DataTables.TabelaLinha), MemberType = typeof(DataTables))]
        public void VerificaSeColunasDaLinhaExisteNaTabela(DataTable tabela, DataRow row, bool expect)
        {
            Assert.Equal(expect, Util.CheckColumnExists(tabela, row));
        }

        class DataTables
        {
            public static IEnumerable<object[]> TabelaLinha =>
                new List<object[]>
                {
            new object[] { tabela1(), dataRowTabela1(), true },
            new object[] { tabela2(), dataRowTabela1(), false },
                };

            public static IEnumerable<object[]> TabelaDados =>
               new List<object[]>
               {
            new object[] { tabelaEstruturaGeneric(), tabelaGerenericDados(), "" },
               };

            public static DataRow dataRowTabela1()
            {
                var dt = new DataTable();
                dt.Columns.Add("Descr_Tabela", typeof(string));
                dt.Columns.Add("Qtd", typeof(decimal));
                dt.Columns.Add("Nr_Tabela", typeof(int));
                var dr1 = dt.NewRow();
                dr1["Descr_Tabela"] = "descricao qualquer";
                dr1["Qtd"] = 1;
                dr1["Nr_Tabela"] = 123;
                return dr1;
            }

            public static IEnumerable<object[]> TabelasIdenticas =>
                new List<DataTable[]>
                {
            new DataTable[] { tabela1(), tabela1() },
            new DataTable[] { tabela1(), tabela3() },
                };

            public static IEnumerable<object[]> TabelasDiferentes =>
               new List<DataTable[]>
               {
            new DataTable[] { tabela1(), tabela2() },
            new DataTable[] { tabela2(), tabela3() },
               };

            public static IEnumerable<object[]> TabelasColunaFaltando =>
             new List<DataTable[]>
             {
            new DataTable[] { tabela1(), tabela2() },
             };

            public static DataTable tabela1()
            {
                var dt = new DataTable();
                dt.Columns.Add("Cod", typeof(int));
                dt.Columns.Add("COLUMN_NAME", typeof(string));
                dt.Columns.Add("DATA_TYPE", typeof(string));
                var dr1 = dt.NewRow();
                dr1["Cod"] = 0;
                dr1["COLUMN_NAME"] = "Descr_Tabela";
                dr1["DATA_TYPE"] = "varchar";
                dt.Rows.Add(dr1);
                var dr2 = dt.NewRow();
                dr2["Cod"] = 0;
                dr2["COLUMN_NAME"] = "Qtd";
                dr2["DATA_TYPE"] = "decimal";
                dt.Rows.Add(dr2);
                var dr3 = dt.NewRow();
                dr3["Cod"] = 0;
                dr3["COLUMN_NAME"] = "Nr_Tabela";
                dr3["DATA_TYPE"] = "int";
                dt.Rows.Add(dr3);
                return dt;
            }

            public static DataTable tabela2()
            {
                var dt = new DataTable();
                dt.Columns.Add("Cod", typeof(int));
                dt.Columns.Add("COLUMN_NAME", typeof(string));
                dt.Columns.Add("DATA_TYPE", typeof(string));
                var dr1 = dt.NewRow();
                dr1["Cod"] = 0;
                dr1["COLUMN_NAME"] = "Nr_Tabela";
                dr1["DATA_TYPE"] = "money";
                dt.Rows.Add(dr1);
                var dr2 = dt.NewRow();
                dr2["Cod"] = 0;
                dr2["COLUMN_NAME"] = "Qtd";
                dr2["DATA_TYPE"] = "decimal";
                dt.Rows.Add(dr2);
                return dt;
            }

            public static DataTable tabela3()
            {
                var dt = new DataTable();
                dt.Columns.Add("Cod", typeof(int));
                dt.Columns.Add("COLUMN_NAME", typeof(string));
                dt.Columns.Add("DATA_TYPE", typeof(string));
                var dr1 = dt.NewRow();
                dr1["Cod"] = 0;
                dr1["COLUMN_NAME"] = "Descr_Tabela";
                dr1["DATA_TYPE"] = "varchar";
                dt.Rows.Add(dr1);
                var dr2 = dt.NewRow();
                dr2["Cod"] = 0;
                dr2["COLUMN_NAME"] = "Qtd";
                dr2["DATA_TYPE"] = "decimal";
                dt.Rows.Add(dr2);
                var dr3 = dt.NewRow();
                dr3["Cod"] = 0;
                dr3["COLUMN_NAME"] = "Nr_Tabela";
                dr3["DATA_TYPE"] = "int";
                dt.Rows.Add(dr3);
                return dt;
            }


            public static DataTable tabelaEstruturaGeneric()
            {
                var dt = new DataTable();
                dt.Columns.Add("TABLE_CATALOG", typeof(string));
                dt.Columns.Add("TABLE_SCHEMA", typeof(string));
                dt.Columns.Add("TABLE_NAME", typeof(string));
                dt.Columns.Add("COLUMN_NAME", typeof(string));
                dt.Columns.Add("IS_NULLABLE", typeof(int));
                dt.Columns.Add("DATA_TYPE", typeof(string));
                dt.Columns.Add("Se_PK", typeof(int));
                dt.Columns.Add("IsIdentity", typeof(int));
                var dr1 = dt.NewRow();
                dr1["TABLE_CATALOG"] = "ERP";
                dr1["TABLE_SCHEMA"] = "promocao";
                dr1["TABLE_NAME"] = "Promocao";
                dr1["COLUMN_NAME"] = "COD";
                dr1["IS_NULLABLE"] = 0;
                dr1["DATA_TYPE"] = "int";
                dr1["Se_PK"] = 1;
                dr1["IsIdentity"] = 1;
                dt.Rows.Add(dr1);

                var dr2 = dt.NewRow();
                dr2["TABLE_CATALOG"] = "ERP";
                dr2["TABLE_SCHEMA"] = "promocao";
                dr2["TABLE_NAME"] = "Promocao";
                dr2["COLUMN_NAME"] = "DESCR";
                dr2["IS_NULLABLE"] = 0;
                dr2["DATA_TYPE"] = "nvarchar";
                dr2["Se_PK"] = 0;
                dr2["IsIdentity"] = 0;
                dt.Rows.Add(dr2);

                var dr3 = dt.NewRow();
                dr3["TABLE_CATALOG"] = "ERP";
                dr3["TABLE_SCHEMA"] = "promocao";
                dr3["TABLE_NAME"] = "Promocao";
                dr3["COLUMN_NAME"] = "SE_DESC_PORC";
                dr3["IS_NULLABLE"] = 0;
                dr3["DATA_TYPE"] = "bit";
                dr3["Se_PK"] = 0;
                dr3["IsIdentity"] = 0;
                dt.Rows.Add(dr3);

                var dr4 = dt.NewRow();
                dr4["TABLE_CATALOG"] = "ERP";
                dr4["TABLE_SCHEMA"] = "promocao";
                dr4["TABLE_NAME"] = "Promocao";
                dr4["COLUMN_NAME"] = "COD_USUARIO";
                dr4["IS_NULLABLE"] = 0;
                dr4["DATA_TYPE"] = "uniqueidentifier";
                dr4["Se_PK"] = 0;
                dr4["IsIdentity"] = 0;
                dt.Rows.Add(dr4);

                var dr5 = dt.NewRow();
                dr5["TABLE_CATALOG"] = "ERP";
                dr5["TABLE_SCHEMA"] = "promocao";
                dr5["TABLE_NAME"] = "Promocao";
                dr5["COLUMN_NAME"] = "DATA_CRIACAO";
                dr5["IS_NULLABLE"] = 0;
                dr5["DATA_TYPE"] = "datetime";
                dr5["Se_PK"] = 0;
                dr5["IsIdentity"] = 0;
                dt.Rows.Add(dr5);

                var dr6 = dt.NewRow();
                dr6["TABLE_CATALOG"] = "ERP";
                dr6["TABLE_SCHEMA"] = "promocao";
                dr6["TABLE_NAME"] = "Promocao";
                dr6["COLUMN_NAME"] = "COD_TIPO";
                dr6["IS_NULLABLE"] = 0;
                dr6["DATA_TYPE"] = "smallint";
                dr6["Se_PK"] = 0;
                dr6["IsIdentity"] = 0;
                dt.Rows.Add(dr6);

                var dr7 = dt.NewRow();
                dr7["TABLE_CATALOG"] = "ERP";
                dr7["TABLE_SCHEMA"] = "promocao";
                dr7["TABLE_NAME"] = "Promocao";
                dr7["COLUMN_NAME"] = "COD_TIPO_DESCONTO";
                dr7["IS_NULLABLE"] = 0;
                dr7["DATA_TYPE"] = "numeric";
                dr7["Se_PK"] = 0;
                dr7["IsIdentity"] = 0;
                dt.Rows.Add(dr7);

                var dr8 = dt.NewRow();
                dr8["TABLE_CATALOG"] = "ERP";
                dr8["TABLE_SCHEMA"] = "promocao";
                dr8["TABLE_NAME"] = "Promocao";
                dr8["COLUMN_NAME"] = "ARQUIVO";
                dr8["IS_NULLABLE"] = 1;
                dr8["DATA_TYPE"] = "varbinary";
                dr8["Se_PK"] = 0;
                dr8["IsIdentity"] = 0;
                dt.Rows.Add(dr8);
                return dt;
            }

            public static DataTable tabelaGerenericDados()
            {
                var dt = new DataTable();
                dt.Columns.Add("COD", typeof(int));
                dt.Columns.Add("DESCR", typeof(string));
                dt.Columns.Add("SE_DESC_PORC", typeof(bool));
                dt.Columns.Add("COD_USUARIO", typeof(Guid));
                dt.Columns.Add("DATA_CRIACAO", typeof(DateTime));
                dt.Columns.Add("COD_TIPO", typeof(int));
                dt.Columns.Add("COD_TIPO_DESCONTO", typeof(int));
                dt.Columns.Add("ARQUIVO", typeof(byte[]));
                var dr1 = dt.NewRow();
                dr1["COD"] = 1;
                dr1["DESCR"] = "PrimeiraLinha";
                dr1["SE_DESC_PORC"] = 0;
                dr1["COD_USUARIO"] = new Guid("2859A9B4-EBF2-4E81-A198-B7FBCAA04078");
                dr1["DATA_CRIACAO"] = new DateTime(2020, 08, 05);
                dr1["COD_TIPO"] = 1;
                dr1["COD_TIPO_DESCONTO"] = 2;
                dr1["ARQUIVO"] = null;
                dt.Rows.Add(dr1);

                return dt;
            }
        }
    }
}
