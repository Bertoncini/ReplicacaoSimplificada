using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Xunit;

namespace ReplicacaoSimplificadaTest
{
    public class QueryTest
    {
        [Fact]
        public void VerificaQueryParaRetornarNomeTabelaEColunas()
        {
            var queryEsperada = $@"
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
            Assert.Equal(queryEsperada, ReplicacaoSimplificada.Query.ReturnTableColumnNameType);
        }

        [Fact]
        public void VerificaQueryInsertExceptionNomeColunaNaoExiste()
        {
            var dt = new System.Data.DataTable();
            dt.Columns.Add("TABLE_SCHEMA");
            dt.Columns.Add("TABLE_NAME");
            dt.Columns.Add("COLUMN_NAME");
            dt.Columns.Add("IS_NULLABLE");
            var dr1 = dt.NewRow();
            dr1["TABLE_SCHEMA"] = "dbo";
            dr1["TABLE_NAME"] = "tabela1";
            dr1["COLUMN_NAME"] = "coluna1";
            dr1["IS_NULLABLE"] = 0;
            var ex = Assert.Throws<Exception>(() => ReplicacaoSimplificada.Query.GenerateInsert(dt, dr1));

            Assert.Equal("Não foi possivel localizar o nome da coluna na tabela de destino.", ex.Message);
        }

        [Theory]
        [MemberData(nameof(DataTables.TabelaDadosInsert), MemberType = typeof(DataTables))]
        public void VerificaQueryInsert(DataTable dtDestino, DataRow row, string rowExpect)
        {
            var result = ReplicacaoSimplificada.Query.GenerateInsert(dtDestino, row);

            Assert.Equal(rowExpect, result);
        }

        [Fact]
        public void VerificaQueryUpdateExceptionNomeColunaNaoExiste()
        {
            var dt = new System.Data.DataTable();
            dt.Columns.Add("TABLE_SCHEMA");
            dt.Columns.Add("TABLE_NAME");
            dt.Columns.Add("COLUMN_NAME");
            dt.Columns.Add("IS_NULLABLE");
            var dr1 = dt.NewRow();
            dr1["TABLE_SCHEMA"] = "dbo";
            dr1["TABLE_NAME"] = "tabela1";
            dr1["COLUMN_NAME"] = "coluna1";
            dr1["IS_NULLABLE"] = 0;
            var ex = Assert.Throws<Exception>(() => ReplicacaoSimplificada.Query.GenerateUpdate(dt, dr1, "COD"));

            Assert.Equal("Não foi possivel localizar o nome da coluna na tabela de destino.", ex.Message);
        }

        [Theory]
        [MemberData(nameof(DataTables.TabelaDadosUpdate), MemberType = typeof(DataTables))]
        public void VerificaQueryUpdate(DataTable dtDestino, DataRow row, string rowExpect)
        {
            var result = ReplicacaoSimplificada.Query.GenerateUpdate(dtDestino, row, "COD");

            Assert.Equal(rowExpect, result);
        }

        [Theory]
        [MemberData(nameof(DataTables.TabelaDadosLinhasParaInserUpdade), MemberType = typeof(DataTables))]
        public void VerificaQueryMerge(string sourceTable, string destinationSchema, string destinationTable, string columnPK, DataTable colunasDestino)
        {
            var expect = $@"
SET IDENTITY_INSERT schemaDestino.tabelaDestino ON
MERGE schemaDestino.tabelaDestino AS TargetTable
    USING tabelaAondeArmazenouDados AS SourceTable
    ON (TargetTable.[COD] = SourceTable.[COD])
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (COD, DESCR, SE_DESC_PORC, COD_USUARIO, DATA_CRIACAO, VALOR, COD_TIPO_DESCONTO, ARQUIVO, DESCR2)
            VALUES (SourceTable.COD, SourceTable.DESCR, SourceTable.SE_DESC_PORC, SourceTable.COD_USUARIO, SourceTable.DATA_CRIACAO, SourceTable.VALOR, SourceTable.COD_TIPO_DESCONTO, SourceTable.ARQUIVO, SourceTable.DESCR2)
    WHEN MATCHED
        THEN UPDATE SET
           TargetTable.DESCR = SourceTable.DESCR, TargetTable.SE_DESC_PORC = SourceTable.SE_DESC_PORC, TargetTable.COD_USUARIO = SourceTable.COD_USUARIO, TargetTable.DATA_CRIACAO = SourceTable.DATA_CRIACAO, TargetTable.VALOR = SourceTable.VALOR, TargetTable.COD_TIPO_DESCONTO = SourceTable.COD_TIPO_DESCONTO, TargetTable.ARQUIVO = SourceTable.ARQUIVO, TargetTable.DESCR2 = SourceTable.DESCR2
;SET IDENTITY_INSERT schemaDestino.tabelaDestino OFF
";
            var actual = ReplicacaoSimplificada.Query.GenerateMerge(sourceTable, destinationSchema, destinationTable, columnPK, colunasDestino);

            Assert.Equal(expect, actual);
        }

        class DataTables
        {
            public static IEnumerable<object[]> TabelaDadosLinhasParaInserUpdade =>
               new List<object[]>
               {
            new object[] { "tabelaAondeArmazenouDados","schemaDestino","tabelaDestino","COD", tabelaEstruturaGeneric() },
               };


            public static IEnumerable<object[]> TabelaDadosInsert =>
               new List<object[]>
               {
            new object[] { tabelaEstruturaGeneric(), tabelaGerenericDados(), "Insert into promocao.Promocao (COD, DESCR, SE_DESC_PORC, COD_USUARIO, DATA_CRIACAO, VALOR, COD_TIPO_DESCONTO, ARQUIVO, DESCR2) values ('1','PrimeiraLinha','False','2859a9b4-ebf2-4e81-a198-b7fbcaa04078','20200805 00:00:00.000','1.69','2',null,null)" },
            new object[] { tabelaEstruturaGeneric(), tabelaGerenericDados2(), "Insert into promocao.Promocao (COD, DESCR, SE_DESC_PORC, COD_USUARIO, DATA_CRIACAO, VALOR, COD_TIPO_DESCONTO, ARQUIVO, DESCR2) values ('1','PrimeiraLinha','False','2859a9b4-ebf2-4e81-a198-b7fbcaa04078','20200805 00:00:00.000','1.69','2',CAST('QWxndW0gQXJxdWl2byBlbSBTdHJpbmcvU3RyZWFt' AS VARBINARY(MAX)),null)" },
               };

            public static IEnumerable<object[]> TabelaDadosUpdate =>
              new List<object[]>
              {
            new object[] { tabelaEstruturaGeneric(), tabelaGerenericDados(), "update promocao.Promocao set DESCR = 'PrimeiraLinha', SE_DESC_PORC = 'False', COD_USUARIO = '2859a9b4-ebf2-4e81-a198-b7fbcaa04078', DATA_CRIACAO = '20200805 00:00:00.000', VALOR = '1.69', COD_TIPO_DESCONTO = '2', ARQUIVO = null, DESCR2 = null where COD = '1'" },
            new object[] { tabelaEstruturaGeneric(), tabelaGerenericDados2(), "update promocao.Promocao set DESCR = 'PrimeiraLinha', SE_DESC_PORC = 'False', COD_USUARIO = '2859a9b4-ebf2-4e81-a198-b7fbcaa04078', DATA_CRIACAO = '20200805 00:00:00.000', VALOR = '1.69', COD_TIPO_DESCONTO = '2', ARQUIVO = CAST('QWxndW0gQXJxdWl2byBlbSBTdHJpbmcvU3RyZWFt' AS VARBINARY(MAX)), DESCR2 = null where COD = '1'" },
              };

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
                dr6["COLUMN_NAME"] = "VALOR";
                dr6["IS_NULLABLE"] = 0;
                dr6["DATA_TYPE"] = "decimal";
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

                var dr9 = dt.NewRow();
                dr9["TABLE_CATALOG"] = "ERP";
                dr9["TABLE_SCHEMA"] = "promocao";
                dr9["TABLE_NAME"] = "Promocao";
                dr9["COLUMN_NAME"] = "DESCR2";
                dr9["IS_NULLABLE"] = 1;
                dr9["DATA_TYPE"] = "nvarchar";
                dr9["Se_PK"] = 0;
                dr9["IsIdentity"] = 0;
                dt.Rows.Add(dr9);

                return dt;
            }

            public static DataRow tabelaGerenericDados()
            {
                var dt = new DataTable();
                dt.Columns.Add("COD", typeof(int));
                dt.Columns.Add("DESCR", typeof(string));
                dt.Columns.Add("SE_DESC_PORC", typeof(bool));
                dt.Columns.Add("COD_USUARIO", typeof(Guid));
                dt.Columns.Add("DATA_CRIACAO", typeof(DateTime));
                dt.Columns.Add("VALOR", typeof(decimal));
                dt.Columns.Add("COD_TIPO_DESCONTO", typeof(int));
                dt.Columns.Add("ARQUIVO", typeof(byte[]));
                dt.Columns.Add("DESCR2", typeof(string));
                var dr1 = dt.NewRow();
                dr1["COD"] = 1;
                dr1["DESCR"] = "PrimeiraLinha";
                dr1["SE_DESC_PORC"] = 0;
                dr1["COD_USUARIO"] = new Guid("2859A9B4-EBF2-4E81-A198-B7FBCAA04078");
                dr1["DATA_CRIACAO"] = new DateTime(2020, 08, 05);
                dr1["VALOR"] = 1.69M;
                dr1["COD_TIPO_DESCONTO"] = 2;
                dr1["ARQUIVO"] = null;
                dr1["DESCR2"] = null;
                dt.Rows.Add(dr1);

                return dr1;
            }

            public static DataRow tabelaGerenericDados2()
            {
                var dt = new DataTable();
                dt.Columns.Add("COD", typeof(int));
                dt.Columns.Add("DESCR", typeof(string));
                dt.Columns.Add("SE_DESC_PORC", typeof(bool));
                dt.Columns.Add("COD_USUARIO", typeof(Guid));
                dt.Columns.Add("DATA_CRIACAO", typeof(DateTime));
                dt.Columns.Add("VALOR", typeof(decimal));
                dt.Columns.Add("COD_TIPO_DESCONTO", typeof(int));
                dt.Columns.Add("ARQUIVO", typeof(byte[]));
                dt.Columns.Add("DESCR2", typeof(string));
                var dr1 = dt.NewRow();
                dr1["COD"] = 1;
                dr1["DESCR"] = "PrimeiraLinha";
                dr1["SE_DESC_PORC"] = 0;
                dr1["COD_USUARIO"] = new Guid("2859A9B4-EBF2-4E81-A198-B7FBCAA04078");
                dr1["DATA_CRIACAO"] = new DateTime(2020, 08, 05);
                dr1["VALOR"] = 1.69M;
                dr1["COD_TIPO_DESCONTO"] = 2;
                dr1["ARQUIVO"] = Encoding.ASCII.GetBytes("Algum Arquivo em String/Stream"); ;
                dr1["DESCR2"] = null;
                dt.Rows.Add(dr1);

                return dr1;
            }

            public static DataTable dadosMerge()
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

        }
    }
}
