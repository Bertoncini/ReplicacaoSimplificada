using ReplicacaoSimplificada;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ReplicacaoSimplificadaTest
{
    public class SqlConnectionFactoryTest
    {
        [Theory]
        [InlineData("", "db", "user", "password")]
        public void ValidarParametroServerException(string server, string database, string user, string password)
        {
            var ex = Assert.Throws<ArgumentException>(() => new SqlConnectionFactory(server, database, user, password));

            Assert.Equal("O parametro server é óbrigatório!", ex.Message);
        }

        [Theory]
        [InlineData("server", "", "user", "password")]
        public void ValidarParametroDatabaseException(string server, string database, string user, string password)
        {
            var ex = Assert.Throws<ArgumentException>(() => new SqlConnectionFactory(server, database, user, password));

            Assert.Equal("O parametro database é óbrigatório!", ex.Message);
        }

        [Theory]
        [InlineData("server", "db", "user", "password")]
        [InlineData("server", "db", "", "password")]
        [InlineData("server", "db", "", "")]
        public void ValidarCriacaoDeConexaoSqlConnection(string server, string database, string user, string password)
        {
            var connectionFactory = new SqlConnectionFactory(server, database, user, password);

            var sqlConnection = connectionFactory.CreateConnection();

            Assert.True(sqlConnection.GetType() == typeof(System.Data.SqlClient.SqlConnection));

        }
    }
}
