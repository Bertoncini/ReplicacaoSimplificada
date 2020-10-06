using ReplicacaoSimplificada;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ReplicacaoSimplificadaTest
{
    public class ConexaoTest
    {
        [Fact]
        public void ValidarParametroConnectionFactoryException()
        {
            var ex = Assert.Throws<System.ArgumentNullException>(() => new Conexao(null));

            Assert.Equal("Value cannot be null.\r\nParameter name: connectionFactory", ex.Message);
        }

        [Fact]
        public void ValidarValicaoConexao()
        {
            var dbConnectionFactory = new SqlConnectionFactory("localhost", "master", string.Empty, string.Empty);
            var conexao = new Conexao(dbConnectionFactory);

            conexao.validaConexao(null);

            var connection = dbConnectionFactory.CreateConnection();

            conexao.validaConexao(connection);
        }
    }
}
