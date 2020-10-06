using ReplicacaoSimplificada;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ReplicacaoSimplificadaTest
{
    public class EventStatusTest
    {
        [Fact]
        public void VerificaSeEventoFoiChamado()
        {
            var disparoEvento = false;
            var evento = new Notification();
            evento.UpdateNotification += (sender, e) =>
            {
                disparoEvento = true;
            };
            evento.NotificationUpdate(string.Empty, string.Empty, null);
            Assert.True(disparoEvento);
        }

        static readonly TimeSpan tenMinutes = new TimeSpan(0, 10, 0);

        [Theory]
        [MemberData(nameof(paramsNotification))]
        public void VerificarRetornoDaNotificacao(string destination, string message, TimeSpan? timeExecution)
        {
            var evento = new Notification();
            evento.UpdateNotification += (sender, e) =>
            {
                Assert.Equal(destination, e.destination);
                Assert.Equal(message, e.message);
                if (!timeExecution.HasValue)
                    timeExecution = new TimeSpan(0, 0, 0);
                Assert.Equal(timeExecution, e.timeExecution);
            };
            evento.NotificationUpdate(destination, message, timeExecution);
        }

        public static readonly object[][] paramsNotification =
{
    new object[] { "PDV", "mensagem", new TimeSpan(1,10,0)},
    new object[] { "ERP", "mensagem 2", new TimeSpan(0,20,1)},
    new object[] { "ERP", "mensagem 2", new TimeSpan(0,0, 1)},
    new object[] { "ERP", "mensagem 2", null}
};
    }
}
