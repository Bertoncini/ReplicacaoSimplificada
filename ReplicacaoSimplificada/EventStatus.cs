using System;
using System.Collections.Generic;
using System.Text;

namespace ReplicacaoSimplificada
{

    public delegate void NotificationHandler(object source, NotificactionStatus e);

    public class NotificactionStatus
    {
        public event NotificationHandler UpdateStatus;

        public string destination;
        public string message;
        public TimeSpan timeExecution;
    }

    public class Notification
    {
        public event NotificationHandler UpdateNotification;

        public void NotificationUpdate(string destination, string message, TimeSpan? timeExecution = null)
        {
            if (timeExecution == null)
                timeExecution = new TimeSpan(0, 0, 0);

            var eventArgs = new NotificactionStatus { destination = destination, message = message, timeExecution = timeExecution.Value };
            UpdateNotification(this, eventArgs);
        }

    }
}
