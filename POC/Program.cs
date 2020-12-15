using System;

namespace POC
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            ReplicacaoSimplificada.Program.Main(new string[] {
                "sourceserver", "serveSource", "sourcedatabase","database", "sourceschema","schema", "sourcetable","table", "sourceuser","user_source", "sourcepassword","password_source",
                "destinationserver","serveDestination", "destinationdatabase","database", "destinationschema","schema", "destinationtable","table", "destinationuser","user_destination", "destinationpassword","password_destination"});

            Console.WriteLine(ReplicacaoSimplificada.Program.Mensagem);
            Console.WriteLine(ReplicacaoSimplificada.Program.SeAtualizado);
        }
    }
}
