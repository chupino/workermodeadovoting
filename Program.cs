using System;
using System.Net.Sockets;
using System.Threading;
using Confluent.Kafka;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                // Configuración del consumidor de Kafka
                var config = new ConsumerConfig
                {
                    GroupId = "vote-group", 
                    BootstrapServers = "54.161.135.240:9093",
                    AutoOffsetReset = AutoOffsetReset.Earliest  // Para leer desde el principio si no hay offset almacenado
                };

                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    // Suscribirse al tópico donde se reciben los votos
                    consumer.Subscribe("bigdata");

                    // Bucle para consumir mensajes de Kafka
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(CancellationToken.None);
                            if (consumeResult != null)
                            {
                                // Aquí simplemente imprimimos "Recibido"
                                Console.WriteLine("Recibido");
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occurred: {e.Error.Reason}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }
    }
}