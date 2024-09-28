using System;
using System.Net.Sockets;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                Console.WriteLine("Iniciando el consumidor de Kafka...");

                // Configuración del consumidor de Kafka
                var config = new ConsumerConfig
                {
                    GroupId = "vote-group",
                    BootstrapServers = "54.161.135.240:9093",
                    AutoOffsetReset = AutoOffsetReset.Earliest // Leer desde el principio si no hay offset almacenado
                };

                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    // Suscribirse al tópico donde se reciben los votos
                    consumer.Subscribe("bigdata");
                    Console.WriteLine("Conexión establecida y suscrito al tópico 'testtopic'. Esperando mensajes...");

                    // Bucle para consumir mensajes de Kafka
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(CancellationToken.None);
                            if (consumeResult != null)
                            {
                                string json = consumeResult.Message.Value;
                                dynamic vote = JsonConvert.DeserializeObject(json);

                                Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");

                                // Aquí puedes añadir lógica para procesar el voto si es necesario
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occurred: {e.Error.Reason}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error inesperado: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Excepción general: {ex.Message}");
                return 1;
            }
        }
    }
}