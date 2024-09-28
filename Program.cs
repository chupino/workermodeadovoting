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
                    Console.WriteLine("Intentando conectar con el servidor de Kafka...");

                    try
                    {
                        // Suscribirse al tópico donde se reciben los mensajes
                        consumer.Subscribe("bigdata");
                        Console.WriteLine("Conexión establecida y suscrito al tópico 'bigdata'. Esperando mensajes...");
                    }
                    catch (SocketException e)
                    {
                        Console.Error.WriteLine($"Error al conectar al servidor Kafka: {e.Message}");
                        return 1;
                    }

                    // Bucle para consumir mensajes de Kafka
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(CancellationToken.None);
                            if (consumeResult != null)
                            {
                                // Imprimir detalles del mensaje recibido
                                Console.WriteLine($"Recibido mensaje del topic '{consumeResult.Topic}'");
                                Console.WriteLine($"Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}");
                                Console.WriteLine($"Mensaje: {consumeResult.Message.Value}");
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error de consumo: {e.Error.Reason}");
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