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

                    // Manejo para reintentar conexión a Kafka si hay fallos de red
                    int intentos = 0;
                    bool conectado = false;
                    while (!conectado && intentos < 5)
                    {
                        try
                        {
                            consumer.Subscribe("bigdata");
                            Console.WriteLine("Conexión establecida y suscrito al tópico 'bigdata'. Esperando mensajes...");
                            conectado = true;
                        }
                        catch (SocketException e)
                        {
                            intentos++;
                            Console.Error.WriteLine($"Error al conectar al servidor Kafka (intento {intentos}): {e.Message}");
                            if (intentos < 5)
                            {
                                Console.WriteLine("Reintentando conexión en 5 segundos...");
                                Thread.Sleep(5000); // Espera 5 segundos antes de reintentar
                            }
                            else
                            {
                                Console.Error.WriteLine("No se pudo conectar al servidor Kafka después de múltiples intentos.");
                                return 1;
                            }
                        }
                    }

                    // Manejo de cancelación para detener el bucle de consumo de mensajes
                    var cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (_, e) =>
                    {
                        e.Cancel = true; // Prevenir cierre inmediato
                        cts.Cancel(); // Solicitar cancelación limpia
                    };

                    // Bucle para consumir mensajes de Kafka
                    try
                    {
                        while (!cts.Token.IsCancellationRequested)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(cts.Token);

                                if (consumeResult?.Message != null)
                                {
                                    // Imprimir detalles del mensaje recibido
                                    Console.WriteLine($"Recibido mensaje del topic '{consumeResult.Topic}'");
                                    Console.WriteLine($"Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}");
                                    Console.WriteLine($"Mensaje: {consumeResult.Message.Value}");
                                }
                                else
                                {
                                    Console.WriteLine("Mensaje nulo o vacío recibido.");
                                }
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Error de consumo: {e.Error.Reason}");
                            }
                            catch (OperationCanceledException)
                            {
                                // Salida limpia cuando se cancela el token
                                Console.WriteLine("Consumo cancelado. Saliendo...");
                                break;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error inesperado: {ex.Message}");
                            }
                        }
                    }
                    finally
                    {
                        consumer.Close(); // Cerrar correctamente el consumidor antes de salir
                        Console.WriteLine("Consumidor cerrado.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Excepción general: {ex.Message}");
                return 1;
            }

            return 0; // Salida exitosa
        }
    }
}