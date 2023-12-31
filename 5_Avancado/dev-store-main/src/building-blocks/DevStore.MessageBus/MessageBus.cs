using Confluent.Kafka;
using DevStore.Core.Messages.Integration;
using DevStore.MessageBus.Serializador;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DevStore.MessageBus
{
    public class MessageBus : IMessageBus
    {
        private readonly string _bootstrapserver;

        public MessageBus(string bootstrapserver)
        {
            _bootstrapserver = bootstrapserver;
        }

        public async Task ProducerAsyc<T>(string topic, T message) where T : IntegrationEvent
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapserver
            };

            var producer = new ProducerBuilder<string, T>(config)
                                                                .SetValueSerializer(new SerializerDevStore<T>()) //nova config para serialização
                                                                .Build();
            var result = await producer.ProduceAsync(topic, new Message<string, T>
            {
                Key = Guid.NewGuid().ToString(),
                Value = message
            });
            await Task.CompletedTask;
        }

        public async Task ConsumerAsync<T>(string topic, Func<T, Task> onMessage, CancellationToken cancellation) where T : IntegrationEvent
        {
            _ = Task.Factory.StartNew(async () =>
            {
                var config = new ConsumerConfig
                {
                    GroupId = "grupo-curso",
                    BootstrapServers = _bootstrapserver,
                    EnableAutoCommit = false,
                    EnablePartitionEof = true
                };
                using var consumer = new ConsumerBuilder<string, T>(config)
                                                                           .SetValueDeserializer(new DeserializerDevStore<T>())//nova config para deserialização
                                                                           .Build();
                consumer.Subscribe(topic);

                while (!cancellation.IsCancellationRequested)
                {
                    var result = consumer.Consume();
                    if (result.IsPartitionEOF)
                    {
                        continue;
                    }
                    await onMessage(result.Message.Value);
                    consumer.Commit();
                }
            }, cancellation, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            await Task.CompletedTask;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}