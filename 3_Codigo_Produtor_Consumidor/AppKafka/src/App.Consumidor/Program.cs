using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

var schemaConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);


var config = new ConsumerConfig
{
    GroupId = "devTreinamento",  // Definimos o ID do grupo para o consumidor
    BootstrapServers = "localhost:9092"  // Definimos o endereço do servidor Kafka
};

// Cria um consumidor Kafka com base na configuração fornecida
//using var consumer = new ConsumerBuilder<string, string>(config).Build();  

using var consumer = new ConsumerBuilder<string, treinamento.Curso>(config)
                                                                       .SetValueDeserializer(new AvroDeserializer<treinamento.Curso>(schemaRegistry).AsSyncOverAsync())
                                                                       .Build();  //~~> Criação do produtor Kafka com ~~> schema


consumer.Subscribe("Cursos");  // Inscreve o consumidor no tópico especificado

while (true)  // Loop infinito para receber continuamente mensagens
{
    var result = consumer.Consume();  // Aguarda e consome uma mensagem do tópico inscrito
    //Console.WriteLine($"Messagem: {result.Message.Key}-{result.Message.Value}");  // Exibe a chave e o valor da mensagem consumida no console
    Console.WriteLine($"Messagem: {result.Message.Value.Descricao}");  // Exibe a descrição do Schema
}
