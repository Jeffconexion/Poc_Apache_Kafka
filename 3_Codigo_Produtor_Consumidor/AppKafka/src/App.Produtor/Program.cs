using Confluent.Kafka;  // Importação do namespace Confluent.Kafka
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

//configuração do arquivo yml
var schemaConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);


var config = new ProducerConfig { BootstrapServers = "localhost:9092" };  // Criação da configuração do produtor Kafka com o endereço do servidor
//using var producer = new ProducerBuilder<string, string>(config).Build();  //~~> Criação do produtor Kafka

using var producer = new ProducerBuilder<string, treinamento.Curso>(config)
                     .SetValueSerializer(new AvroSerializer<treinamento.Curso>(schemaRegistry))
                     .Build();  //~~> Criação do produtor Kafka com ~~> schema

// Criação da mensagem a ser produzida
//var message = new Message<string, string>  
//{
//    Key = Guid.NewGuid().ToString(),  // Geração de uma chave aleatória usando Guid
//    Value = $"Mensagem teste - {Guid.NewGuid()}"  // Geração de um valor aleatório para a mensagem
//};

// Criação da mensagem a ser produzida com Schema
var message = new Message<string, treinamento.Curso>
{
    Key = Guid.NewGuid().ToString(),
    Value = new treinamento.Curso
    {
        Id = Guid.NewGuid().ToString(),
        Descricao = "Curso de Apache Kafka"
    }
};

try  // Bloco de código a ser executado e que pode gerar exceções
{
    var result = await producer.ProduceAsync("Cursos", message);  // Produção assíncrona da mensagem no tópico especificado
    Console.WriteLine($"{result.Offset}");  // Impressão do deslocamento (offset) da mensagem
    Console.WriteLine("Programa executado com sucesso!");  // Impressão de uma mensagem de sucesso
}
catch (Exception ex)  // Captura e tratamento de exceções
{
    Console.WriteLine($"Erro ao produzir mensagem: {ex.Message}");  // Impressão de uma mensagem de erro com a descrição da exceção
}
