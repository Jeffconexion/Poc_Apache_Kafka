using Confluent.Kafka;  // Importação do namespace Confluent.Kafka

var config = new ProducerConfig { BootstrapServers = "localhost:9092" };  // Criação da configuração do produtor Kafka com o endereço do servidor
using var producer = new ProducerBuilder<string, string>(config).Build();  // Criação do produtor Kafka

var message = new Message<string, string>  // Criação da mensagem a ser produzida
{
    Key = Guid.NewGuid().ToString(),  // Geração de uma chave aleatória usando Guid
    Value = $"Mensagem teste - {Guid.NewGuid()}"  // Geração de um valor aleatório para a mensagem
};

try  // Bloco de código a ser executado e que pode gerar exceções
{
    var result = await producer.ProduceAsync("topico-teste", message);  // Produção assíncrona da mensagem no tópico especificado
    Console.WriteLine($"{result.Offset}");  // Impressão do deslocamento (offset) da mensagem
    Console.WriteLine("Programa executado com sucesso!");  // Impressão de uma mensagem de sucesso
}
catch (Exception ex)  // Captura e tratamento de exceções
{
    Console.WriteLine($"Erro ao produzir mensagem: {ex.Message}");  // Impressão de uma mensagem de erro com a descrição da exceção
}
