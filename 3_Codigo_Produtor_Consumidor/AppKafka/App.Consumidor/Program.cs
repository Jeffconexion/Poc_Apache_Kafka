using Confluent.Kafka;

var config = new ConsumerConfig
{
    GroupId = "devTreinamento",  // Definimos o ID do grupo para o consumidor
    BootstrapServers = "localhost:9092"  // Definimos o endereço do servidor Kafka
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();  // Cria um consumidor Kafka com base na configuração fornecida
consumer.Subscribe("topico-teste");  // Inscreve o consumidor no tópico especificado

while (true)  // Loop infinito para receber continuamente mensagens
{
    var result = consumer.Consume();  // Aguarda e consome uma mensagem do tópico inscrito
    Console.WriteLine($"Messagem: {result.Message.Key}-{result.Message.Value}");  // Exibe a chave e o valor da mensagem consumida no console
}
