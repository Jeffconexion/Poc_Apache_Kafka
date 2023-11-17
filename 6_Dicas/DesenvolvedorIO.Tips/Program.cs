using Confluent.Kafka;


const string Topico = "testetreinamento";

var configuracao = new ProducerConfig
{
    Acks = Acks.All,
    BootstrapServers = "localhost:9092"
};

var mensage = $"Mensagem - {Guid.NewGuid()}";

try
{
    using var producer = new ProducerBuilder<Null, string>(configuracao).Build();
    var result = await producer.ProduceAsync(Topico, new Message<Null, string>
    {
        Value = mensage
    });
}
catch (Exception ex)
{
    Console.Error.WriteLine(ex.ToString());
}

