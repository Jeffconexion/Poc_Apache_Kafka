using Confluent.Kafka;

const string Topico = "topic_60";

var i = 1;
for (i = 1; i <= 5; i++)
{
    //await Produzir(i);
}

_ = Task.Run(() => Consumir("grupo1", AutoOffsetReset.Earliest));
_ = Task.Run(() => Consumir("grupo2", AutoOffsetReset.Earliest));
_ = Task.Run(() => Consumir("grupo3", AutoOffsetReset.Earliest));
_ = Task.Run(() => Consumir("grupo3", AutoOffsetReset.Latest));


while (true)
{
    Console.ReadLine();
    Produzir(i).GetAwaiter().GetResult();
    i++;
}
static async Task Produzir(int i)
{
    var configuracao = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
        Acks = Acks.All, // ~~> habilitar de que maneira essas mensagens serão entregues.
    };

    var mensagem = $"Mensagem ({i}) - *{Guid.NewGuid()} - {DateTime.Now}";
    Console.WriteLine(">> Enviada:\t " + mensagem);

    try
    {
        using var producer = new ProducerBuilder<Null, string>(configuracao).Build();

        var result = await producer.ProduceAsync(Topico, new Message<Null, string>
        {
            Value = mensagem
        });

        await Task.CompletedTask;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.ToString());
    }
}

static void Consumir(string consumerId, AutoOffsetReset autoOffsetReset)
{
    var clientId = Guid.NewGuid().ToString().Substring(0, 5);

    var conf = new ConsumerConfig
    {
        ClientId = clientId,
        GroupId = consumerId,
        BootstrapServers = "localhost:9092",
        AutoOffsetReset = autoOffsetReset,
        EnablePartitionEof = true,
        EnableAutoCommit = false,
    };

    using var consumer = new ConsumerBuilder<Null, string>(conf).Build();

    consumer.Subscribe(Topico);

    while (true)
    {
        var result = consumer.Consume();

        if (result.IsPartitionEOF)
        {
            continue;
        }

        var messsage = "<< Recebida: \t" + result.Message.Value;
        Console.WriteLine(messsage);

        consumer.Commit(result);
    }
}