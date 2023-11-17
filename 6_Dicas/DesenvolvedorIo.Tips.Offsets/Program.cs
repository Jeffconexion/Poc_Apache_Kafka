using Confluent.Kafka;

const string Topico = "topic_60";

var i = 1;
for (i = 1; i <= 5; i++)
{
    await Produzir(i);
}

//_ = Task.Run(() => Consumir("grupo1", AutoOffsetReset.Earliest));
//_ = Task.Run(() => Consumir("grupo2", AutoOffsetReset.Earliest));
//_ = Task.Run(() => Consumir("grupo3", AutoOffsetReset.Earliest));
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
        EnableAutoOffsetStore = false,

        // Configurar para consumir apenas mensagens confirmadas.
        IsolationLevel = IsolationLevel.ReadCommitted,
    };

    using var consumer = new ConsumerBuilder<string, string>(conf).Build();

    consumer.Subscribe(Topico);

    int Tentativas = 0;

    while (true)
    {
        var result = consumer.Consume();

        if (result.IsPartitionEOF)
        {
            continue;
        }

        //var messsage = "<< Recebida: \t" + result.Message.Value + $" - {consumerId}-{autoOffsetReset}-{clientId}";
        var messsage = "<< Recebida: \t" + result.Message.Value;
        Console.WriteLine(messsage);

        // Tentar processar mensagem
        Tentativas++;
        if (!ProcessarMensagem(result) && Tentativas < 3)
        {
            consumer.Seek(result.TopicPartitionOffset);

            continue;
        }

        if (Tentativas > 1)
        {
            // Publicar mensagem em uma fila para analise!
            Console.WriteLine("Enviando mensagem para: DeadLetter");
            Tentativas = 0;
        }

        consumer.Commit(result);
        consumer.StoreOffset(result.TopicPartitionOffset);
    }
}

static bool ProcessarMensagem(ConsumeResult<string, string> result)
{
    Console.WriteLine($"KEY:{result.Message.Key} - {DateTime.Now}");
    Task.Delay(2000).Wait();
    return false;
}