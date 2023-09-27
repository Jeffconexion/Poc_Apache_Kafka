using Confluent.Kafka;
using System;
using System.IO;
using System.IO.Compression;
using System.Text.Json;

namespace DevStore.MessageBus.Serializador
{
    public class DeserializerDevStore<T> : IDeserializer<T>
    {
        // Este método deserializa um array de bytes em um objeto genérico.
        // Ele recebe o array de bytes a ser deserializado (data), um indicador de nulidade (isNull) e o contexto de deserialização (context),
        // e retorna um objeto do tipo T deserializado.
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            // Cria um MemoryStream com base no array de bytes a ser deserializado.
            using var memoryStream = new MemoryStream(data.ToArray());

            // Cria um GZipStream para descomprimir os dados do MemoryStream.
            // O GZipStream é inicializado com o memoryStream como origem, usando o modo de descompressão e 
            // a opção de deixar o stream aberto após a descompressão.
            using var zip = new GZipStream(memoryStream, CompressionMode.Decompress, true);

            // Deserializa o objeto do tipo T do fluxo descomprimido usando o método Deserialize<T> da classe JsonSerializer.
            var objDesserialize = JsonSerializer.Deserialize<T>(zip);

            // Retorna o objeto deserializado.
            return objDesserialize;
        }

    }
}
