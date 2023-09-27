using Confluent.Kafka;
using System.IO;
using System.IO.Compression;
using System.Text.Json;

namespace DevStore.MessageBus.Serializador
{
    public class SerializerDevStore<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            // Converte o objeto para um array de bytes usando o método SerializeToUtf8Bytes da classe JsonSerializer.
            var bytes = JsonSerializer.SerializeToUtf8Bytes(data);

            // Cria um MemoryStream para armazenar os dados comprimidos.
            using var memoryStream = new MemoryStream();

            // Cria um GZipStream para realizar a compressão dos dados.
            // O GZipStream é inicializado com o memoryStream como destino, usando o modo de compressão e 
            // a opção de deixar o stream aberto após a compressão.
            using var zipStream = new GZipStream(memoryStream, CompressionMode.Compress, true);

            // Grava os bytes do objeto serializado no GZipStream.
            zipStream.Write(bytes, 0, bytes.Length);

            // Finaliza a compressão e fecha o GZipStream.
            zipStream.Close();

            // Converte o memoryStream em um array de bytes.
            var buffer = memoryStream.ToArray();

            // Retorna o array de bytes representando o objeto serializado e comprimido.
            return buffer;
        }
    }
}
