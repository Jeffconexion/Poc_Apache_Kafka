using System.Threading.Tasks;

namespace DevStore.Core.Http
{
    public interface IRestClient
    {
        Task<TResult> PostAsync<T, TResult>(T @event, string token = null);
    }
}
