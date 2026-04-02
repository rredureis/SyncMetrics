
namespace SyncMetrics.Pipeline.Extraction;

public interface IHttpClientWrapper
{
    Task<string> GetStringAsync(string url, CancellationToken ct = default);
}