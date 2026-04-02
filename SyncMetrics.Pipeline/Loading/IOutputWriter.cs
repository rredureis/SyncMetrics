using SyncMetrics.Pipeline.Models;

namespace SyncMetrics.Pipeline.Loading;

public interface IOutputWriter
{
    Task<string> WriteAsync(IReadOnlyList<CanonicalWeatherRecord> records,
                            CancellationToken ct = default);
}