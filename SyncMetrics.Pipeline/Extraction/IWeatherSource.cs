using SyncMetrics.Pipeline.Models;

namespace SyncMetrics.Pipeline.Extraction;

public interface IWeatherSource
{
    string SourceName { get; }

    bool CanHandle(string sourceName);

    Task<IReadOnlyList<CanonicalWeatherRecord>> FetchAsync(
        Location location, CancellationToken ct = default);
}