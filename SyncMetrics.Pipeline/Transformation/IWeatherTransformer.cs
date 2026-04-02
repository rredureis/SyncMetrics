// === Transformation/IWeatherTransformer.cs ===

using SyncMetrics.Pipeline.Models;

namespace SyncMetrics.Pipeline.Transformation;

public interface IWeatherTransformer
{
    (IReadOnlyList<CanonicalWeatherRecord> Valid, List<string> Warnings)
        Validate(IReadOnlyList<CanonicalWeatherRecord> records);
}