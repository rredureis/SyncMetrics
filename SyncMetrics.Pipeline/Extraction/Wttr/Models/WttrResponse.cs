using System.Text.Json.Serialization;

namespace SyncMetrics.Pipeline.Extraction.Wttr.Models;

public sealed class WttrResponse
{
    [JsonPropertyName("weather")]
    public List<WttrDayData>? Weather { get; init; }
}
