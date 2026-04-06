using System.Text.Json.Serialization;

namespace SyncMetrics.Pipeline.Extraction.Wttr.Models;

public sealed class WttrHourlyData
{
    [JsonPropertyName("precipMM")]
    public string? PrecipMM { get; init; }

    [JsonPropertyName("windspeedKmph")]
    public string? WindspeedKmph { get; init; }
}
