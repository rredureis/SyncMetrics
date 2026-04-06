using System.Text.Json.Serialization;

namespace SyncMetrics.Pipeline.Extraction.Wttr.Models;

public sealed class WttrDayData
{
    [JsonPropertyName("date")]
    public string? Date { get; init; }

    [JsonPropertyName("maxtempC")]
    public string? MaxtempC { get; init; }

    [JsonPropertyName("mintempC")]
    public string? MintempC { get; init; }

    [JsonPropertyName("uvIndex")]
    public string? UvIndex { get; init; }

    [JsonPropertyName("hourly")]
    public List<WttrHourlyData>? Hourly { get; init; }
}
