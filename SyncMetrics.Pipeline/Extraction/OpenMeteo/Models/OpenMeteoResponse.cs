using System.Text.Json.Serialization;

namespace SyncMetrics.Pipeline.Extraction.OpenMeteo.Models;

public sealed class OpenMeteoResponse
{
    [JsonPropertyName("latitude")]
    public double Latitude { get; set; }

    [JsonPropertyName("longitude")]
    public double Longitude { get; set; }

    [JsonPropertyName("daily")]
    public OpenMeteoDailyData? Daily { get; set; }
}