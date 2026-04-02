using System.Text.Json.Serialization;

namespace SyncMetrics.Pipeline.Extraction.OpenMeteo.Models;

public sealed class OpenMeteoDailyData
{
    [JsonPropertyName("time")]
    public List<string>? Time { get; set; }

    [JsonPropertyName("temperature_2m_max")]
    public List<decimal?>? Temperature2mMax { get; set; }

    [JsonPropertyName("temperature_2m_min")]
    public List<decimal?>? Temperature2mMin { get; set; }

    [JsonPropertyName("precipitation_sum")]
    public List<decimal?>? PrecipitationSum { get; set; }

    [JsonPropertyName("wind_speed_10m_max")]
    public List<decimal?>? WindSpeed10mMax { get; set; }

    [JsonPropertyName("uv_index_max")]
    public List<decimal?>? UvIndexMax { get; set; }
}