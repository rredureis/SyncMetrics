namespace SyncMetrics.Pipeline.Configuration;

public sealed class SourceConfig
{
    public string BaseUrl { get; set; } = string.Empty;
    public string DailyFields { get; set; } = string.Empty;
    public int ForecastDays { get; set; } = 7;
    public List<FieldMapping> FieldMappings { get; set; } = [];
}
