namespace SyncMetrics.Pipeline.Configuration;

public sealed class PipelineConfig
{
    public List<LocationConfig> Locations { get; set; } = [];
    public Dictionary<string, SourceConfig> Sources { get; set; } = [];
    public OutputConfig Output { get; set; } = new();
    public RetryConfig Retry { get; set; } = new();
}