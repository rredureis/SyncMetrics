namespace SyncMetrics.Pipeline.Configuration;

/// <summary>
/// Represents the configuration settings for a data processing pipeline, including locations, sources, output, and
/// retry policies.
/// </summary>
public sealed class PipelineConfig
{
    public List<LocationConfig> Locations { get; set; } = [];
    public Dictionary<string, SourceConfig> Sources { get; set; } = [];
    public OutputConfig Output { get; set; } = new();
    public RetryConfig Retry { get; set; } = new();
}