namespace SyncMetrics.Pipeline.Configuration;

/// <summary>
/// Represents the configuration settings for output file generation, including the target directory and file name
/// prefix.
/// </summary>
public sealed class OutputConfig
{
    public string Directory { get; set; } = "output";
    public string FilePrefix { get; set; } = "weather_normalized";
}
