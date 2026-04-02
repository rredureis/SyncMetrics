namespace SyncMetrics.Pipeline.Configuration;

public sealed class OutputConfig
{
    public string Directory { get; set; } = "output";
    public string FilePrefix { get; set; } = "weather_normalized";
}
