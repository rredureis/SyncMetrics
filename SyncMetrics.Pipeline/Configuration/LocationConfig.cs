namespace SyncMetrics.Pipeline.Configuration;

public sealed class LocationConfig
{
    public string Name { get; set; } = string.Empty;
    public double Latitude { get; set; }
    public double Longitude { get; set; }
}
