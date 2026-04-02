namespace SyncMetrics.Pipeline.Configuration;

/// <summary>
/// Represents the configuration details for a geographic location, including its name and coordinates.
/// </summary>
public sealed class LocationConfig
{
    public string Name { get; set; } = string.Empty;
    public double Latitude { get; set; }
    public double Longitude { get; set; }
}
