namespace SyncMetrics.Pipeline.Models;

public sealed class CanonicalWeatherRecord : ICanonicalRecord
{
    public string? LocationName { get; init; }
    public double Latitude { get; init; }
    public double Longitude { get; init; }
    public string? Source { get; init; }
    public DateOnly Date { get; init; }
    public DateTimeOffset IngestedAtUtc { get; init; }
    public decimal? TemperatureMaxC { get; init; }
    public decimal? TemperatureMinC { get; init; }
    public decimal? PrecipitationMm { get; init; }
    public decimal? WindSpeedMaxKmh { get; init; }
    public decimal? UvIndexMax { get; init; }
}