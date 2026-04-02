namespace SyncMetrics.Pipeline.Models
{
    public interface ICanonicalRecord
    {
        public string? LocationName { get; init; }
        public double Latitude { get; init; }
        public double Longitude { get; init; }
        public string? Source { get; init; }
        public DateOnly Date { get; init; }
        public DateTimeOffset IngestedAtUtc { get; init; }
    }
}