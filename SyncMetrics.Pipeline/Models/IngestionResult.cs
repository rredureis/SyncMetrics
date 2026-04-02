namespace SyncMetrics.Pipeline.Models;

public sealed class IngestionResult
{
    public string? LocationName { get; init; }
    public string? Source { get; init; }
    public bool Success { get; init; }
    public int RecordsProduced { get; init; }
    public string? Error { get; init; }
    public TimeSpan Duration { get; init; }
}