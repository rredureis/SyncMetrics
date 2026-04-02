using SyncMetrics.Pipeline.Models;

namespace SyncMetrics.Pipeline.Orchestration;

public sealed class PipelineSummary
{
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset CompletedAt { get; init; }
    public TimeSpan TotalDuration { get; init; }
    public int TotalLocations { get; init; }
    public int SuccessfulLocations { get; init; }
    public int FailedLocations { get; init; }
    public int TotalRecords { get; init; }
    public List<string>? AllWarnings { get; init; }
    public List<IngestionResult>? Results { get; init; }
    public string? OutputFilePath { get; init; }

    public void PrintToConsole()
    {
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════");
        Console.WriteLine("  PIPELINE EXECUTION SUMMARY");
        Console.WriteLine("═══════════════════════════════════════════");
        Console.WriteLine($"  Started:    {StartedAt:yyyy-MM-dd HH:mm:ss UTC}");
        Console.WriteLine($"  Completed:  {CompletedAt:yyyy-MM-dd HH:mm:ss UTC}");
        Console.WriteLine($"  Duration:   {TotalDuration.TotalSeconds:F1}s");
        Console.WriteLine("───────────────────────────────────────────");
        Console.WriteLine($"  Locations:  {SuccessfulLocations}/{TotalLocations} succeeded");
        Console.WriteLine($"  Records:    {TotalRecords} total");
        Console.WriteLine($"  Warnings:   {AllWarnings?.Count}");
        Console.WriteLine($"  Output:     {OutputFilePath ?? "none"}");
        Console.WriteLine("───────────────────────────────────────────");

        if (Results != null)
        {
            foreach (var r in Results)
            {
                var icon = r.Success ? "OK" : "FAIL";
                Console.WriteLine($"  [{icon}] {r.LocationName} ({r.Source}): " +
                                  $"{r.RecordsProduced} records in {r.Duration.TotalMilliseconds:F0}ms" +
                                  (r.Error is not null ? $" — {r.Error}" : ""));
            }
        }


        if (AllWarnings?.Count > 0)
        {
            Console.WriteLine("───────────────────────────────────────────");
            Console.WriteLine("  Warnings:");
            foreach (var w in AllWarnings.Take(20))
                Console.WriteLine($"    - {w}");
            if (AllWarnings.Count > 20)
                Console.WriteLine($"    ... and {AllWarnings.Count - 20} more");
        }

        Console.WriteLine("═══════════════════════════════════════════");
    }
}