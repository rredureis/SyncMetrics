using SyncMetrics.Pipeline.Extraction;
using SyncMetrics.Pipeline.Loading;
using SyncMetrics.Pipeline.Models;
using SyncMetrics.Pipeline.Transformation;
using System.Diagnostics;

namespace SyncMetrics.Pipeline.Orchestration;

public sealed class WeatherPipeline
{
    private readonly IEnumerable<IWeatherSource> _sources;
    private readonly IWeatherTransformer _transformer;
    private readonly IOutputWriter _writer;

    public WeatherPipeline(
        IEnumerable<IWeatherSource> sources,
        IWeatherTransformer transformer,
        IOutputWriter writer)
    {
        _sources = sources;
        _transformer = transformer;
        _writer = writer;
    }

    public async Task<PipelineSummary> RunAsync(
        IReadOnlyList<Location> locations,
        string sourceName = "OpenMeteo",
        CancellationToken ct = default)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var sw = Stopwatch.StartNew();

        var source = _sources.FirstOrDefault(s => s.CanHandle(sourceName))
            ?? throw new InvalidOperationException($"No source registered for '{sourceName}'.");

        // Extract — fetch all locations concurrently
        var extractionTasks = locations.Select(loc =>
            ExtractWithTracking(source, loc, ct));
        var results = await Task.WhenAll(extractionTasks);

        // Gather all records from successful extractions
        var allRecords = results
            .Where(r => r.Result.Success)
            .SelectMany(r => r.Records)
            .ToList();

        // Transform — validate and flag anomalies
        var (valid, warnings) = _transformer.Validate(allRecords);

        // Load — write output file
        string? outputPath = null;
        if (valid.Count > 0)
        {
            try
            {
                outputPath = await _writer.WriteAsync(valid, ct);
            }
            catch (Exception ex)
            {
                warnings.Add($"Output write failed: {ex.Message}");
            }
        }

        sw.Stop();

        return new PipelineSummary
        {
            StartedAt = startedAt,
            CompletedAt = DateTimeOffset.UtcNow,
            TotalDuration = sw.Elapsed,
            TotalLocations = locations.Count,
            SuccessfulLocations = results.Count(r => r.Result.Success),
            FailedLocations = results.Count(r => !r.Result.Success),
            TotalRecords = valid.Count,
            AllWarnings = warnings.Concat(results.SelectMany(r => r.Result.Warnings)).ToList(),
            Results = results.Select(r => r.Result).ToList(),
            OutputFilePath = outputPath
        };
    }

    private static async Task<(IngestionResult Result, IReadOnlyList<CanonicalWeatherRecord> Records)>
        ExtractWithTracking(IWeatherSource source, Location location, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var records = await source.FetchAsync(location, ct);
            sw.Stop();

            return (new IngestionResult
            {
                LocationName = location.Name,
                Source = source.SourceName,
                Success = true,
                RecordsProduced = records.Count,
                Duration = sw.Elapsed
            }, records);
        }
        catch (Exception ex)
        {
            sw.Stop();
            return (new IngestionResult
            {
                LocationName = location.Name,
                Source = source.SourceName,
                Success = false,
                RecordsProduced = 0,
                Error = ex.Message,
                Duration = sw.Elapsed
            }, []);
        }
    }
}