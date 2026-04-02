using Microsoft.Extensions.Logging;
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
    private readonly ILogger<WeatherPipeline> _logger;

    public WeatherPipeline(
        IEnumerable<IWeatherSource> sources,
        IWeatherTransformer transformer,
        IOutputWriter writer,
        ILogger<WeatherPipeline> logger)
    {
        _sources = sources;
        _transformer = transformer;
        _writer = writer;
        _logger = logger;
    }

    public async Task<PipelineSummary> RunAsync(
        IReadOnlyList<Location> locations,
        string sourceName = "OpenMeteo",
        CancellationToken ct = default)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var sw = Stopwatch.StartNew();

        var source = _sources.FirstOrDefault(s => s.CanHandle(sourceName));
        if (source is null)
        {
            sw.Stop();
            _logger.LogError("No source registered for '{SourceName}'.", sourceName);
            return new PipelineSummary
            {
                StartedAt = startedAt,
                CompletedAt = DateTimeOffset.UtcNow,
                TotalDuration = sw.Elapsed,
                TotalLocations = locations.Count,
                SuccessfulLocations = 0,
                FailedLocations = locations.Count,
                TotalRecords = 0,
                AllWarnings = [$"No source registered for '{sourceName}'."],
                Results = locations.Select(l => new IngestionResult
                {
                    LocationName = l.Name,
                    Source = sourceName,
                    Success = false,
                    Error = $"No source registered for '{sourceName}'."
                }).ToList(),
                OutputFilePath = null
            };
        }

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
                _logger.LogError(ex, "Failed to write output file.");
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
            AllWarnings = warnings,
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
