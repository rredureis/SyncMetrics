using Microsoft.Extensions.Options;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Extraction.OpenMeteo.Models;
using SyncMetrics.Pipeline.Models;
using System.Globalization;
using System.Text.Json;

namespace SyncMetrics.Pipeline.Extraction.OpenMeteo;

public sealed class OpenMeteoSource : IWeatherSource
{
    private readonly IHttpClientWrapper _http;
    private readonly SourceConfig _config;

    public string SourceName => "OpenMeteo";

    public OpenMeteoSource(IHttpClientWrapper http, IOptions<PipelineConfig> config)
    {
        _http = http;
        _config = config.Value.Sources.TryGetValue(SourceName, out var src)
            ? src
            : throw new InvalidOperationException("OpenMeteo source not configured.");
    }

    public bool CanHandle(string sourceName) =>
        string.Equals(sourceName, SourceName, StringComparison.OrdinalIgnoreCase);

    public async Task<IReadOnlyList<CanonicalWeatherRecord>> FetchAsync(
        Location location, CancellationToken ct = default)
    {
        var url = BuildUrl(location);
        var json = await _http.GetStringAsync(url, ct);
        var response = ParseResponse(json);
        return MapToCanonical(response, location);
    }

    public string BuildUrl(Location loc)
    {
        var lat = loc.Latitude.ToString(CultureInfo.InvariantCulture);
        var lon = loc.Longitude.ToString(CultureInfo.InvariantCulture);
        return $"{_config.BaseUrl}?latitude={lat}&longitude={lon}" +
               $"&daily={_config.DailyFields}&timezone=auto&forecast_days={_config.ForecastDays}";
    }

    internal static OpenMeteoResponse ParseResponse(string json)
    {
        OpenMeteoResponse? response;
        try
        {
            response = JsonSerializer.Deserialize<OpenMeteoResponse>(json);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Malformed JSON from OpenMeteo: {ex.Message}", ex);
        }

        if (response?.Daily?.Time is null || response.Daily.Time.Count == 0)
        {
            throw new InvalidOperationException("OpenMeteo response missing daily time series data.");
        }

        return response;
    }

    internal List<CanonicalWeatherRecord> MapToCanonical(
        OpenMeteoResponse response, Location location)
    {
        var daily = response.Daily!;
        var now = DateTimeOffset.UtcNow;

        // Build target-field → data-array lookup driven entirely by config FieldMappings
        var fieldArrays = _config.FieldMappings.ToDictionary(
            m => m.Target,
            m => daily.GetFieldData(m.Source));

        decimal? Get(string target, int i) =>
            fieldArrays.TryGetValue(target, out var arr) ? SafeIndex(arr, i) : null;

        var records = new List<CanonicalWeatherRecord>();
        for (var i = 0; i < daily.Time!.Count; i++)
        {
            if (!DateOnly.TryParse(daily.Time[i], out var date))
            {
                // Skip unparseable dates without failing the entire batch
                continue;
            }

            records.Add(new CanonicalWeatherRecord
            {
                LocationName = location.Name,
                Latitude = location.Latitude,
                Longitude = location.Longitude,
                Source = "OpenMeteo",
                Date = date,
                IngestedAtUtc = now,
                TemperatureMaxC = Get("TemperatureMaxC", i),
                TemperatureMinC = Get("TemperatureMinC", i),
                PrecipitationMm = Get("PrecipitationMm", i),
                WindSpeedMaxKmh = Get("WindSpeedMaxKmh", i),
                UvIndexMax = Get("UvIndexMax", i)
            });
        }

        return records;
    }

    private static decimal? SafeIndex(List<decimal?>? list, int index) =>
        list is not null && index < list.Count ? list[index] : null;
}