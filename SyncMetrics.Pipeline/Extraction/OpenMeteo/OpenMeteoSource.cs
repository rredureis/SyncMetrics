using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Extraction.OpenMeteo.Models;
using SyncMetrics.Pipeline.Models;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SyncMetrics.Pipeline.Extraction.OpenMeteo;

public sealed class OpenMeteoSource : BaseExtractionSource<OpenMeteoDailyData, CanonicalWeatherRecord>, IWeatherSource
{
    private const string sourceString = "OpenMeteo";
    private readonly IHttpClientWrapper _http;
    public OpenMeteoSource(IHttpClientWrapper http, IOptions<PipelineConfig> config, ILogger<OpenMeteoSource> logger)
        : base(ResolveConfig(config), logger)
    {
        _http = http;
        if (!config.Value.Sources.ContainsKey(sourceString))
            logger.LogError("OpenMeteo source configuration not found in appsettings; all fetch operations will fail.");
    }

    public override string SourceName => sourceString;

    public override bool CanHandle(string sourceName) =>
        string.Equals(sourceName, SourceName, StringComparison.OrdinalIgnoreCase);

    public override async Task<IReadOnlyList<CanonicalWeatherRecord>> FetchAsync(
        Location location, CancellationToken ct = default)
    {
        var url = BuildUrl(location);
        var json = await _http.GetStringAsync(url, ct);
        var response = ParseResponse(json);
        if (response is null)
            return [];
        return MapToCanonical(response.Daily!, location);
    }

    protected override IReadOnlyList<RawDailyRecord> Normalize(OpenMeteoDailyData daily)
    {
        // Build source-field-name → array map from all non-Time properties.
        // JsonPropertyName takes precedence over C# property name.
        var fieldArrays = typeof(OpenMeteoDailyData)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.Name != nameof(OpenMeteoDailyData.Time))
            .Select(p =>
            {
                var name = p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? p.Name;
                return (name, list: p.GetValue(daily) as System.Collections.IList);
            })
            .ToList();

        var records = new List<RawDailyRecord>(daily.Time!.Count);
        for (var i = 0; i < daily.Time.Count; i++)
        {
            var fields = new Dictionary<string, string?>(fieldArrays.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var (name, list) in fieldArrays)
            {
                var val = list is not null && i < list.Count ? list[i] : null;
                // Use InvariantCulture so decimal values always use '.' as separator.
                fields[name] = val is IFormattable f
                    ? f.ToString(null, CultureInfo.InvariantCulture)
                    : val?.ToString();
            }
            records.Add(new RawDailyRecord { Date = daily.Time[i], Fields = fields });
        }
        return records;
    }

    private static SourceConfig ResolveConfig(IOptions<PipelineConfig> config) =>
        config.Value.Sources.GetValueOrDefault(sourceString) ?? new SourceConfig();

    private string BuildUrl(Location loc)
    {
        var lat = loc.Latitude.ToString(CultureInfo.InvariantCulture);
        var lon = loc.Longitude.ToString(CultureInfo.InvariantCulture);
        return $"{Config.BaseUrl}?latitude={lat}&longitude={lon}" +
               $"&daily={Config.DailyFields}&timezone=auto&forecast_days={Config.ForecastDays}";
    }

    private OpenMeteoResponse? ParseResponse(string json)
    {
        OpenMeteoResponse? response;
        try
        {
            response = JsonSerializer.Deserialize<OpenMeteoResponse>(json);
        }
        catch (JsonException ex)
        {
            Logger.LogError(ex, "Malformed JSON received from OpenMeteo.");
            return null;
        }

        if (response?.Daily?.Time is null || response.Daily.Time.Count == 0)
        {
            Logger.LogError("OpenMeteo response is missing daily time series data.");
            return null;
        }

        return response;
    }
}