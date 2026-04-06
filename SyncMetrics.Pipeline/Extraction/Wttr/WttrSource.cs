using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Extraction.Wttr.Models;
using SyncMetrics.Pipeline.Models;
using System.Globalization;
using System.Text.Json;

namespace SyncMetrics.Pipeline.Extraction.Wttr;

public sealed class WttrSource : BaseExtractionSource<WttrResponse, CanonicalWeatherRecord>, IWeatherSource
{
    private const string SourceString = "Wttr";
    private readonly IHttpClientWrapper _http;

    public WttrSource(IHttpClientWrapper http, IOptions<PipelineConfig> config, ILogger<WttrSource> logger)
        : base(ResolveConfig(config), logger)
    {
        _http = http;
        if (!config.Value.Sources.ContainsKey(SourceString))
            logger.LogError("Wttr source configuration not found in appsettings; all fetch operations will fail.");
    }

    public override string SourceName => SourceString;

    public override bool CanHandle(string sourceName) =>
        string.Equals(sourceName, SourceName, StringComparison.OrdinalIgnoreCase);

    public override async Task<IReadOnlyList<CanonicalWeatherRecord>> FetchAsync(
        Location location, CancellationToken ct = default)
    {
        var url = BuildUrl(location);
        var json = await _http.GetStringAsync(url, ct);
        var response = ParseResponse(json);
        if (response is null)
            throw new InvalidOperationException($"Failed to parse response from {SourceName}; see logs for details.");
        return MapToCanonical(response, location);
    }

    protected override IReadOnlyList<RawDailyRecord> Normalize(WttrResponse sourceData)
    {
        var records = new List<RawDailyRecord>(sourceData.Weather!.Count);
        foreach (var day in sourceData.Weather)
        {
            decimal precipSum = 0m;
            decimal windMax = 0m;
            foreach (var hour in day.Hourly ?? [])
            {
                if (decimal.TryParse(hour.PrecipMM, NumberStyles.Any, CultureInfo.InvariantCulture, out var precip))
                    precipSum += precip;
                if (decimal.TryParse(hour.WindspeedKmph, NumberStyles.Any, CultureInfo.InvariantCulture, out var wind))
                    windMax = Math.Max(windMax, wind);
            }

            var fields = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["maxtempC"]             = day.MaxtempC,
                ["mintempC"]             = day.MintempC,
                ["uvIndex"]              = day.UvIndex,
                ["precipitation_sum_mm"] = precipSum.ToString(CultureInfo.InvariantCulture),
                ["windspeed_max_kmph"]   = windMax.ToString(CultureInfo.InvariantCulture)
            };

            records.Add(new RawDailyRecord { Date = day.Date ?? string.Empty, Fields = fields });
        }
        return records;
    }

    private static SourceConfig ResolveConfig(IOptions<PipelineConfig> config) =>
        config.Value.Sources.GetValueOrDefault(SourceString) ?? new SourceConfig();

    private string BuildUrl(Location loc)
    {
        var lat = loc.Latitude.ToString(CultureInfo.InvariantCulture);
        var lon = loc.Longitude.ToString(CultureInfo.InvariantCulture);
        return $"{Config.BaseUrl}/{lat},{lon}?format=j1";
    }

    private WttrResponse? ParseResponse(string json)
    {
        WttrResponse? response;
        try
        {
            response = JsonSerializer.Deserialize<WttrResponse>(json);
        }
        catch (JsonException ex)
        {
            Logger.LogError(ex, "Malformed JSON received from Wttr.");
            return null;
        }

        if (response?.Weather is null || response.Weather.Count == 0)
        {
            Logger.LogError("Wttr response is missing weather data.");
            return null;
        }

        return response;
    }
}
