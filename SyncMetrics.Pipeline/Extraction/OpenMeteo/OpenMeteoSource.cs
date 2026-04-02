using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Extraction.OpenMeteo.Models;
using SyncMetrics.Pipeline.Models;
using System.Globalization;
using System.Text.Json;

namespace SyncMetrics.Pipeline.Extraction.OpenMeteo;

public sealed class OpenMeteoSource : BaseExtractionSource<OpenMeteoDailyData, CanonicalWeatherRecord>, IWeatherSource
{
    private readonly IHttpClientWrapper _http;

    public OpenMeteoSource(IHttpClientWrapper http, IOptions<PipelineConfig> config, ILogger<OpenMeteoSource> logger)
        : base(ResolveConfig(config), logger)
    {
        _http = http;
        if (!config.Value.Sources.ContainsKey("OpenMeteo"))
            logger.LogError("OpenMeteo source configuration not found in appsettings; all fetch operations will fail.");
    }

    public override string SourceName => "OpenMeteo";

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
        return MapToCanonical(response.Daily!, response.Daily!.Time!, location);
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

    private string BuildUrl(Location loc)
    {
        var lat = loc.Latitude.ToString(CultureInfo.InvariantCulture);
        var lon = loc.Longitude.ToString(CultureInfo.InvariantCulture);
        return $"{Config.BaseUrl}?latitude={lat}&longitude={lon}" +
               $"&daily={Config.DailyFields}&timezone=auto&forecast_days={Config.ForecastDays}";
    }

    private static SourceConfig ResolveConfig(IOptions<PipelineConfig> config) =>
        config.Value.Sources.GetValueOrDefault("OpenMeteo") ?? new SourceConfig();
}
