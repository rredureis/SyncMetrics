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

    public OpenMeteoSource(IHttpClientWrapper http, IOptions<PipelineConfig> config)
        : base(config.Value.Sources.TryGetValue("OpenMeteo", out var src)
            ? src
            : throw new InvalidOperationException("OpenMeteo source not configured."))
    {
        _http = http;
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
        return MapToCanonical(response.Daily!, response.Daily!.Time!, location);
    }

    private static OpenMeteoResponse ParseResponse(string json)
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
            throw new InvalidOperationException("OpenMeteo response missing daily time series data.");

        return response;
    }

    private string BuildUrl(Location loc)
    {
        var lat = loc.Latitude.ToString(CultureInfo.InvariantCulture);
        var lon = loc.Longitude.ToString(CultureInfo.InvariantCulture);
        return $"{Config.BaseUrl}?latitude={lat}&longitude={lon}" +
               $"&daily={Config.DailyFields}&timezone=auto&forecast_days={Config.ForecastDays}";
    }
}