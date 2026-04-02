using Microsoft.Extensions.Options;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Models;
using System.Globalization;
using System.Text;

namespace SyncMetrics.Pipeline.Loading;

public sealed class TabDelimitedWriter : IOutputWriter
{
    private readonly OutputConfig _config;

    private static readonly string[] Headers =
    [
        "LocationName", "Latitude", "Longitude", "Source", "Date",
        "IngestedAtUtc", "TemperatureMaxC", "TemperatureMinC",
        "PrecipitationMm", "WindSpeedMaxKmh", "UvIndexMax"
    ];

    public TabDelimitedWriter(IOptions<PipelineConfig> config) =>
        _config = config.Value.Output;

    public async Task<string> WriteAsync(IReadOnlyList<CanonicalWeatherRecord> records,
                                          CancellationToken ct = default)
    {
        if (records.Count == 0)
            throw new InvalidOperationException("No records to write.");

        Directory.CreateDirectory(_config.Directory);
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var path = Path.Combine(_config.Directory, $"{_config.FilePrefix}_{timestamp}.tsv");

        var sb = new StringBuilder();
        sb.AppendLine(string.Join('\t', Headers));

        foreach (var rec in records)
        {
            sb.AppendLine(string.Join('\t',
                Escape(rec.LocationName),
                rec.Latitude.ToString(CultureInfo.InvariantCulture),
                rec.Longitude.ToString(CultureInfo.InvariantCulture),
                Escape(rec.Source),
                rec.Date.ToString("yyyy-MM-dd"),
                rec.IngestedAtUtc.ToString("o"),
                FormatDecimal(rec.TemperatureMaxC),
                FormatDecimal(rec.TemperatureMinC),
                FormatDecimal(rec.PrecipitationMm),
                FormatDecimal(rec.WindSpeedMaxKmh),
                FormatDecimal(rec.UvIndexMax)));
        }

        await File.WriteAllTextAsync(path, sb.ToString(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), ct);
        return path;
    }

    public static string FormatDecimal(decimal? value) =>
        value.HasValue ? value.Value.ToString("F2", CultureInfo.InvariantCulture) : "";

    public static string? Escape(string? value) =>
        value?.Replace("\t", " ").Replace("\n", " ").Replace("\r", " ");
}