// === Transformation/WeatherTransformer.cs ===

using SyncMetrics.Pipeline.Models;

namespace SyncMetrics.Pipeline.Transformation;

public sealed class WeatherTransformer : IWeatherTransformer
{
    // Reasonable physical bounds — flags data quality issues without discarding records
    private const decimal MinTempC = -90m;

    private const decimal MaxTempC = 60m;
    private const decimal MaxPrecipMm = 1000m;
    private const decimal MaxWindKmh = 500m;
    private const decimal MaxUvIndex = 20m;

    public (IReadOnlyList<CanonicalWeatherRecord> Valid, List<string> Warnings)
        Validate(IReadOnlyList<CanonicalWeatherRecord> records)
    {
        var valid = new List<CanonicalWeatherRecord>();
        var warnings = new List<string>();

        foreach (var rec in records)
        {
            var recWarnings = new List<string>();

            if (rec.TemperatureMaxC.HasValue && rec.TemperatureMinC.HasValue
                && rec.TemperatureMinC > rec.TemperatureMaxC)
            {
                recWarnings.Add(
                    $"{rec.LocationName} {rec.Date}: min temp ({rec.TemperatureMinC}) > max temp ({rec.TemperatureMaxC})");
            }

            CheckBound(rec.TemperatureMaxC, MinTempC, MaxTempC,
                $"{rec.LocationName} {rec.Date}: max temp", recWarnings);
            CheckBound(rec.TemperatureMinC, MinTempC, MaxTempC,
                $"{rec.LocationName} {rec.Date}: min temp", recWarnings);
            CheckBound(rec.PrecipitationMm, 0, MaxPrecipMm,
                $"{rec.LocationName} {rec.Date}: precipitation", recWarnings);
            CheckBound(rec.WindSpeedMaxKmh, 0, MaxWindKmh,
                $"{rec.LocationName} {rec.Date}: wind speed", recWarnings);
            CheckBound(rec.UvIndexMax, 0, MaxUvIndex,
                $"{rec.LocationName} {rec.Date}: UV index", recWarnings);

            // Track whether all weather fields are null (might indicate API issue)
            if (!rec.TemperatureMaxC.HasValue && !rec.TemperatureMinC.HasValue
                && !rec.PrecipitationMm.HasValue && !rec.WindSpeedMaxKmh.HasValue
                && !rec.UvIndexMax.HasValue)
            {
                recWarnings.Add($"{rec.LocationName} {rec.Date}: all weather fields are null");
            }

            warnings.AddRange(recWarnings);
            valid.Add(rec); // Keep the record — warn but don't discard
        }

        return (valid, warnings);
    }

    private static void CheckBound(decimal? value, decimal min, decimal max,
        string label, List<string> warnings)
    {
        if (value.HasValue && (value < min || value > max))
            warnings.Add($"{label} value {value} outside expected range [{min}, {max}]");
    }
}