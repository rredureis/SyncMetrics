using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Models;
using System.Reflection;
using System.Text.Json.Serialization;

namespace SyncMetrics.Pipeline.Extraction;

public abstract class BaseWeatherSource<TDailyData> : IWeatherSource
{
    protected readonly SourceConfig Config;

    // Static per-type-instantiation: computed once per TDailyData, shared across all instances.
    private static readonly IReadOnlyDictionary<string, PropertyInfo> SourceProperties =
        BuildSourcePropertyMap();

    private static readonly IReadOnlyDictionary<string, PropertyInfo> TargetProperties =
        typeof(CanonicalWeatherRecord)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(p => p.Name);
    protected BaseWeatherSource(SourceConfig config) => Config = config;

    public abstract string SourceName { get; }

    public abstract bool CanHandle(string sourceName);

    public abstract Task<IReadOnlyList<CanonicalWeatherRecord>> FetchAsync(
        Location location, CancellationToken ct = default);

    protected virtual List<CanonicalWeatherRecord> MapToCanonical(
        TDailyData dailyData, IReadOnlyList<string> dates, Location location)
    {
        var now = DateTimeOffset.UtcNow;
        var records = new List<CanonicalWeatherRecord>();

        // Resolve source arrays once per call — avoids repeated reflection per row
        var fieldArrays = Config.FieldMappings
            .Select(m => (Mapping: m, Array: GetSourceArray(dailyData, m.Source)))
            .ToList();

        for (var i = 0; i < dates.Count; i++)
        {
            if (!DateOnly.TryParse(dates[i], out var date))
                continue;

            var record = new CanonicalWeatherRecord
            {
                LocationName = location.Name,
                Latitude = location.Latitude,
                Longitude = location.Longitude,
                Source = SourceName,
                Date = date,
                IngestedAtUtc = now
            };

            foreach (var (mapping, arr) in fieldArrays)
            {
                if (!TargetProperties.TryGetValue(mapping.Target, out var targetProp))
                    continue;

                var rawValue = arr is not null && i < arr.Count ? arr[i] : null;
                targetProp.SetValue(record, ConvertValue(rawValue, mapping.Type, targetProp.PropertyType));
            }

            records.Add(record);
        }

        return records;
    }

    private static IReadOnlyDictionary<string, PropertyInfo> BuildSourcePropertyMap()
    {
        var dict = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (var prop in typeof(TDailyData).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            // JsonPropertyName takes precedence — matches JSON field names like "temperature_2m_max"
            var jsonAttr = prop.GetCustomAttribute<JsonPropertyNameAttribute>();
            if (jsonAttr is not null)
                dict[jsonAttr.Name] = prop;

            // Property name registered as fallback
            dict.TryAdd(prop.Name, prop);
        }
        return dict;
    }

    private static object? ConvertValue(decimal? value, string mappingType, Type targetType)
    {
        if (!value.HasValue)
            return null;

        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        return mappingType.ToLowerInvariant() switch
        {
            "decimal" => (decimal?)value.Value,
            "double" => (double?)Convert.ToDouble(value.Value),
            "float" => (float?)Convert.ToSingle(value.Value),
            "int" or "integer" => (int?)Convert.ToInt32(value.Value),
            "string" => value.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => Convert.ChangeType(value.Value, underlying)
        };
    }

    private static List<decimal?>? GetSourceArray(TDailyData dailyData, string sourceName)
    {
        if (!SourceProperties.TryGetValue(sourceName, out var prop))
            return null;
        return prop.GetValue(dailyData) as List<decimal?>;
    }
}