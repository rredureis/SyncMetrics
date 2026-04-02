using Microsoft.Extensions.Logging;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Models;
using System.Reflection;
using System.Text.Json.Serialization;

namespace SyncMetrics.Pipeline.Extraction;

/// <summary>
/// Base class for weather sources. Provides a config-driven <see cref="MapToCanonical"/> that
/// resolves source fields on <typeparamref name="TSourceData"/> by <see cref="JsonPropertyNameAttribute"/>
/// first, then by property name, and sets target fields on <see cref="TCanonicalRecord"/>
/// by the <see cref="FieldMapping.Target"/> name with type conversion driven by <see cref="FieldMapping.Type"/>.
/// </summary>
public abstract class BaseExtractionSource<TSourceData, TCanonicalRecord>
    where TCanonicalRecord : ICanonicalRecord, new()
{
    protected readonly SourceConfig Config;
    protected readonly ILogger Logger;

    // Static per-type-instantiation: computed once per TSourceData, shared across all instances.
    private static readonly IReadOnlyDictionary<string, PropertyInfo> SourceProperties =
        BuildSourcePropertyMap();

    private static readonly IReadOnlyDictionary<string, PropertyInfo> TargetProperties =
        typeof(TCanonicalRecord)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(p => p.Name);

    protected BaseExtractionSource(SourceConfig config, ILogger logger)
    {
        Config = config;
        Logger = logger;
    }

    public abstract string SourceName { get; }

    public abstract bool CanHandle(string sourceName);

    public abstract Task<IReadOnlyList<TCanonicalRecord>> FetchAsync(
        Location location, CancellationToken ct = default);

    protected virtual List<TCanonicalRecord> MapToCanonical(
        TSourceData sourceData, IReadOnlyList<string> dates, Location location)
    {
        var now = DateTimeOffset.UtcNow;
        var records = new List<TCanonicalRecord>();

        // Resolve and validate all mappings once before the row loop
        var resolvedMappings = new List<(FieldMapping Mapping, System.Collections.IList? Array, PropertyInfo TargetProp)>();
        foreach (var m in Config.FieldMappings)
        {
            if (!TargetProperties.TryGetValue(m.Target, out var targetProp))
            {
                Logger.LogWarning(
                    "FieldMapping target '{Target}' not found on {RecordType}; mapping skipped.",
                    m.Target, typeof(TCanonicalRecord).Name);
                continue;
            }

            if (!SourceProperties.ContainsKey(m.Source))
                Logger.LogWarning(
                    "FieldMapping source '{Source}' not found on {DataType}; field will be null.",
                    m.Source, typeof(TSourceData).Name);

            resolvedMappings.Add((m, GetSourceArray(sourceData, m.Source), targetProp));
        }

        for (var i = 0; i < dates.Count; i++)
        {
            if (!DateOnly.TryParse(dates[i], out var date))
                continue;

            var record = new TCanonicalRecord
            {
                LocationName = location.Name,
                Latitude = location.Latitude,
                Longitude = location.Longitude,
                Source = SourceName,
                Date = date,
                IngestedAtUtc = now
            };

            foreach (var (mapping, arr, targetProp) in resolvedMappings)
            {
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
        foreach (var prop in typeof(TSourceData).GetProperties(BindingFlags.Instance | BindingFlags.Public))
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

    private static object? ConvertValue(object? rawValue, string mappingType, Type targetType)
    {
        if (rawValue is null)
            return null;

        // Step 1 — interpret rawValue as the declared source type from FieldMapping.Type
        object parsed = mappingType.ToLowerInvariant() switch
        {
            "decimal" => Convert.ToDecimal(rawValue),
            "double" => Convert.ToDouble(rawValue),
            "float" => Convert.ToSingle(rawValue),
            "int" or "integer" => Convert.ToInt32(rawValue),
            "string" => rawValue.ToString()!,
            _ => rawValue
        };

        // Step 2 — convert the intermediate value to the actual target property type
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
        return underlying == parsed.GetType()
            ? parsed
            : Convert.ChangeType(parsed, underlying);
    }

    private static System.Collections.IList? GetSourceArray(TSourceData sourceData, string sourceName)
    {
        if (!SourceProperties.TryGetValue(sourceName, out var prop))
            return null;
        return prop.GetValue(sourceData) as System.Collections.IList;
    }
}