using Microsoft.Extensions.Logging;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Models;
using System.Globalization;
using System.Reflection;

namespace SyncMetrics.Pipeline.Extraction;

/// <summary>
/// Provides a base class for extraction sources that transform source data into canonical records using configurable
/// field mappings.
/// </summary>
/// <remarks>This class defines the core workflow for mapping and normalizing source data into canonical records,
/// including field mapping validation and logging. Derived classes must implement normalization logic specific to their
/// data source. </remarks>
/// <typeparam name="TSourceData">The type representing the raw source data to be normalized and mapped.</typeparam>
/// <typeparam name="TCanonicalRecord">The type of the canonical record produced by the extraction source. Must implement ICanonicalRecord</typeparam>
public abstract class BaseExtractionSource<TSourceData, TCanonicalRecord>
    where TCanonicalRecord : ICanonicalRecord, new()
{
    protected readonly SourceConfig Config;
    protected readonly ILogger Logger;

    // Target properties cached per TCanonicalRecord type-instantiation.
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

    protected virtual List<TCanonicalRecord> MapToCanonical(TSourceData sourceData, Location location)
    {
        var rawRecords = Normalize(sourceData);
        var now = DateTimeOffset.UtcNow;
        var records = new List<TCanonicalRecord>(rawRecords.Count);

        // Validate target mappings once and log any issues before iterating rows.
        var resolvedMappings = new List<(FieldMapping Mapping, PropertyInfo TargetProp)>();
        foreach (var m in Config.FieldMappings)
        {
            if (!TargetProperties.TryGetValue(m.Target, out var targetProp))
            {
                Logger.LogWarning(
                    "FieldMapping target '{Target}' not found on {RecordType}; mapping skipped.",
                    m.Target, typeof(TCanonicalRecord).Name);
                continue;
            }
            resolvedMappings.Add((m, targetProp));
        }

        // Warn once against the first record if a source field is absent.
        if (rawRecords.Count > 0)
        {
            foreach (var (m, _) in resolvedMappings)
            {
                if (!rawRecords[0].Fields.ContainsKey(m.Source))
                    Logger.LogWarning(
                        "FieldMapping source '{Source}' not found in normalized data; field will be null.",
                        m.Source);
            }
        }

        foreach (var raw in rawRecords)
        {
            if (!DateOnly.TryParse(raw.Date, out var date))
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

            foreach (var (mapping, targetProp) in resolvedMappings)
            {
                raw.Fields.TryGetValue(mapping.Source, out var rawValue);
                targetProp.SetValue(record, ConvertValue(rawValue, mapping.Type, targetProp.PropertyType));
            }

            records.Add(record);
        }

        return records;
    }

    /// <summary>
    /// Transforms the specified source data into a normalized collection of daily records.
    /// </summary>
    /// <remarks>Implementations should ensure that the returned records accurately represent the normalized
    /// form of the input data. The method does not modify the input parameter.</remarks>
    /// <param name="sourceData">The input data to be normalized. Must contain the raw information required to produce daily records.</param>
    /// <returns>A read-only list of normalized daily records derived from the source data. The list will be empty if no records
    /// are produced.</returns>
    protected abstract IReadOnlyList<RawDailyRecord> Normalize(TSourceData sourceData);

    /// <summary>
    /// Converts a raw string value to an object of the specified target type using the provided mapping type.
    /// </summary>
    /// <remarks>If the conversion cannot be performed due to format, overflow, or invalid cast, the method
    /// returns null and logs a warning. Supported mapping types include common primitives such as "int", "decimal",
    /// "double", "float", and "string". For other types, standard type conversion is attempted using the invariant
    /// culture.</remarks>
    /// <param name="rawValue">The string representation of the value to convert. If null, the method returns null.</param>
    /// <param name="mappingType">A string indicating the type to which the value should be converted (e.g., "int", "decimal", "string").
    /// Case-insensitive.</param>
    /// <param name="targetType">The target .NET type to convert the value to. May be a nullable type.</param>
    /// <returns>An object representing the converted value, or null if the conversion fails or the input value is null.</returns>
    private object? ConvertValue(string? rawValue, string mappingType, Type targetType)
    {
        if (rawValue is null)
            return null;

        try
        {
            var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

            return mappingType.ToLowerInvariant() switch
            {
                "decimal" => (decimal?)decimal.Parse(rawValue, CultureInfo.InvariantCulture),
                "double" => (double?)double.Parse(rawValue, CultureInfo.InvariantCulture),
                "float" => (float?)float.Parse(rawValue, CultureInfo.InvariantCulture),
                "int" or "integer" => (int?)int.Parse(rawValue, CultureInfo.InvariantCulture),
                "string" => rawValue,
                _ => Convert.ChangeType(rawValue, underlying, CultureInfo.InvariantCulture)
            };
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or InvalidCastException)
        {
            Logger.LogWarning(
                "Could not convert value '{Value}' as type '{MappingType}'; field will be null.",
                rawValue, mappingType);
            return null;
        }
    }
}