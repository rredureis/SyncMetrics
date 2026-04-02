namespace SyncMetrics.Pipeline.Extraction;

/// <summary>
/// Source-agnostic intermediate record produced by each source's <c>Normalize</c> step.
/// One instance per day; field values are strings keyed by the source field name.
/// The base class maps these to canonical records driven by <c>FieldMappings</c> config.
/// </summary>
public sealed class RawDailyRecord
{
    public string Date { get; init; } = string.Empty;
    public IReadOnlyDictionary<string, string?> Fields { get; init; } =
        new Dictionary<string, string?>();
}
