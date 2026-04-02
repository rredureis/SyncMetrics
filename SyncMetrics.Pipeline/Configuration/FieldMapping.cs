namespace SyncMetrics.Pipeline.Configuration;

/// <summary>
/// Represents a mapping between a source field and a target field, including the data type to use for the mapping.
/// </summary>
/// <remarks>Use this class to define how data should be transferred or transformed from one field to another,
/// typically in data migration or integration scenarios. The default type is "decimal" if not specified.</remarks>
public sealed class FieldMapping
{
    public string Source { get; set; } = string.Empty;
    public string Target { get; set; } = string.Empty;
    public string Type { get; set; } = "decimal";
}
