namespace SyncMetrics.Pipeline.Configuration;

/// <summary>
/// Represents a mapping between a source field and a target field, including the data type to use for the mapping.
/// </summary>
public sealed class FieldMapping
{
    /// <summary>
    /// The name of the field in the source data that you want to map from. This should correspond to a property or column name in the source data structure.
    /// </summary>
    public string Source { get; set; } = string.Empty;

    /// <summary>
    /// The name of the field in the target data structure that you want to map to.
    /// </summary>
    public string Target { get; set; } = string.Empty;

    /// <summary>
    /// The data type of the property on the Source object. This is used to determine how to read and convert the value from the source data.
    /// </summary>
    public string Type { get; set; } = "decimal";
}
