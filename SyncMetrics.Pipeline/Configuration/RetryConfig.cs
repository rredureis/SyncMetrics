namespace SyncMetrics.Pipeline.Configuration;

/// <summary>
/// Represents the configuration settings for retrying failed operations.
/// </summary>
public sealed class RetryConfig
{
    public int MaxRetries { get; set; } = 3;
    public int BaseDelaySeconds { get; set; } = 2;
}