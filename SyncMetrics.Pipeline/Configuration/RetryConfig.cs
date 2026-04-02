namespace SyncMetrics.Pipeline.Configuration;

public sealed class RetryConfig
{
    public int MaxRetries { get; set; } = 3;
    public int BaseDelaySeconds { get; set; } = 2;
}