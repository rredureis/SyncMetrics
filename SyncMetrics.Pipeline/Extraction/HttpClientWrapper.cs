namespace SyncMetrics.Pipeline.Extraction;

/// <summary>
/// Provides a wrapper around an HTTP client for sending HTTP requests and receiving responses as strings.
/// </summary>
public sealed class HttpClientWrapper : IHttpClientWrapper
{
    private readonly HttpClient _client;

    public HttpClientWrapper(HttpClient client) => _client = client;

    public async Task<string> GetStringAsync(string url, CancellationToken ct = default)
    {
        var response = await _client.GetAsync(url, ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(ct);
    }
}