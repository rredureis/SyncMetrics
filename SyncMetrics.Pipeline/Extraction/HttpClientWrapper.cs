namespace SyncMetrics.Pipeline.Extraction;

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