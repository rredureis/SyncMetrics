using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Extensions.Http;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Extraction;
using SyncMetrics.Pipeline.Extraction.OpenMeteo;
using SyncMetrics.Pipeline.Extraction.Wttr;
using SyncMetrics.Pipeline.Loading;
using SyncMetrics.Pipeline.Models;
using SyncMetrics.Pipeline.Orchestration;
using SyncMetrics.Pipeline.Transformation;

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddJsonFile("appsettings.Development.json", optional: true)
    .Build();

var services = new ServiceCollection();

// Logging
services.AddLogging(builder =>
{
    builder.AddConsole();
});

// Bind configuration
services.Configure<PipelineConfig>(config.GetSection("Pipeline"));

// HTTP client with Polly retry policy (bonus requirement)
var retryConfig = config.GetSection("Pipeline:Retry").Get<RetryConfig>() ?? new RetryConfig();
services.AddHttpClient<IHttpClientWrapper, HttpClientWrapper>()
    .AddPolicyHandler(GetRetryPolicy(retryConfig));

// Extraction — register sources (strategy pattern with self-selection)
services.AddTransient<IWeatherSource, OpenMeteoSource>();
services.AddTransient<IWeatherSource, WttrSource>();

// Transformation
services.AddTransient<IWeatherTransformer, WeatherTransformer>();

// Loading
services.AddTransient<IOutputWriter, TabDelimitedWriter>();

// Pipeline orchestrator
services.AddTransient<WeatherPipeline>();

var provider = services.BuildServiceProvider();

// Resolve config and map locations
var pipelineConfig = provider.GetRequiredService<IOptions<PipelineConfig>>().Value;
var locations = pipelineConfig.Locations
    .Select(l => new Location(l.Name, l.Latitude, l.Longitude))
    .ToList();

Console.WriteLine($"SyncMetrics Weather Pipeline — fetching {locations.Count} locations...");
Console.WriteLine();

// Run pipeline for all registered sources
var pipeline = provider.GetRequiredService<WeatherPipeline>();
var sources = provider.GetRequiredService<IEnumerable<IWeatherSource>>();

var allFailed = 0;
foreach (var source in sources)
{
    Console.WriteLine($"--- Running pipeline for source: {source.SourceName} ---");
    Console.WriteLine();

    var summary = await pipeline.RunAsync(locations, source.SourceName);
    summary.PrintToConsole();

    allFailed += summary.FailedLocations;
    Console.WriteLine();
}

return allFailed > 0 ? 1 : 0;

// --- Polly retry policy ---
static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(RetryConfig retry)
{
    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .OrResult(r => r.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
        .WaitAndRetryAsync(
            retry.MaxRetries,
            attempt => TimeSpan.FromSeconds(Math.Pow(retry.BaseDelaySeconds, attempt)),
            onRetry: (outcome, delay, attempt, _) =>
            {
                Console.WriteLine($"  [Retry] Attempt {attempt} after {delay.TotalSeconds:F1}s " +
                                  $"— {outcome.Exception?.Message ?? outcome.Result?.StatusCode.ToString()}");
            });
}