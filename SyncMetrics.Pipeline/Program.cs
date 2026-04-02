using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Extensions.Http;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Extraction;
using SyncMetrics.Pipeline.Extraction.OpenMeteo;
using SyncMetrics.Pipeline.Loading;
using SyncMetrics.Pipeline.Models;
using SyncMetrics.Pipeline.Orchestration;
using SyncMetrics.Pipeline.Transformation;

var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
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
services.AddHttpClient<IHttpClientWrapper, HttpClientWrapper>("OpenMeteo")
    .AddPolicyHandler(GetRetryPolicy(config));

// Extraction — register sources (strategy pattern with self-selection)
services.AddTransient<IWeatherSource, OpenMeteoSource>();
// Future: services.AddTransient<IWeatherSource, WeatherApiSource>();

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

// Run pipeline
var pipeline = provider.GetRequiredService<WeatherPipeline>();
var summary = await pipeline.RunAsync(locations, "OpenMeteo");
summary.PrintToConsole();

return summary.FailedLocations > 0 ? 1 : 0;

// --- Polly retry policy ---
static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(IConfiguration config)
{
    var retryConfig = config.GetSection("Pipeline:Retry");
    var maxRetries = retryConfig.GetValue("MaxRetries", 3);
    var baseDelay = retryConfig.GetValue("BaseDelaySeconds", 2);

    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .OrResult(r => r.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
        .WaitAndRetryAsync(
            maxRetries,
            attempt => TimeSpan.FromSeconds(Math.Pow(baseDelay, attempt)),
            onRetry: (outcome, delay, attempt, _) =>
            {
                Console.WriteLine($"  [Retry] Attempt {attempt} after {delay.TotalSeconds:F1}s " +
                                  $"— {outcome.Exception?.Message ?? outcome.Result?.StatusCode.ToString()}");
            });
}