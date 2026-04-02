using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using SyncMetrics.Pipeline.Extraction;
using SyncMetrics.Pipeline.Loading;
using SyncMetrics.Pipeline.Models;
using SyncMetrics.Pipeline.Orchestration;
using SyncMetrics.Pipeline.Transformation;
using Xunit;

namespace SyncMetrics.Pipeline.Tests.Pipeline;

public class WeatherPipelineTests
{
    private static readonly Location Nyc = new("New York", 40.7128, -74.0060);
    private static readonly Location London = new("London", 51.5074, -0.1278);

    private static WeatherPipeline CreatePipeline(
        IEnumerable<IWeatherSource> sources, IWeatherTransformer transformer, IOutputWriter writer) =>
        new(sources, transformer, writer, NullLogger<WeatherPipeline>.Instance);

    private static CanonicalWeatherRecord MakeRecord(string location, string date) => new()
    {
        LocationName = location,
        Latitude = 40.71,
        Longitude = -74.01,
        Source = "TestSource",
        Date = DateOnly.Parse(date),
        IngestedAtUtc = DateTimeOffset.UtcNow,
        TemperatureMaxC = 20m,
        TemperatureMinC = 10m,
        PrecipitationMm = 0m,
        WindSpeedMaxKmh = 15m,
        UvIndexMax = 5m
    };

    [Fact]
    public async Task RunAsync_AllLocationsSucceed_ProducesCorrectSummary()
    {
        var source = Substitute.For<IWeatherSource>();
        source.SourceName.Returns("TestSource");
        source.CanHandle("TestSource").Returns(true);
        source.FetchAsync(Nyc, Arg.Any<CancellationToken>())
            .Returns([MakeRecord("New York", "2026-03-28")]);
        source.FetchAsync(London, Arg.Any<CancellationToken>())
            .Returns([MakeRecord("London", "2026-03-28")]);

        var transformer = new WeatherTransformer();

        var writer = Substitute.For<IOutputWriter>();
        writer.WriteAsync(Arg.Any<IReadOnlyList<CanonicalWeatherRecord>>(), Arg.Any<CancellationToken>())
            .Returns("output/test.tsv");

        var pipeline = CreatePipeline([source], transformer, writer);
        var summary = await pipeline.RunAsync([Nyc, London], "TestSource");

        summary.TotalLocations.Should().Be(2);
        summary.SuccessfulLocations.Should().Be(2);
        summary.FailedLocations.Should().Be(0);
        summary.TotalRecords.Should().Be(2);
        summary.OutputFilePath.Should().Be("output/test.tsv");
    }

    [Fact]
    public async Task RunAsync_OneLocationFails_OthersStillProcessed()
    {
        var source = Substitute.For<IWeatherSource>();
        source.SourceName.Returns("TestSource");
        source.CanHandle("TestSource").Returns(true);
        source.FetchAsync(Nyc, Arg.Any<CancellationToken>())
            .Returns([MakeRecord("New York", "2026-03-28")]);
        source.FetchAsync(London, Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("API timeout"));

        var transformer = new WeatherTransformer();
        var writer = Substitute.For<IOutputWriter>();
        writer.WriteAsync(Arg.Any<IReadOnlyList<CanonicalWeatherRecord>>(), Arg.Any<CancellationToken>())
            .Returns("output/test.tsv");

        var pipeline = CreatePipeline([source], transformer, writer);
        var summary = await pipeline.RunAsync([Nyc, London], "TestSource");

        summary.SuccessfulLocations.Should().Be(1);
        summary.FailedLocations.Should().Be(1);
        summary.TotalRecords.Should().Be(1); // NYC still produced records
        summary.Results!.First(r => !r.Success).Error.Should().Contain("API timeout");
    }

    [Fact]
    public async Task RunAsync_UnknownSource_ReturnsAllLocationsFailed()
    {
        var source = Substitute.For<IWeatherSource>();
        source.CanHandle("UnknownApi").Returns(false);

        var pipeline = CreatePipeline([source], new WeatherTransformer(), Substitute.For<IOutputWriter>());

        var summary = await pipeline.RunAsync([Nyc], "UnknownApi");

        summary.FailedLocations.Should().Be(1);
        summary.SuccessfulLocations.Should().Be(0);
        summary.TotalRecords.Should().Be(0);
        summary.AllWarnings.Should().ContainSingle().Which.Should().Contain("UnknownApi");
    }

    [Fact]
    public async Task RunAsync_AllLocationsFail_NoOutputWritten()
    {
        var source = Substitute.For<IWeatherSource>();
        source.SourceName.Returns("TestSource");
        source.CanHandle("TestSource").Returns(true);
        source.FetchAsync(Arg.Any<Location>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("Server down"));

        var writer = Substitute.For<IOutputWriter>();
        var pipeline = CreatePipeline([source], new WeatherTransformer(), writer);

        var summary = await pipeline.RunAsync([Nyc, London], "TestSource");

        summary.FailedLocations.Should().Be(2);
        summary.TotalRecords.Should().Be(0);
        summary.OutputFilePath.Should().BeNull();
        await writer.DidNotReceive()
            .WriteAsync(Arg.Any<IReadOnlyList<CanonicalWeatherRecord>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RunAsync_ConcurrentExecution_AllLocationsProcessed()
    {
        var source = Substitute.For<IWeatherSource>();
        source.SourceName.Returns("TestSource");
        source.CanHandle("TestSource").Returns(true);

        // Simulate varying response times to verify concurrency
        source.FetchAsync(Arg.Any<Location>(), Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                await Task.Delay(50); // simulate network latency
                var loc = callInfo.Arg<Location>();
                return (IReadOnlyList<CanonicalWeatherRecord>)[MakeRecord(loc.Name, "2026-03-28")];
            });

        var writer = Substitute.For<IOutputWriter>();
        writer.WriteAsync(Arg.Any<IReadOnlyList<CanonicalWeatherRecord>>(), Arg.Any<CancellationToken>())
            .Returns("output/test.tsv");

        var pipeline = CreatePipeline([source], new WeatherTransformer(), writer);

        var locations = Enumerable.Range(0, 10)
            .Select(i => new Location($"City{i}", 40 + i, -74 + i))
            .ToList();

        var summary = await pipeline.RunAsync(locations, "TestSource");

        summary.SuccessfulLocations.Should().Be(10);
        summary.TotalRecords.Should().Be(10);
        // 10 locations × 50ms each, but concurrent should be much less than 500ms
        summary.TotalDuration.Should().BeLessThan(TimeSpan.FromSeconds(2));
    }
}
