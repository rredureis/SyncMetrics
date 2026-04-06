using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Extraction;
using SyncMetrics.Pipeline.Extraction.Wttr;
using SyncMetrics.Pipeline.Models;
using Xunit;

namespace SyncMetrics.Pipeline.Tests.Extraction;

public class WttrSourceTests
{
    // Two days of data; 8 hourly slots each.
    // Day 1: maxtempC=15, mintempC=5, uvIndex=1
    //   precipMM sum  = 0+0+0+0+0+0+0+0 = 0.0
    //   windspeedKmph max = 7,5,3,6,10,13,14,10 = 14
    // Day 2: maxtempC=18, mintempC=7, uvIndex=1
    //   precipMM sum  = 0+0+0+0+0+0+0+0 = 0.0
    //   windspeedKmph max = 9,9,9,15,17,18,17,12 = 18
    private static readonly string ValidResponse = """
    {
      "weather": [
        {
          "date": "2026-04-06",
          "maxtempC": "15",
          "mintempC": "5",
          "uvIndex": "1",
          "hourly": [
            { "precipMM": "0.0", "windspeedKmph": "7" },
            { "precipMM": "0.0", "windspeedKmph": "5" },
            { "precipMM": "0.0", "windspeedKmph": "3" },
            { "precipMM": "0.0", "windspeedKmph": "6" },
            { "precipMM": "0.0", "windspeedKmph": "10" },
            { "precipMM": "0.0", "windspeedKmph": "13" },
            { "precipMM": "0.0", "windspeedKmph": "14" },
            { "precipMM": "0.0", "windspeedKmph": "10" }
          ]
        },
        {
          "date": "2026-04-07",
          "maxtempC": "18",
          "mintempC": "7",
          "uvIndex": "1",
          "hourly": [
            { "precipMM": "0.0", "windspeedKmph": "9" },
            { "precipMM": "0.0", "windspeedKmph": "9" },
            { "precipMM": "0.0", "windspeedKmph": "9" },
            { "precipMM": "0.0", "windspeedKmph": "15" },
            { "precipMM": "0.0", "windspeedKmph": "17" },
            { "precipMM": "0.0", "windspeedKmph": "18" },
            { "precipMM": "0.0", "windspeedKmph": "17" },
            { "precipMM": "0.0", "windspeedKmph": "12" }
          ]
        }
      ]
    }
    """;

    private static readonly string PrecipResponse = """
    {
      "weather": [
        {
          "date": "2026-04-06",
          "maxtempC": "10",
          "mintempC": "5",
          "uvIndex": "2",
          "hourly": [
            { "precipMM": "1.2", "windspeedKmph": "5" },
            { "precipMM": "0.5", "windspeedKmph": "8" },
            { "precipMM": "0.0", "windspeedKmph": "12" },
            { "precipMM": "2.1", "windspeedKmph": "20" },
            { "precipMM": "0.3", "windspeedKmph": "15" },
            { "precipMM": "0.0", "windspeedKmph": "10" },
            { "precipMM": "0.0", "windspeedKmph": "7" },
            { "precipMM": "0.4", "windspeedKmph": "3" }
          ]
        }
      ]
    }
    """;

    private readonly Location _london = new("London", 51.5074, -0.1278);

    private static IOptions<PipelineConfig> DefaultConfig() => Options.Create(new PipelineConfig
    {
        Sources = new Dictionary<string, SourceConfig>
        {
            ["Wttr"] = new SourceConfig
            {
                BaseUrl = "https://wttr.in",
                FieldMappings =
                [
                    new FieldMapping { Source = "maxtempC",            Target = "TemperatureMaxC", Type = "decimal" },
                    new FieldMapping { Source = "mintempC",            Target = "TemperatureMinC", Type = "decimal" },
                    new FieldMapping { Source = "precipitation_sum_mm",Target = "PrecipitationMm", Type = "decimal" },
                    new FieldMapping { Source = "windspeed_max_kmph",  Target = "WindSpeedMaxKmh", Type = "decimal" },
                    new FieldMapping { Source = "uvIndex",             Target = "UvIndexMax",      Type = "decimal" }
                ]
            }
        }
    });

    private WttrSource CreateSource(string jsonResponse)
    {
        var http = Substitute.For<IHttpClientWrapper>();
        http.GetStringAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(jsonResponse);
        return CreateSource(http);
    }

    private WttrSource CreateSource(IHttpClientWrapper http) =>
        new(http, DefaultConfig(), NullLogger<WttrSource>.Instance);

    [Fact]
    public async Task FetchAsync_ValidResponse_ReturnsCorrectRecordCount()
    {
        var source = CreateSource(ValidResponse);
        var records = await source.FetchAsync(_london);

        records.Should().HaveCount(2);
    }

    [Fact]
    public async Task FetchAsync_ValidResponse_MapsDirectFieldsCorrectly()
    {
        var source = CreateSource(ValidResponse);
        var records = await source.FetchAsync(_london);

        var first = records[0];
        first.LocationName.Should().Be("London");
        first.Source.Should().Be("Wttr");
        first.Date.Should().Be(new DateOnly(2026, 4, 6));
        first.TemperatureMaxC.Should().Be(15m);
        first.TemperatureMinC.Should().Be(5m);
        first.UvIndexMax.Should().Be(1m);
    }

    [Fact]
    public async Task FetchAsync_ValidResponse_AggregatesWindMax()
    {
        var source = CreateSource(ValidResponse);
        var records = await source.FetchAsync(_london);

        // Day 1 max wind across 8 slots: max(7,5,3,6,10,13,14,10) = 14
        records[0].WindSpeedMaxKmh.Should().Be(14m);
        // Day 2 max wind: max(9,9,9,15,17,18,17,12) = 18
        records[1].WindSpeedMaxKmh.Should().Be(18m);
    }

    [Fact]
    public async Task FetchAsync_ValidResponse_SumsPrecipitation()
    {
        var source = CreateSource(PrecipResponse);
        var records = await source.FetchAsync(_london);

        // 1.2 + 0.5 + 0.0 + 2.1 + 0.3 + 0.0 + 0.0 + 0.4 = 4.5
        records[0].PrecipitationMm.Should().Be(4.5m);
    }

    [Fact]
    public async Task FetchAsync_MalformedJson_ThrowsSoCallerRecordsFailure()
    {
        var source = CreateSource("{ not valid json }}}");
        var act = () => source.FetchAsync(_london);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Failed to parse*");
    }

    [Fact]
    public async Task FetchAsync_MissingWeatherArray_ThrowsSoCallerRecordsFailure()
    {
        var source = CreateSource("""{ "nearest_area": [] }""");
        var act = () => source.FetchAsync(_london);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Failed to parse*");
    }

    [Fact]
    public async Task FetchAsync_EmptyWeatherArray_ThrowsSoCallerRecordsFailure()
    {
        var source = CreateSource("""{ "weather": [] }""");
        var act = () => source.FetchAsync(_london);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Failed to parse*");
    }

    [Fact]
    public async Task FetchAsync_HttpFailure_PropagatesException()
    {
        var http = Substitute.For<IHttpClientWrapper>();
        http.GetStringAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("503 Service Unavailable"));

        var source = CreateSource(http);
        var act = () => source.FetchAsync(_london);

        await act.Should().ThrowAsync<HttpRequestException>();
    }

    [Fact]
    public async Task FetchAsync_BuildsCorrectUrl()
    {
        var http = Substitute.For<IHttpClientWrapper>();
        http.GetStringAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ValidResponse);

        var source = CreateSource(http);
        await source.FetchAsync(_london);

        await http.Received(1).GetStringAsync(
            Arg.Is<string>(url =>
                url.Contains("51.5074") &&
                url.Contains("-0.1278") &&
                url.Contains("format=j1")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchAsync_MissingHourlyArray_TreatsAggregatesAsZero()
    {
        var json = """
        {
          "weather": [
            {
              "date": "2026-04-06",
              "maxtempC": "12",
              "mintempC": "4",
              "uvIndex": "3"
            }
          ]
        }
        """;
        var source = CreateSource(json);
        var records = await source.FetchAsync(_london);

        records.Should().HaveCount(1);
        records[0].PrecipitationMm.Should().Be(0m);
        records[0].WindSpeedMaxKmh.Should().Be(0m);
    }
}
