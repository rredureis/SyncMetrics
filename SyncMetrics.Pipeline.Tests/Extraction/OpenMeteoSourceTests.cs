using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using SyncMetrics.Pipeline.Configuration;
using SyncMetrics.Pipeline.Extraction;
using SyncMetrics.Pipeline.Extraction.OpenMeteo;
using SyncMetrics.Pipeline.Models;
using Xunit;

namespace SyncMetrics.Pipeline.Tests.Extraction;

public class OpenMeteoSourceTests
{
    private static readonly string ValidResponse = """
    {
      "latitude": 40.71,
      "longitude": -74.01,
      "daily": {
        "time": ["2026-03-28", "2026-03-29", "2026-03-30"],
        "temperature_2m_max": [18.5, 20.1, null],
        "temperature_2m_min": [8.2, 10.0, 7.5],
        "precipitation_sum": [0.0, 2.3, 15.1],
        "wind_speed_10m_max": [25.4, 12.1, 30.0],
        "uv_index_max": [5.0, 3.2, null]
      }
    }
    """;

    private readonly Location _nyc = new("New York", 40.7128, -74.0060);

    private static IOptions<PipelineConfig> DefaultConfig() => Options.Create(new PipelineConfig
    {
        Sources = new Dictionary<string, SourceConfig>
        {
            ["OpenMeteo"] = new SourceConfig
            {
                BaseUrl = "https://api.open-meteo.com/v1/forecast",
                DailyFields = "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,uv_index_max",
                ForecastDays = 7,
                FieldMappings =
                [
                    new FieldMapping { Source = "temperature_2m_max", Target = "TemperatureMaxC", Type = "decimal" },
                    new FieldMapping { Source = "temperature_2m_min", Target = "TemperatureMinC", Type = "decimal" },
                    new FieldMapping { Source = "precipitation_sum",  Target = "PrecipitationMm",  Type = "decimal" },
                    new FieldMapping { Source = "wind_speed_10m_max", Target = "WindSpeedMaxKmh",  Type = "decimal" },
                    new FieldMapping { Source = "uv_index_max",       Target = "UvIndexMax",       Type = "decimal" }
                ]
            }
        }
    });

    private OpenMeteoSource CreateSource(string jsonResponse)
    {
        var http = Substitute.For<IHttpClientWrapper>();
        http.GetStringAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(jsonResponse);
        return CreateSource(http);
    }

    private OpenMeteoSource CreateSource(IHttpClientWrapper http) =>
        new(http, DefaultConfig(), NullLogger<OpenMeteoSource>.Instance);

    [Fact]
    public async Task FetchAsync_ValidResponse_ReturnsCorrectRecordCount()
    {
        var source = CreateSource(ValidResponse);
        var records = await source.FetchAsync(_nyc, TestContext.Current.CancellationToken);

        records.Should().HaveCount(3);
    }

    [Fact]
    public async Task FetchAsync_ValidResponse_MapsFieldsCorrectly()
    {
        var source = CreateSource(ValidResponse);
        var records = await source.FetchAsync(_nyc, TestContext.Current.CancellationToken);

        var first = records[0];
        first.LocationName.Should().Be("New York");
        first.Source.Should().Be("OpenMeteo");
        first.Date.Should().Be(new DateOnly(2026, 3, 28));
        first.TemperatureMaxC.Should().Be(18.5m);
        first.TemperatureMinC.Should().Be(8.2m);
        first.PrecipitationMm.Should().Be(0.0m);
        first.WindSpeedMaxKmh.Should().Be(25.4m);
        first.UvIndexMax.Should().Be(5.0m);
    }

    [Fact]
    public async Task FetchAsync_NullValues_PreservedAsNull()
    {
        var source = CreateSource(ValidResponse);
        var records = await source.FetchAsync(_nyc, TestContext.Current.CancellationToken);

        // Third record has null temperature_2m_max and null uv_index_max
        var third = records[2];
        third.TemperatureMaxC.Should().BeNull();
        third.UvIndexMax.Should().BeNull();
        // Other fields should still have values
        third.TemperatureMinC.Should().Be(7.5m);
    }

    [Fact]
    public async Task FetchAsync_MalformedJson_ThrowsSoCallerRecordsFailure()
    {
        var source = CreateSource("{ this is not valid json }}}");
        var act = () => source.FetchAsync(_nyc, TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Failed to parse*");
    }

    [Fact]
    public async Task FetchAsync_MissingDailyData_ThrowsSoCallerRecordsFailure()
    {
        var source = CreateSource("""{ "latitude": 40.71, "longitude": -74.01 }""");
        var act = () => source.FetchAsync(_nyc, TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Failed to parse*");
    }

    [Fact]
    public async Task FetchAsync_EmptyTimeArray_ThrowsSoCallerRecordsFailure()
    {
        var json = """
        {
          "latitude": 40.71,
          "longitude": -74.01,
          "daily": { "time": [] }
        }
        """;
        var source = CreateSource(json);
        var act = () => source.FetchAsync(_nyc, TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Failed to parse*");
    }

    [Fact]
    public async Task FetchAsync_HttpFailure_PropagatesException()
    {
        var http = Substitute.For<IHttpClientWrapper>();
        http.GetStringAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("503 Service Unavailable"));

        var config = Options.Create(new PipelineConfig
        {
            Sources = new Dictionary<string, SourceConfig>
            {
                ["OpenMeteo"] = new SourceConfig
                {
                    BaseUrl = "https://api.open-meteo.com/v1/forecast",
                    DailyFields = "temperature_2m_max",
                    ForecastDays = 7,
                    FieldMappings =
                    [
                        new FieldMapping { Source = "temperature_2m_max", Target = "TemperatureMaxC", Type = "decimal" }
                    ]
                }
            }
        });

        var source = new OpenMeteoSource(http, config, NullLogger<OpenMeteoSource>.Instance);
        var act = () => source.FetchAsync(_nyc, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<HttpRequestException>();
    }

    [Fact]
    public async Task FetchAsync_BuildsCorrectUrl()
    {
        var http = Substitute.For<IHttpClientWrapper>();
        http.GetStringAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ValidResponse);

        var source = CreateSource(http);
        await source.FetchAsync(_nyc, TestContext.Current.CancellationToken);

        await http.Received(1).GetStringAsync(
            Arg.Is<string>(url =>
                url.Contains("latitude=40.7128") &&
                url.Contains("longitude=-74.006") &&
                url.Contains("forecast_days=7") &&
                url.Contains("timezone=auto")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FetchAsync_PartiallyMissingArrays_HandlesGracefully()
    {
        // wind_speed_10m_max array is shorter than time array
        var json = """
        {
          "latitude": 40.71,
          "longitude": -74.01,
          "daily": {
            "time": ["2026-03-28", "2026-03-29"],
            "temperature_2m_max": [18.5, 20.1],
            "temperature_2m_min": [8.2, 10.0],
            "precipitation_sum": [0.0, 2.3],
            "wind_speed_10m_max": [25.4],
            "uv_index_max": null
          }
        }
        """;
        var source = CreateSource(json);
        var records = await source.FetchAsync(_nyc, TestContext.Current.CancellationToken);

        records.Should().HaveCount(2);
        records[0].WindSpeedMaxKmh.Should().Be(25.4m);
        records[1].WindSpeedMaxKmh.Should().BeNull(); // array too short
        records[0].UvIndexMax.Should().BeNull();       // entire array is null
    }
}