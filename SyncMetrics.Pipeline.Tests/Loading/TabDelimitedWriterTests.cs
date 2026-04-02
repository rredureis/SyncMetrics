using FluentAssertions;
using SyncMetrics.Pipeline.Loading;
using SyncMetrics.Pipeline.Models;
using Xunit;

namespace SyncMetrics.Pipeline.Tests.Loading;

public class TabDelimitedWriterTests
{
    [Fact]
    public void FormatDecimal_Value_FormatsToTwoDecimals()
    {
        TabDelimitedWriter.FormatDecimal(18.5m).Should().Be("18.50");
        TabDelimitedWriter.FormatDecimal(0m).Should().Be("0.00");
        TabDelimitedWriter.FormatDecimal(-3.14159m).Should().Be("-3.14");
    }

    [Fact]
    public void FormatDecimal_Null_ReturnsEmpty()
    {
        TabDelimitedWriter.FormatDecimal(null).Should().BeEmpty();
    }

    [Fact]
    public void Escape_TabsAndNewlines_ReplacedWithSpaces()
    {
        TabDelimitedWriter.Escape("New\tYork").Should().Be("New York");
        TabDelimitedWriter.Escape("Line\nBreak").Should().Be("Line Break");
        TabDelimitedWriter.Escape("Carriage\rReturn").Should().Be("Carriage Return");
    }

    [Fact]
    public void Escape_CleanString_Unchanged()
    {
        TabDelimitedWriter.Escape("New York").Should().Be("New York");
    }

    [Fact]
    public async Task WriteAsync_EmptyRecords_Throws()
    {
        var config = Microsoft.Extensions.Options.Options.Create(
            new Configuration.PipelineConfig
            {
                Output = new Configuration.OutputConfig
                {
                    Directory = Path.Combine(Path.GetTempPath(), "syncmetrics_test"),
                    FilePrefix = "test"
                }
            });

        var writer = new TabDelimitedWriter(config);

        var act = () => writer.WriteAsync([], CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*No records*");
    }

    [Fact]
    public async Task WriteAsync_ValidRecords_ProducesCorrectTsvFormat()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"syncmetrics_test_{Guid.NewGuid():N}");
        var config = Microsoft.Extensions.Options.Options.Create(
            new Configuration.PipelineConfig
            {
                Output = new Configuration.OutputConfig { Directory = tempDir, FilePrefix = "test" }
            });

        var writer = new TabDelimitedWriter(config);
        var records = new List<CanonicalWeatherRecord>
        {
            new()
            {
                LocationName = "New York", Latitude = 40.71, Longitude = -74.01,
                Source = "OpenMeteo", Date = new DateOnly(2026, 3, 28),
                IngestedAtUtc = DateTimeOffset.Parse("2026-03-28T12:00:00Z"),
                TemperatureMaxC = 18.5m, TemperatureMinC = 8.2m,
                PrecipitationMm = 0m, WindSpeedMaxKmh = 25.4m, UvIndexMax = 5.0m
            }
        };

        var path = await writer.WriteAsync(records);

        try
        {
            File.Exists(path).Should().BeTrue();
            var lines = await File.ReadAllLinesAsync(path);
            lines.Should().HaveCount(2); // header + 1 data row

            var header = lines[0].Split('\t');
            header[0].Should().Be("LocationName");
            header[4].Should().Be("Date");

            var data = lines[1].Split('\t');
            data[0].Should().Be("New York");
            data[4].Should().Be("2026-03-28");
            data[6].Should().Be("18.50");
            data[8].Should().Be("0.00");
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }
}