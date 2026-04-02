using FluentAssertions;
using SyncMetrics.Pipeline.Models;
using SyncMetrics.Pipeline.Transformation;
using Xunit;

namespace SyncMetrics.Pipeline.Tests.Transformation;

public class WeatherTransformerTests
{
    private readonly WeatherTransformer _transformer = new();

    private static CanonicalWeatherRecord MakeRecord(
        decimal? maxC = 20m, decimal? minC = 10m,
        decimal? precip = 0m, decimal? wind = 15m, decimal? uv = 5m,
        string location = "Test", string date = "2026-03-28") => new()
        {
            LocationName = location,
            Latitude = 40.71,
            Longitude = -74.01,
            Source = "Test",
            Date = DateOnly.Parse(date),
            IngestedAtUtc = DateTimeOffset.UtcNow,
            TemperatureMaxC = maxC,
            TemperatureMinC = minC,
            PrecipitationMm = precip,
            WindSpeedMaxKmh = wind,
            UvIndexMax = uv
        };

    [Fact]
    public void Validate_NormalData_NoWarnings()
    {
        var records = new[] { MakeRecord() };
        var (valid, warnings) = _transformer.Validate(records);

        valid.Should().HaveCount(1);
        warnings.Should().BeEmpty();
    }

    [Fact]
    public void Validate_MinGreaterThanMax_WarnsButKeepsRecord()
    {
        var records = new[] { MakeRecord(maxC: 10m, minC: 25m) };
        var (valid, warnings) = _transformer.Validate(records);

        valid.Should().HaveCount(1);
        warnings.Should().ContainSingle().Which.Should().Contain("min temp");
    }

    [Fact]
    public void Validate_ExtremeTemperature_WarnsOutOfRange()
    {
        var records = new[] { MakeRecord(maxC: 70m) };
        var (valid, warnings) = _transformer.Validate(records);

        valid.Should().HaveCount(1);
        warnings.Should().ContainSingle().Which.Should().Contain("outside expected range");
    }

    [Fact]
    public void Validate_NegativePrecipitation_Warns()
    {
        var records = new[] { MakeRecord(precip: -5m) };
        var (_, warnings) = _transformer.Validate(records);

        warnings.Should().ContainSingle().Which.Should().Contain("precipitation");
    }

    [Fact]
    public void Validate_AllNullFields_WarnsAllNull()
    {
        var records = new[] { MakeRecord(maxC: null, minC: null, precip: null, wind: null, uv: null) };
        var (valid, warnings) = _transformer.Validate(records);

        valid.Should().HaveCount(1);
        warnings.Should().ContainSingle().Which.Should().Contain("all weather fields are null");
    }

    [Fact]
    public void Validate_MultipleRecords_AggregatesWarnings()
    {
        var records = new[]
        {
            MakeRecord(maxC: 70m),
            MakeRecord(precip: -1m),
            MakeRecord()
        };
        var (valid, warnings) = _transformer.Validate(records);

        valid.Should().HaveCount(3);
        warnings.Should().HaveCount(2);
    }

    [Fact]
    public void Validate_EmptyList_ReturnsEmpty()
    {
        var (valid, warnings) = _transformer.Validate([]);

        valid.Should().BeEmpty();
        warnings.Should().BeEmpty();
    }
}