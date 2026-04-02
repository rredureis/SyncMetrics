# SyncMetrics Weather Pipeline

Extensible weather data ingestion pipeline that fetches from multiple APIs, normalizes into a unified schema, and outputs tab-delimited files.

## Quick Start

```bash
dotnet run --project SyncMetrics.Pipeline
dotnet test
```

Output lands in `output/weather_normalized_*.tsv`.

## Architecture

The pipeline follows a hexagonal (ports & adapters) architecture with four clear stages:

- **Extraction** — `IWeatherSource` interface; each API is an adapter extending `BaseExtractionSource<TSourceData, TCanonicalRecord>`. HTTP calls are behind `IHttpClientWrapper`.
- **Normalization** — each source implements `Normalize(TSourceData)` to convert its specific JSON shape into `IReadOnlyList<RawDailyRecord>` — one record per day, fields as strings keyed by source field name. The base class then applies `FieldMappings` config to produce canonical records, with no knowledge of the original shape.
- **Transformation** — `IWeatherTransformer` validates records against physical bounds (e.g., temp between -90°C and 60°C). Warns but doesn't discard — downstream analytics decides what to do with flagged records.
- **Loading** — `IOutputWriter` interface. Currently: TSV files. Could swap to CSV, database, cloud storage.

Sources are resolved via the strategy pattern with self-selection (`CanHandle`). DI wires everything.

## Design Decisions

**Normalize before mapping.** Each source's `Normalize` method is the only place that knows the API's JSON shape. The base class only sees `RawDailyRecord` — a flat `(date, fields)` structure. This means a columnar API (Open-Meteo) and an array-of-objects API can both be supported without any changes to the shared mapping logic.

**Warn, don't discard.** The transformer flags anomalies (min > max, values outside physical bounds) as warnings but keeps all records. In a BI pipeline, silently dropping data is worse than flagging it — analysts can filter on warnings downstream.

**Nullable weather fields.** API responses have null values (e.g., UV index not available for all dates). The canonical model uses `decimal?` throughout and the TSV writer outputs empty strings for nulls — explicit rather than sentinel values like -999.

**Concurrent extraction, sequential load.** Locations fetch concurrently via `Task.WhenAll`. The output write is sequential because we're writing a single file.

**Polly retry with exponential backoff.** Configured for transient HTTP failures and 429 (rate limiting). Config-driven via `appsettings.json`.

**Parse failures are location failures.** `ParseResponse` logs the detailed error; `FetchAsync` throws so `WeatherPipeline` records the location as failed in the summary. No silent successes with 0 records.

## Configuration

Locations, API settings, field mappings, and retry policy are all in `appsettings.json`. Add a new location by adding a JSON entry — no code changes.

## Adding a New Weather Source

1. Create `Extraction/NewSource/Models/NewSourceResponse.cs` — deserialize the raw JSON
2. Create `Extraction/NewSource/NewSource.cs` extending `BaseExtractionSource<NewSourceResponse, CanonicalWeatherRecord>` and implementing `IWeatherSource`
3. Implement `Normalize(NewSourceResponse)` — convert the source's JSON shape to `IReadOnlyList<RawDailyRecord>`
4. Implement `FetchAsync` — HTTP call + `ParseResponse` + `MapToCanonical`
5. Register in `Program.cs`: `services.AddTransient<IWeatherSource, NewSource>()`
6. Add source config and `FieldMappings` in `appsettings.json` under `Sources`

The pipeline discovers it automatically via `CanHandle`. No changes to the base class, pipeline, or any existing source.
