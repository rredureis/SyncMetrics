# SyncMetrics Weather Pipeline

Extensible weather data ingestion pipeline that fetches from multiple APIs, normalizes into a unified schema, and outputs tab-delimited files.

## Quick Start

```bash
dotnet run --project SyncMetrics.Pipeline
dotnet test
```

Output lands in `output/weather_normalized_*.tsv`.

## Architecture

The pipeline follows a hexagonal (ports & adapters) architecture with three clear stages:

- **Extraction** — `IWeatherSource` interface. Each API source is an adapter. Currently: Open-Meteo. Adding a new source = one class implementing `IWeatherSource`, one registration line in DI.
- **Transformation** — Validates records against physical bounds (e.g., temp between -90°C and 60°C). Warns but doesn't discard — downstream analytics decides what to do with flagged records.
- **Loading** — `IOutputWriter` interface. Currently: TSV files. Could swap to CSV, database, cloud storage.

All sources are resolved via the strategy pattern with self-selection (`CanHandle`). DI wires everything. HTTP is behind `IHttpClientWrapper` for testability.

## Design Decisions

**Warn, don't discard.** The transformer flags anomalies (min > max, values outside physical bounds) as warnings but keeps all records. In a BI pipeline, silently dropping data is worse than flagging it — analysts can filter on warnings downstream.

**Nullable weather fields.** API responses have null values (e.g., UV index not available for all dates). The canonical model uses `decimal?` throughout and the TSV writer outputs empty strings for nulls. This is explicit rather than using sentinel values like -999.

**Concurrent extraction, sequential load.** Locations fetch concurrently via `Task.WhenAll`. The output write is sequential because we're writing a single file. If we needed per-location files, the write could parallelize too.

**Polly retry with exponential backoff.** Configured for transient HTTP failures and 429 (rate limiting). Config-driven via `appsettings.json`.

## Configuration

Locations, API settings, field mappings, and retry policy are all in `appsettings.json`. Add a new location by adding a JSON entry — no code changes.

## Adding a New Weather Source

1. Create a folder: `Extraction/NewSource/`
2. Create a folder for the models: `Extraction/NewSource/Models`
2. Add a response model: `NewSourceResponse.cs`
3. Add a source: `NewSource.cs` implementing `IWeatherSource`
4. Register in `Program.cs`: `services.AddTransient<IWeatherSource, NewSource>();`
5. Add config for source in `appsettings.json` under `Sources`

The pipeline discovers it automatically via `CanHandle`.