# AI Usage

**Tools used:** Claude (architecture discussion, code review), Claude Code (scaffolding and implementations), GitHub Copilot (inline completion)

**AI-driven:** Initial project scaffolding — folder structure, `.csproj` files, DI wiring in `Program.cs`, Polly retry configuration, `CanonicalWeatherRecord`, `IOutputWriter`, `TabDelimitedWriter`, `OpenMeteoDailyData` JSON model, and the first pass of unit tests.

**AI-scaffolded, human-refined:** `BaseExtractionSource` went through two structural corrections: AI's first version hardcoded `List<decimal?>` in `GetSourceArray`, ignoring `FieldMapping.Type` entirely — fixed to use `IList` with type-driven conversion. The second version still reflected over `TSourceData` in the base class, which broke for array-of-objects JSON shapes; replaced with the `Normalize` abstraction and `RawDailyRecord` so the base class has no knowledge of source shape.

**AI-overridden:** Strategy pattern with `CanHandle` self-selection — AI suggested a factory/registry; self-selection was chosen because adding a source touches one class and one DI line, not three places. AI also didn't apply `FieldMappings` to the parsing step initially; that required explicit correction to make the config-driven mapping actually work.

**Deliberately no AI:** The `ConvertValue` two-step (parse as source type via `FieldMapping.Type`, then convert to target type) and the use of `InvariantCulture` throughout normalization — AI used `Convert.ToDecimal` which is culture-sensitive and would produce wrong results on non-English systems. This was caught and corrected manually. Class XML summaries were also written by hand.
