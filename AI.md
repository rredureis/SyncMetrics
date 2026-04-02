# AI Usage

**Tools used:** Claude (architecture discussion, code review), GitHub Copilot (inline completion)

**AI-driven:** Initial project scaffolding — folder structure, `.csproj` files, DI wiring in `Program.cs`, and Polly retry configuration. AI generated the boilerplate efficiently so I could focus on design decisions.

**AI-scaffolded, human-refined:** 

**AI-overridden:** Using strategy pattern with self-selection as it reduces the number of changes needed to implement new sources, splitting configuration classes into multiple files, Claude didn´t use fieldMapping on the source parsing, so i had to implement it myself

**Deliberately no AI:** Class summaries, as these are intended to be human readed