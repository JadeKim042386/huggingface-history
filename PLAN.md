# Plan: keeping the history of records that get reset

## Goal

Show the download history of Hugging Face datasets as charts, the way star-history does for GitHub stars.
Beyond downloads, let the user choose any record that is reset or disappears and keep its history.
The records we collect must remain even if Hugging Face resets its own.

## Verified facts (2026-11-21)

- The Hugging Face API returns current values only: `downloads` (last 30 days) and `downloadsAllTime`. There is no per-day series.
- The public dataset `cfahlgren1/hub-stats` uploads hub-wide stats daily. `datasets.parquet` has 793 commits, starting 2024-07-29.
- The `downloadsAllTime` column exists from the 2025-02-27 snapshot. Earlier snapshots have only 30-day downloads and likes.
- `_id` survives a rename. Calling the API with an old name redirects to the new one.
- Older snapshots have a single row group, so each file needs a full read of the `_id` column: about 1.7 s per file with 8 threads.
- For `rajpurkar/squad`, 30-day downloads jump from 8,184 to 55,531 between 2024-10-21 and 10-22. This looks like a change in how downloads are counted.
- For the same dataset, total downloads never decreased across 547 observations. Likes dropped from 1,247 to 874 on 2026-10-19.
- Back-estimating totals from 30-day downloads: the 30-day increase of the total divided by 30-day downloads has median 1.02, min 0.78, max 1.23; 94% of 180 pairs fall within 0.9 to 1.1.
- `raw.githubusercontent.com` serves the chart SVGs as `image/svg+xml`, so they can be embedded in pages outside GitHub.

## Design decisions

| Decision | Reason |
|---|---|
| Append-only ledger `data/ledger.csv` (`observed_at, entity_id, target, metric, value, origin`) | Existing lines are never edited, so past observations survive a reset. Only rows identical in every field are skipped. |
| Targets are identified by `entity_id` (the Hugging Face `_id`) | Tells a rename apart from a delete-and-recreate. |
| Times are UTC | Local runs and server runs agree on the date. |
| Failed collection is never recorded | A 0 or blank in the ledger could not be taken back. |
| A decreasing value is not auto-corrected | A retroactive adjustment and a real reset cannot be told apart from the value alone. Raw values are kept as they are. |
| Targets can be `<kind>/<owner>/*` | The owner's repositories are looked up on every run, so new ones are tracked automatically. A deleted one stops being collected; its records and chart line remain. |
| Metrics are listed per kind | Spaces have no download metrics. |
| Charts are static SVG | They embed in READMEs and Hugging Face cards and follow light/dark mode. |
| The default is one `summary` chart | Five charts in mixed forms and styles were hard to read. One panel answers the three questions a visitor has: how much (the total), is it growing (30-day change and the stacked history), and which repositories matter (the table, largest first). |
| The stack's top edge is the header total | The big number and the picture say the same thing. The five largest targets get a layer; the rest fold into "Other". |
| 30-day change needs an observation within 3 days of 30 days ago | Otherwise a gap in collection, or a repository tracked late, would report older growth as recent. Such rows show a dash. |
| Other forms and styles stay available in `[[charts]]` | Forms: summary, line, area, bars. Styles: auto, light, dark, sketch. |
| Monthly bars interpolate the value at month boundaries | A missing month-end snapshot does not distort month length. Incomplete months are not drawn. |
| Past snapshots are never looked up by name | A different repository recreated under the same name would be mixed in. |
| A failed backfill does not fail the run | If the snapshot source is gone or a target is not in it, history simply starts on the day collection begins. |
| `data/ledger.csv` uses the union merge driver | Lines appended on both sides of a merge are all kept. |

## Phases

| Phase | Content | Status |
|---|---|---|
| 0 | Fact checks: column introduction date, query speed, back-estimation error | Done |
| 1 | Live collection, ledger, target and metric selection (`targets.toml`) | Done |
| 2 | Backfill from hub-stats, SVG charts, README chart and embed blocks | Done (verified for datasets only) |
| 3 | Daily collection with GitHub Actions; public template repository | Done |
| 4 | Verify backfill for models and spaces; more metrics; markers for sharp drops, renames, and `_id` changes | Pending |
| 5 | Static web page that reads the ledger (comparison, hover) | Pending |
| 6 | Sources outside Hugging Face (GitHub traffic and so on) as separate scripts writing the same ledger format | When needed |

## Out of scope

- A web page where any id gives an instant chart: backfilling one target takes up to 20 minutes, so only tracked targets are shown.
- Back-estimating totals before 2025-02: the error is mostly within 10%, but counting changed on 2024-10-22, which leaves only four usable months.
- Matching the Hugging Face site theme toggle: `auto` charts follow the operating system setting, not the site toggle.

## Known limits

- Line, area and bar charts draw at most 8 targets. The summary counts all of them, but past 8 its colors repeat.
- Every chart draws all tracked targets. There is no per-chart target selection yet.
- A day when a target was absent from a snapshot is queried again on every run.

## Reviews with Antigravity

- Round 1 (plan): 7 points, 6 adopted. Dropped automatic offset correction, keyed targets by `_id`, UTC times, per-kind metrics, live collection first, no instant-lookup web page.
- Round 2 (code): 8 bugs, all fixed. Among them: observations lost when a value returned the same day, the card comparing against 37 days ago while saying 30, and distorted month lengths in the bars.
- Round 3 (workflow, wildcards, embed): 6 points, 4 adopted (empty listings fail the run, paginated listings, rename and case matching, workflow step conditions and `git add`). Not adopted: color slots fixed across the whole ledger (colors shift only on a deliberate config change), and full-history clones in the workflow (a failed push loses one day of live values, which the next run backfills).
- Round 4 (simplification): agreed on one panel; adopted the stacked area, the sorted table, and dropping the owner prefix.
- Round 5 (summary code): 7 points, all fixed. Among them: targets past the 8th missing from the total, 30-day change inflated by gaps and late-tracked repositories, the layer stroke hiding thin layers and the axis, and an empty chart on the first day of collection.
- Two claims from the reviews were not verified independently: a `huggingface_hub` issue number about decreasing totals, and a counting change in September 2024 (the data shows 2024-10-22).
