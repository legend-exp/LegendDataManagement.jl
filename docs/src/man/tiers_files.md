# Data Tiers and Files

The data of a LEGEND production is stored in one tree of directories per data tier. `l200.tier` gives the paths of these directories and files, and [`search_disk`](@ref) lists what exists on disk. Reading the content of the files is described in [Reading Data](@ref).

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

## Layout

All tier data lies below the path configured for the prefix `tier`. Below it, the directories are named after the tier, category, period and run, and the file names start with the file key of the DAQ cycle:

```
<tier path>/<tier>/<category>/<period>/<run>/<setup>-<period>-<run>-<category>-<timestamp>-tier_<tier>.lh5
```

A tier is a directory name such as `raw`, `dsp`, `hit` or, for the tiers of the Juleana.jl data processing, `jldsp`, `jlhit` or `jlevt`. LDM defines no list of tiers: every directory below the tier path whose name looks like a tier label (three lowercase letters, optionally prefixed by `jl`) is a tier. The tiers of a production are the directories that its processing creates. A prefix `tier/<label>` in the configuration moves one tier to another directory, see [Configuration and LegendData](@ref).

## Paths with `l200.tier`

The property `tier` of a [`LegendData`](@ref) is a [`LegendDataManagement.LegendTierData`](@ref). Indexing it returns a path as a `String`. The path is built from the configuration; it is not checked for existence.

The paths in the following examples are shown relative to the tier directory:

```@example ldm
rel(path) = relpath(path, l200.tier[])
nothing # hide
```

Directories are selected by a tier, a category, a period and a run, each of which may be given as an identifier, a symbol or a string (see [Identifiers and FileKeys](@ref)):

```@example ldm
l200.tier[]  == data_path(l200, "tier")
```

```@example ldm
rel.([l200.tier[:jldsp], l200.tier[:jldsp, :cal], l200.tier[:jldsp, :cal, :p18], l200.tier[:jldsp, :cal, "p18", "r000"]])
```

```@example ldm
l200.tier[DataTier(:jldsp), DataCategory(:cal), DataPeriod(18), DataRun(0)] == l200.tier[:jldsp, :cal, :p18, :r000]
```

The file of a DAQ cycle is selected by a tier and a [`FileKey`](@ref), or the string of a file key. `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`) and `det` a detector ID with data in these files. `mfk` is a file key of the toy production in the test metadata (period 2, run 6, category `cal`).

```@example ldm
rel(l200.tier[:jldsp, fk])
```

```@example ldm
l200.tier[:jldsp, fk] == l200.tier[:jldsp, string(fk)]
```

A string as second index is read as a category if it consists of three lowercase letters, and as a file key otherwise. The path is a file path whether or not the file exists:

```@example ldm
isfile(l200.tier[:jldsp, fk]), isfile(l200.tier[:raw, mfk])
```

### Files of Detectors and Channels

Some tiers store one file per detector or per channel in the run directory. Their names contain the detector or channel in place of the timestamp. They are selected by a detector or a channel after the run, or after a file key:

```@example ldm
rel(l200.tier[:jlhit, :cal, :p18, :r000, det])
```

```@example ldm
rel(l200.tier[:jlhit, fk, det]) == rel(l200.tier[:jlhit, :cal, :p18, :r000, det])
```

```@example ldm
rel(l200.tier[:raw, fk, ChannelId("ch1083204")])
```

The name part of the file is the text of the last index: a [`ChannelId`](@ref) or [`DetectorId`](@ref) prints as `ch1083204` or `V97061A`, a string or symbol is used as written, so it has to be the full channel or detector name.

Period and run numbers must be wrapped in [`DataPeriod`](@ref) and [`DataRun`](@ref) when they are given as integers; `l200.tier[:raw, :cal, 18]` throws a `MethodError`.

### Paths Below the Tier Directory

[`data_path`](@ref) with the tier object resolves path components below the tier directory. This is the way to reach directories that are not part of the layout above:

```@example ldm
rel(data_path(l200.tier, "jldsp", "cal"))
```

`data_path(l200, "tier", ...)` gives the same path, and `data_path(l200, "par", ...)`, `data_path(l200, "metadata", ...)` resolve the other prefixes.

## Searching the Disk

`search_disk(T, path)` lists the entries of the directory `path` whose names convert to the selector type `T`. It returns the sorted, unique values and ignores entries that do not match. The directory must exist.

```@example ldm
search_disk(DataTier, l200.tier[])
```

```@example ldm
search_disk(DataCategory, l200.tier[:jldsp])
```

```@example ldm
search_disk(DataPeriod, l200.tier[:raw, :cal])
```

```@example ldm
search_disk(DataRun, l200.tier[:jldsp, :cal, :p18])
```

For the file keys of a run, `T` is `FileKey`. A file name that starts with a file key converts to it, so one key results per DAQ cycle, also when several tiers or several files per cycle exist:

```@example ldm
search_disk(FileKey, l200.tier[:jldsp, :cal, DataPeriod(18), DataRun(0)])
```

Files whose names carry a detector or channel in place of the timestamp, as the per-detector files of `jlhit`, do not convert to a file key. Searching such a directory returns an empty list; the file keys of a run are listed from a tier with one file per DAQ cycle:

```@example ldm
search_disk(FileKey, l200.tier[:jlhit, :cal, :p18, :r000])
```

### Searching a Data Production

`search_disk(DataSet, data)` collects the file keys on disk for all runs of `runinfo(data)`, as a [`DataSet`](@ref). It needs the metadata and the run information of the production, and it writes the result to the file `filekeys.txt` in the run directories of the tier `jlfks` (keyword `save_tier`), which then serve as a cache. The tier directory must be writable for this.

```julia
using LegendDataManagement

l200 = LegendData(:l200)

search_disk(DataSet, l200)
search_disk(DataSet, l200; search_categories = [:cal], search_tier = :raw, save_filekeys = false)
```

The keywords select the categories (`search_categories`) and the tier (`search_tier`) that are searched, restrict the result to the analysis runs of the metadata (`only_analysis_runs`), and control the cache file (`save_filekeys`, `ignore_save_tier`, `save_tier`). The result for a `LegendData` object is also cached in memory. See [`read_filekeys`](@ref) and [`write_filekeys`](@ref) for the format of `filekeys.txt`.
