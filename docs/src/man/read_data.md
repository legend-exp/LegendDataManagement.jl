# Reading Data

[`read_ldata`](@ref) reads the tables stored in `lh5` tier files of a [`LegendData`](@ref) object. A selection of tier, DAQ cycle, run, partition or time and of a detector determines the files and rows that are read; columns can be selected or computed while reading.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
using TypedTables, Unitful, YAML

# A second LegendData object whose tier directory holds jldsp tables for every
# DAQ cycle of the metadata-only production (period 2, run 6).
toy_config = joinpath(scratch_dir, "toy_config.yaml")
YAML.write_file(toy_config, Dict("setups" => Dict("l200" => Dict("paths" => Dict(
    "tier" => joinpath(scratch_dir, "toy_tier"),
    "metadata" => data_path(l200, "metadata"),
    "par" => data_path(l200, "par"),
)))))
l200_p02 = withenv(() -> LegendData(:l200), "LEGEND_DATA_CONFIG" => toy_config)
for (i, key) in enumerate(only(runinfo(l200_p02, (DataPeriod(2), DataRun(6), :cal)).keys))
    path = l200_p02.tier[:jldsp, key]
    mkpath(dirname(path))
    lh5open(path, "w") do f
        f["jldsp/V99000A"] = Table(
            timestamp = (Timestamp(key).unixtime .+ (1:3)) .* 1.0u"s",
            e_cusp = 100.0 .* (1:3) .+ i,
        )
    end
end
```

## Requirements and File Layout

The methods that read files are provided by a package extension that is loaded together with LegendHDF5IO.jl; see [Extensions](@ref). Without `using LegendHDF5IO`, [`read_ldata`](@ref) throws an `ErrorException`.

The extension reads the layout written by the Julia dataflow: a table with the name `<tier>/<detector>` inside the file `l200.tier[tier, filekey]`, or, if present, a file `l200.tier[tier, filekey, detector]` that holds the tables of one detector. The tiers of the Julia dataflow (`jldsp`, `jlhit`, `jlevt`, ...) are stored this way; see [Data Tiers and Files](@ref) for the file names.

The test data contain `jldsp` files of two detectors for one DAQ cycle. `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`) and `det` a detector ID with data in these files.

```@example ldm
using TypedTables, PropertyFunctions

fk
```

## Reading a DAQ Cycle

The selectors follow the `LegendData` object and the optional function or column selection that precedes it. Tier, category, period and run can be given as symbols; they are converted by [`DataTier`](@ref), [`DataCategory`](@ref), [`DataPeriod`](@ref) and [`DataRun`](@ref). With a tier, a [`FileKey`](@ref) and a [`DetectorId`](@ref), the result is a `TypedTables.Table`.

```@example ldm
dsp = read_ldata(l200, :jldsp, fk, det)
size(dsp), first(propertynames(dsp), 6)
```

Without a detector, all channels of the tier are read and returned in a `NamedTuple` keyed by detector. A vector of detectors reads those channels only, in the order given.

```@example ldm
all_dets = read_ldata(l200, :jldsp, fk)
keys(all_dets)
```

```@example ldm
keys(read_ldata(l200, :jldsp, fk, [det, DetectorId(:B93099C)]))
```

A table that is missing in the file throws an `ArgumentError`. With `ignore_missing = true`, such a detector is skipped instead and does not appear in the result.

```@example ldm
keys(read_ldata(l200, :jldsp, fk, [det, DetectorId(:V00000A)]; ignore_missing = true))
```

## Selecting Columns

A tuple of symbols reads only these columns and returns a table; a single symbol does the same for one column.

```@example ldm
read_ldata((:e_cusp, :timestamp), l200, :jldsp, fk, det)[1:2]
```

```@example ldm
read_ldata(:e_cusp, l200, :jldsp, fk, det)[1:2]
```

A `PropertyFunctions.PropertyFunction` is evaluated for every row; only the columns it uses are read. The function is written with the `@pf` macro of PropertyFunctions.jl, where `$name` denotes a column, or compiled from a string expression by [`ljl_propfunc`](@ref) (see [Property Functions and Expressions](@ref)).

```@example ldm
read_ldata(@pf($e_cusp * 2), l200, :jldsp, fk, det)[1:2]
```

```@example ldm
read_ldata(ljl_propfunc("e_cusp > 10000.0"), l200, :jldsp, fk, det)
```

A `PropSelFunction` selects columns and can rename them (`PropSelFunction(:e_cusp => :energy)`); a tuple of symbols is the short form without renaming. Any other function is applied to each row:

```@example ldm
read_ldata(row -> (; energy = row.e_cusp, time = row.timestamp), l200, :jldsp, fk, det)[1:2]
```

## Selecting Rows

The keyword `filterby` takes a property function that is evaluated on the rows of the tier that is read; only the rows for which it is `true` are kept.

```@example ldm
length(read_ldata(l200, :jldsp, fk, det; filterby = @pf($e_cusp > 10000)))
```

A pair `tier => property function` evaluates the function on another tier and applies the result to the tier that is read. Both tiers must hold one row per trigger, so that their rows correspond by position; a mismatch in the number of rows throws a `DimensionMismatch`.

```julia
raw = read_ldata(l200, :raw, filekey, det; filterby = :jlhit => @pf($is_valid_hit))
```

With a vector of timestamps (`Unitful.Time`) after the `FileKey`, only the rows with these times are read, in the order of the timestamps. The time of a row is its `timestamp` column, or `tstart` in an event tier. A timestamp without a row throws an `ArgumentError`, or is skipped with `ignore_missing = true`.

```@example ldm
ts = dsp.timestamp[[2, 5]]
read_ldata(:e_cusp, l200, :jldsp, fk, ts, det)
```

Without a `FileKey`, each timestamp is assigned to the DAQ cycle that contains it ([`find_filekey`](@ref)), which requires the run information of the metadata, and only the rows of that cycle with these times are read.

```julia
using Unitful

dsp = read_ldata(l200, :jldsp, [1.6e9u"s", 1.7e9u"s"], det)
```

## Reading Runs, Periods and Partitions

A category followed by a period and a run reads all DAQ cycles of the run and concatenates the rows in one table. The file keys are those that the metadata list for the run (`only(runinfo(l200, (period, run, category)).keys)`; see [Channel, Run and Partition Information](@ref)); a listed file that is not on disk is an error. These forms are available:

* `(tier, category, period, run[, det])`: one run.
* `(tier, category, period[, det])`: all runs of a period, from `runinfo(l200, period)`.
* `(tier, category, partition, det)`: all runs of the partition of a detector, from `partitioninfo(l200, det, partition)`.
* `(tier, category, runtable[, det])`: the runs listed in a table with the columns `period` and `run`, such as a row selection of `runinfo` or of a partition table.
* `(tier, filekeys[, det])`: a vector of file keys.

If the file `l200.tier[tier, category, period, run, det]` exists, it holds the whole run of the detector and is read in place of the DAQ cycles. The keyword `ignore_keys` (default `:all`) drops the DAQ cycles that [`bad_filekeys`](@ref) lists for that key in the metadata, with `:none` keeping all of them. With `search_disk = true`, the file keys found on disk by [`search_disk`](@ref) are read instead of those from the metadata.

The following examples use a second `LegendData` object, `l200_p02`, whose tier directory holds `jldsp` tables for all DAQ cycles of period 2, run 6, listed by `runinfo`.

```@example ldm
run_keys = only(runinfo(l200_p02, (DataPeriod(2), DataRun(6), :cal)).keys)
length(run_keys)
```

The run is selected by category, period and run.

```@example ldm
run_table = read_ldata(l200_p02, :jldsp, :cal, :p02, :r006, :V99000A)
length(run_table), first(run_table.e_cusp, 4)
```

A run table selects the same runs. The table of `runinfo` has the columns `period` and `run`.

```@example ldm
runs = filter(r -> r.run == DataRun(6), runinfo(l200_p02, DataPeriod(2)))
length(read_ldata(l200_p02, :jldsp, :cal, runs, :V99000A))
```

A vector of file keys reads those DAQ cycles only.

```@example ldm
length(read_ldata(l200_p02, :jldsp, run_keys[1:2], :V99000A))
```

The partition and period forms work in the same way, provided that all runs have files:

```julia
dsp = read_ldata(l200, :jldsp, :cal, DataPartition(:calgroup001a), det)
dsp = read_ldata(l200, :jldsp, :cal, DataPeriod(3), det)
```

## Parallel Reading

For several DAQ cycles, runs or timestamp groups, `parallel = true` reads the files on the workers of the worker pool `wpool` (default `Distributed.default_worker_pool()`). The workers load LegendDataManagement and LegendHDF5IO before the first read.

```julia
dsp = read_ldata(l200, :jldsp, :cal, :p03, :r000, det; parallel = true)
```

## Files in the Python Layout

The `raw`, `dsp` and `hit` files written by the LEGEND Python software store each channel as a top-level group named `ch<channel id>`, with the table inside (`ch1084803/hit`). [`read_ldata`](@ref) looks for `<tier>/<detector>` and therefore does not read these files; it throws an `ArgumentError`.

```@example ldm
py_key = first(search_disk(FileKey, l200.tier[:raw, :cal, DataPeriod(3), DataRun(1)]))

try
    read_ldata(l200, :hit, py_key)
catch err
    println(sprint(showerror, err))
end
```

Such files are opened directly with `lh5open`, which lists the channels and reads a table into memory:

```@example ldm
path = l200.tier[:hit, py_key]
lh5open(f -> first(keys(f), 3), path)
```

```julia
hit = lh5open(f -> f["ch1084803/hit"][:], path)
```

Tables written with `f["<tier>/<detector>"] = table` inside `lh5open(path, "w")` are read back by [`read_ldata`](@ref).
