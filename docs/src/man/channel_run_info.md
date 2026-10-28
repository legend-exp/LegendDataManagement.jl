# Channel, Run and Partition Information

The run information lists the runs of a data production with their start keys and livetimes, the channel information gives the status and hardware properties of each channel in a run or period, and the partition information groups runs across periods per detector. All three are `Table`s of [TypedTables.jl](https://github.com/JuliaData/TypedTables.jl) built from the metadata (see [Metadata and Validity](@ref)).

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

```@example ldm
using TypedTables, PropertyFunctions, Unitful
```

Runs are selected by tuples of identifiers. `(period, run)` is a [`LegendDataManagement.RunSelLike`](@ref), `(period, category)` a [`LegendDataManagement.PeriodSelLike`](@ref) and `(period, run, category)` a [`LegendDataManagement.RunCategorySelLike`](@ref); the elements can be identifier objects such as `DataPeriod(2)`, symbols such as `:p02` or strings (see [Identifiers and FileKeys](@ref)).

## Run Information

[`runinfo`](@ref) returns one row per run of `data.dataset` (all runs of the metadata for the default dataset, see [`LegendData`](@ref)). The columns are `period`, `run`, `keys` (a [`DataSet`](@ref) with the DAQ cycle keys of all categories of the run) and one column per data category found in the metadata.

```@example ldm
rinfo = runinfo(l200, :p02)
(propertynames(rinfo), length(rinfo))
```

```@example ldm
rinfo.run
```

Each category column is itself a table with the columns `startkey`, `endkey`, `livetime`, `is_analysis_run` and `keys`. A run without an entry for a category has `missing` keys, a livetime of `NaN` seconds and `is_analysis_run = false` there.

```@example ldm
rinfo.phy.startkey[1:2]
```

```@example ldm
rinfo.phy.livetime[1:2]
```

`runinfo(data, period)` returns the rows of a period. A run `(period, run)` returns the one-row table of that run, and a run with a category or a [`FileKey`](@ref) returns the one-row table of that category.

```@example ldm
runinfo(l200, (:p02, :r006, :phy)).startkey
```

The following examples use a file key of the toy production in the test metadata: `mfk` is a file key of the toy production in the test metadata (period 2, run 6, category `cal`).

```@example ldm
runinfo(l200, mfk).is_analysis_run
```

Rows are filtered with `PropertyFunctions.filterby` and `@pf` (see [Property Functions and Expressions](@ref)). This selects the calibration runs of a period that are analysis runs:

```@example ldm
rinfo_cal = runinfo(l200, :p02) |> filterby(@pf $cal.is_analysis_run)
first(rinfo_cal).cal.startkey
```

### Start Keys, Livetime and Analysis Runs

[`start_filekey`](@ref) gives the first file key of a run and category, or `missing` if the metadata has none. [`livetime`](@ref) gives the livetime in seconds as a `Unitful` quantity, `NaN` seconds if the metadata has none.

```@example ldm
start_filekey(l200, (:p02, :r006, :cal))
```

```@example ldm
start_filekey(l200, mfk)
```

```@example ldm
livetime(l200, (:p02, :r006, :phy))
```

```@example ldm
ustrip(u"s", livetime(l200, (:p02, :r006, :cal)))
```

The exposure is calculated from the livetime (see [Exposure and Livetime](@ref)).

[`is_lrun`](@ref) tells whether a run exists in the metadata with a start key, and [`is_analysis_run`](@ref) whether it is an analysis run of its category. [`analysis_runs`](@ref) lists all analysis runs of a category. The analysis runs are those listed in the metadata `datasets/runlists` for `data.dataset`, with the run list `valid` for the default dataset.

```@example ldm
is_lrun(l200, (:p02, :r006, :cal)), is_lrun(l200, (:p02, :r099, :cal))
```

```@example ldm
is_analysis_run(l200, (:p02, :r006, :phy))
```

```@example ldm
is_analysis_run(l200, mfk)
```

```@example ldm
analysis_runs(l200, :phy).run[1:3]
```

### Ignored DAQ Cycles

[`bad_filekeys`](@ref) returns the set of file keys that the metadata `datasets/ignored_daq_cycles` lists, e.g. cycles without usable data. `load_key` selects one of the lists of that file; the default `:all` merges them.

```@example ldm
length(bad_filekeys(l200))
```

```@example ldm
bad_filekeys(l200; load_key = :unprocessable) ⊆ bad_filekeys(l200)
```

## Channel Information

[`channelinfo`](@ref) returns a table with one row per channel. The selection determines the time and category whose metadata is used. These forms are equivalent for the run of `mfk`:

```@example ldm
chinfo = channelinfo(l200, mfk)
chinfo == channelinfo(l200, ValiditySelection(mfk)) == channelinfo(l200, (:p02, :r006, :cal)) == channelinfo(l200, :p02, :r006, :cal)
```

The selection can be

* a [`FileKey`](@ref) or [`LegendDataManagement.ValiditySelection`](@ref),
* a run `(period, run, category)`, or `(period, run)` for the category `:cal`,
* a period with a category `(period, category)`, or a period alone for the category `:cal`.

The selections can also be passed as separate arguments, like `channelinfo(l200, :p02, :r006, :cal)`.

For a period, the information of all runs of the category is merged into one table. The status columns `processable`, `usability`, `is_blinded`, `psd_usability` and the PSD `*_status` columns take the best value over the runs; all other columns have to be identical in every run. Only analysis runs are used unless `only_analysis_runs = false`.

```@example ldm
channelinfo(l200, :p02, :r006, :cal).usability
```

```@example ldm
channelinfo(l200, :p02, :cal).usability
```

### Columns

The table has the columns `detector`, `channel`, `fcid`, `rawid`, `system`, `processable`, `usability`, `is_blinded`, `psd_usability`, `low_aoe_status`, `high_aoe_status`, `lq_status`, `ann_status`, `coax_rt_status`, `is_bb_like`, `partition`, `det_type`, `location`, `detstring`, `fiber` and `position`. The columns `detector` and `channel` hold a [`DetectorId`](@ref) and a [`ChannelId`](@ref).

```@example ldm
propertynames(chinfo)
```

```@example ldm
chinfo.detector
```

```@example ldm
chinfo.channel
```

A row is a `NamedTuple`:

```@example ldm
row = chinfo[1]
(row.detector, row.system, row.usability, row.processable)
```

### Keywords

* `system`: Keep only the channels of one system, e.g. `:geds`, `:spms` or `:puls`. Default is `:all`.
* `only_processable`: Keep only channels that are marked as processable. Default is `false`.
* `only_usability`: Keep only channels with this usability, e.g. `:on`. Default is `:all`.
* `sort_by`: Column to sort by, compared as strings; `:string` sorts by detector string and position. Default is `:detector`.
* `extended`: Add hardware and production columns, see below. Default is `false`.
* `verbose`: Warn if the FCCD of a detector is missing for `extended = true`. Default is `true`.
* `only_analysis_runs`: Use only analysis runs for a period selection. Default is `true`.
* `explode_partitions`: One row per detector and partition, see below. Default is `false`.

```@example ldm
chinfo_geds = channelinfo(l200, :p02, :r006, :cal; system = :geds, only_processable = true)
chinfo_geds.detector
```

```@example ldm
channelinfo(l200, :p02, :r006, :cal; only_usability = :on).detector
```

```@example ldm
channelinfo(l200, :p02, :r006, :cal; sort_by = :channel).detector
```

The keyword filters only cover the common cases. Any other condition on the columns is a `filterby` with a property function:

```@example ldm
channelinfo(l200, mfk) |> filterby(@pf $system == :geds && $usability == :on)
```

### One or Several Channels

A [`DetectorId`](@ref) or [`ChannelId`](@ref) after the selection returns the `NamedTuple` row of that channel. A vector of them returns a table with the rows in the given order. An `ArgumentError` is thrown if a channel is not found.

```@example ldm
channelinfo(l200, mfk, :V99000A).channel
```

```@example ldm
channelinfo(l200, :p02, :r006, :cal, :V99000A).fcid
```

```@example ldm
channelinfo(l200, (:p02, :r006, :cal), [:V99000A, :PULS01]).detector
```

### Extended Information

With `extended = true` the table has the additional columns `cc4`, `cc4ch`, `daqcrate`, `daqcard`, `hvcard`, `hvch`, `enrichment`, `mass`, `total_volume`, `active_volume` and `fccd`. The enrichment, FCCD and active volume carry uncertainties (`Measurements.Measurement`), and all have units: `mass` in kg, `enrichment` in percent, `fccd` in mm and the volumes in cm``^3``. The active volume is the detector volume without the dead layer of thickness `fccd`; the total volume is the volume for `fccd = 0`. Values missing in the metadata are `NaN`, and a missing FCCD is set to zero with a warning if `verbose`.

```@example ldm
chinfo_ext = channelinfo(l200, mfk; extended = true, verbose = false)
(chinfo_ext.detector, chinfo_ext.mass)
```

```@example ldm
chinfo_ext.active_volume
```

### Partitions of Channels

The column `partition` holds the [`DataPartition`](@ref) of each detector that contains the run, `missing` for a detector in no partition. If the metadata has no groupings for the category (`datasets/<category>_groupings`), `partition` is `missing` for every detector. For a period selection the column holds the vector of the partitions that contain the period.

```@example ldm
chinfo.partition
```

```@example ldm
channelinfo(l200, :p02, :cal).partition
```

With `explode_partitions = true` the table has one row per detector and partition with a single `DataPartition` in `partition`; detectors in no partition are left out. This is the form for the processing of partitions that span several periods.

```@example ldm
chinfo_parts = channelinfo(l200, :p02, :cal; system = :geds, explode_partitions = true)
(chinfo_parts.detector, chinfo_parts.partition)
```

!!! note
    The result of `channelinfo` is cached per `LegendData` object and selection. Changes of the metadata on disk are not visible to the same `LegendData` object.

## Partition Information

A data partition groups runs of one category, possibly from several periods, for a joint analysis. The partitions are defined per detector in the metadata `datasets/cal_groupings` and `datasets/phy_groupings`, merged over the `default` entry that holds the partitions of all detectors. A [`DataPartition`](@ref) such as `DataPartition(1)` (`calpartition001a`) names a partition.

[`partitioninfo`](@ref) returns the partitions of a detector in these forms:

* `partitioninfo(data, det, category)`: A dictionary that maps each partition to a table with the columns `period`, `run` and a column named like the category with its run information (see above). Only analysis runs are included.
* `partitioninfo(data, det, partition)`: The table of one partition.
* `partitioninfo(data, det, category, period)`: The partitions that contain the period.
* `partitioninfo(data, det, category, period, run)`: The partitions that contain the run.

```@example ldm
partitions = partitioninfo(l200, :V99000A, :cal)
collect(keys(partitions))
```

```@example ldm
part = partitioninfo(l200, :V99000A, DataPartition(1))
part.run
```

```@example ldm
part.cal.startkey[1]
```

```@example ldm
partitioninfo(l200, :V99000A, :cal, DataPeriod(2))
```

```@example ldm
partitioninfo(l200, :V99000A, :cal, :p02, :r006)
```

A `Symbol` or `String` in place of the category in `partitioninfo(data, det, name, category)` is read as a partition or a period, and a [`ChannelId`](@ref) in place of the detector is converted with the channel information of the first calibration run. An `ArgumentError` is thrown if the metadata has no groupings for the category.

[`get_partition_combined_periods`](@ref) lists the periods that share a calibration partition with a period for the given detectors. [`cal_groupings_default`](@ref) and [`phy_groupings_default`](@ref) return the runs of the default groupings as a table with the columns `period` and `run`.

```@example ldm
get_partition_combined_periods(l200, :p02; dets = [:V99000A])
```

```@example ldm
cal_groupings_default(l200).run
```
