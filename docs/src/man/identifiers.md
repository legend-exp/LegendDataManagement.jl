# Identifiers and FileKeys

LDM represents the parts of LEGEND data names as small types that share the supertype [`DataSelector`](@ref LegendDataManagement.DataSelector): the setup, tier, period, run, category, partition, channel, detector, timestamp and the file key that combines five of them. Functions accept these types or the strings, symbols and numbers that convert to them.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

## Selector Types

| Type | Example string | Content |
|:-----|:---------------|:--------|
| [`ExpSetup`](@ref) | `l200` | experimental setup, field `label` |
| [`DataTier`](@ref) | `raw`, `jldsp` | data tier, field `label` |
| [`DataPeriod`](@ref) | `p02` | data-taking period 0 to 99, field `no` |
| [`DataRun`](@ref) | `r006` | data-taking run 0 to 999, field `no` |
| [`DataCategory`](@ref) | `cal`, `phy` | data category, field `label` |
| [`DataPartition`](@ref) | `calpartition001a` | group of runs analyzed together, fields `no`, `set`, `cat` |
| [`ChannelId`](@ref) | `ch1083204` | DAQ channel, field `no` |
| [`DetectorId`](@ref) | `V99000A` | detector, stored as an integer code |
| [`Timestamp`](@ref) | `20221226T200846Z` | time in seconds since the Unix epoch, field `unixtime` |
| [`FileKey`](@ref) | `l200-p02-r006-cal-20221226T200846Z` | one DAQ cycle, see [File Keys](@ref) |

Each type is built from its string, from a `Symbol` with the same text, or from its natural number. The constructors check the format and throw an `ArgumentError` for text that does not look like the identifier:

```@example ldm
DataPeriod(2), DataPeriod("p02"), DataPeriod(:p02)
```

```@example ldm
(DataRun(6), DataCategory(:cal), DataTier("jldsp"), ExpSetup(:l200))
```

```@example ldm
try DataPeriod("2") catch err sprint(showerror, err) end
```

`string` returns the text of an identifier, and the fields hold the parts:

```@example ldm
period = DataPeriod(2)
(string(period), period.no, string(DataRun(6)), string(DataCategory(:phy)))
```

The tier label is any three lowercase letters, optionally with the prefix `jl` (`raw`, `dsp`, `jldsp`, `jlhit`). The category label is three lowercase letters. LDM does not keep a list of valid tiers or categories, see [Data Tiers and Files](@ref).

### Equality, Ordering and Keys

Identifiers of the same type compare with `==`, `isless` and `sort`, in the order of their numbers or labels. They are not equal to the strings or symbols they convert from.

```@example ldm
(DataPeriod(2) == DataPeriod("p02"), DataPeriod(2) < DataPeriod(10), sort([DataRun(3), DataRun(1)]))
```

Identifiers are scalars in broadcasting. A `PropDict` is indexed by an identifier through the `Symbol` of its text:

```@example ldm
DataPeriod.(["p01", "p02"])
```

```@example ldm
using PropDicts
runs = PropDict(:p02 => "r000..r007")
runs[DataPeriod(2)]
```

### The `*Like` Aliases

Functions that take identifiers are annotated with union aliases. They accept the identifier itself and the text that converts to it:

| Alias | Accepted |
|:------|:---------|
| [`ExpSetupLike`](@ref) | `ExpSetup`, `Symbol`, `AbstractString` |
| [`DataTierLike`](@ref) | `DataTier`, `Symbol`, `AbstractString` |
| [`DataPeriodLike`](@ref) | `DataPeriod`, `Symbol`, `AbstractString` |
| [`DataRunLike`](@ref) | `DataRun`, `Symbol`, `AbstractString` |
| [`DataCategoryLike`](@ref) | `DataCategory`, `Symbol`, `AbstractString` |
| [`DataPartitionLike`](@ref) | `DataPartition`, `Symbol`, `AbstractString` |
| [`ChannelIdLike`](@ref) | `ChannelId`, `Int`, `AbstractString` |
| [`DetectorIdLike`](@ref) | `DetectorId`, `Symbol`, `AbstractString`, `Integer` (the integer code) |
| [`TimestampLike`](@ref) | `Timestamp`, `AbstractString`, `Integer`, `Dates.DateTime`, a time with units |
| [`FileKeyLike`](@ref) | `FileKey`, `AbstractString` |

Period and run numbers are not accepted as integers by these aliases: a function that takes a [`DataPeriodLike`](@ref) accepts `:p02`, `"p02"` or `DataPeriod(2)`, but not `2`. The detector and channel aliases are the exception, and an integer is read as a channel number or as the integer code of a detector, depending on the function.

## Categories, Tiers and Setups

The constructors of `ExpSetup`, `DataTier` and `DataCategory` only check the character pattern. A setup label has 3 to 8 lowercase letters and digits and starts with a letter.

```@example ldm
try DataTier(:Raw) catch err sprint(showerror, err) end
```

## Partitions

A [`DataPartition`](@ref) groups runs that are analyzed together. It has a number, a set letter and a category. A number alone gives the category `cal` and the set `a`. The strings `calgroup001a`, `calpartition001a`, `calpart001a` and `part001` select the same partition:

```@example ldm
partition = DataPartition(1)
(string(partition), partition.no, partition.set, partition.cat)
```

```@example ldm
DataPartition("part001") == DataPartition("calgroup001a") == partition
```

```@example ldm
DataPartition(2, :b, DataCategory(:phy))
```

## Channels and Detectors

A [`ChannelId`](@ref) has the string form `ch` followed by three digits for numbers below 1000 and by seven digits for numbers from 100000 up (the first two digits are not both zero). Other numbers are rejected. `Int` returns the number.

```@example ldm
ch = ChannelId("ch1083204")
(ch.no, Int(ch), string(ChannelId(98)))
```

A [`DetectorId`](@ref) is stored as a 32-bit integer that encodes the detector type and the serial number. Its string form is the detector name: `B`, `C`, `P` or `V` with a five-digit serial number and the slice letter for germanium detectors (`V99000A`), `S` with three digits for SiPMs, `PMT` with three digits, and `PULS`, `AUX`, `DUMMY`, `BSLN` and `MUON` with two digits. The docstring of `DetectorId` gives the encoding.

```@example ldm
detector = DetectorId(:V99000A)
(string(detector), Symbol(detector), UInt32(detector), DetectorId(UInt32(detector)) == detector)
```

```@example ldm
DetectorId.(["PULS01", "S002", "PMT001"])
```

Detector names and channel numbers are related by the channel map of the metadata, which depends on the run. `mfk` is a file key of the toy production in the test metadata (period 2, run 6, category `cal`). `detector2channel` and `channel2detector` of `LegendDataManagement.LDMUtils` look them up, see [Detector and Channel Helpers](@ref).

```@example ldm
using LegendDataManagement.LDMUtils: detector2channel, channel2detector
ch = detector2channel(l200, mfk, :V99000A)
(ch, channel2detector(l200, mfk, ch))
```

## Timestamps

A [`Timestamp`](@ref) stores whole seconds since the Unix epoch in UTC. Its string form is `yyyymmddTHHMMSSZ`. It is built from such a string, from a file key string, from the number of seconds, from a `Dates.DateTime` or from a time with units (Unitful.jl).

```@example ldm
using Dates
ts = Timestamp("20221226T200846Z")
(ts.unixtime, string(Timestamp(1672085326)), Timestamp(DateTime(2022, 12, 26, 20, 8, 46)) == ts, DateTime(ts))
```

## File Keys

A [`FileKey`](@ref) identifies the data of one DAQ cycle. Its string form joins the setup, period, run, category and the start time of the cycle:

```@example ldm
filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
```

The fields give the parts, and the constructors of the part types convert a file key back to them:

```@example ldm
(filekey.setup, filekey.period, filekey.run, filekey.category, filekey.time)
```

```@example ldm
(DataPeriod(filekey), DataRun(filekey), DataCategory(filekey), Timestamp(filekey), DateTime(filekey))
```

```@example ldm
string(filekey)
```

The string constructor accepts the name of a file that starts with a file key. Directories and the text after the timestamp are ignored. The file key is the same for the files of all tiers of a DAQ cycle:

```@example ldm
FileKey("/path/to/l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5") == filekey
```

A file key can also be built from its five parts. The parts are converted by their constructors, so strings and symbols are accepted. Period and run are not accepted as integers here; they are wrapped by `DataPeriod` and `DataRun`.

```@example ldm
FileKey(:l200, :p02, :r006, :cal, "20221226T200846Z") == filekey
```

```@example ldm
FileKey(ExpSetup(:l200), DataPeriod(2), DataRun(6), DataCategory(:cal), Timestamp("20221226T200846Z")) == filekey
```

File keys are ordered by setup, period, run, category and time, so `sort` orders the DAQ cycles of a run chronologically.

[`read_filekeys`](@ref) and [`write_filekeys`](@ref) store a list of file keys in a text file with one key per line. The reader ignores empty lines and comments that start with `#`.

In the example, `scratch_dir` is a temporary directory for the files that the examples write. `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`) and `det` a detector ID with data in these files.

```@example ldm
list = joinpath(scratch_dir, "filekeys.txt")
write_filekeys(list, [mfk, fk])
read_filekeys(list)
```

### First and Surrounding File Keys

The metadata knows the DAQ cycles of every run. [`start_filekey`](@ref) returns the first file key of a run and a category, and [`find_filekey`](@ref) the key of the DAQ cycle that contains a time:

```@example ldm
start_filekey(l200, (:p02, :r006, :cal))
```

```@example ldm
find_filekey(l200, "20220701T000500Z")
```

## Selection Tuples

Functions that work on runs take tuples of identifiers in place of a file key. The elements are accepted in their `Like` form:

| Alias | Tuple | Example | Used by |
|:------|:------|:--------|:--------|
| [`LegendDataManagement.RunCategorySelLike`](@ref) | `(period, run, category)` | `(:p02, :r006, :cal)` | [`start_filekey`](@ref), [`runinfo`](@ref), [`livetime`](@ref), [`channelinfo`](@ref), `detector2channel` |
| [`LegendDataManagement.RunSelLike`](@ref) | `(period, run)` | `(:p02, :r006)` | [`runinfo`](@ref), [`channelinfo`](@ref) |
| [`LegendDataManagement.PeriodSelLike`](@ref) | `(period, category)` | `(:p02, :cal)` | [`channelinfo`](@ref) |

[`start_filekey`](@ref) is the usual way to get the file key that stands for a run:

```@example ldm
start_filekey(l200, (DataPeriod(2), DataRun(6), :cal)) == start_filekey(l200, ("p02", "r006", "cal"))
```

```@example ldm
(length(channelinfo(l200, (:p02, :r006, :cal))), length(channelinfo(l200, (:p02, :cal))), livetime(l200, (:p02, :r006, :phy)))
```

The tuples contain the `Like` types, so period and run numbers have to be wrapped: `(DataPeriod(2), DataRun(6), :cal)`, not `(2, 6, :cal)`.

A time-dependent selection of metadata uses a [`FileKey`](@ref) or a [`LegendDataManagement.ValiditySelection`](@ref) instead, see [Metadata and Validity](@ref).
