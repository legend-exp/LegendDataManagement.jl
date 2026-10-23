# This file is a part of jl, licensed under the MIT License (MIT).

"""
    read_ldata(data::LegendData, selectors...; kwargs...)
    read_ldata(f, data::LegendData, selectors...; kwargs...)
    read_ldata(data::LegendData, selectors::Tuple; kwargs...)
    read_ldata(columns::NTuple{<:Any, Symbol}, data::LegendData, selectors::Tuple; kwargs...)
    read_ldata(column::Symbol, data::LegendData, selectors::Tuple; kwargs...)

Reads `lh5` data from disk for a given set of `selectors`.

After reading in, a `PropertyFunction` `f` can be applied to the data. If a
tuple of `Symbol`s is given, the properties from the tuple are selected; a
single `Symbol` selects one property. `det` is a `DetectorId`. The selectors
are a tier followed by a file key, a vector of file keys or timestamps, or a
data category with a period, a run, a partition or a run table, optionally
followed by a detector or a vector of detectors.

The methods that read data are provided by an extension that is loaded together
with `LegendHDF5IO.jl`; without it the call throws an `ErrorException`. The
extension is described in the docstring of the method for
`(tier, filekey, timestamps, det)` selectors.

A vector of `Unitful.Time` timestamps in place of a `FileKey` reads only the rows with those timestamps, in time order:
each is looked up with [`find_filekey`](@ref) in the DAQ cycle that contains it, and only those rows of the cycle's file
are read. The time of a row is its `timestamp` column, or `tstart` in an event tier. A table without one of the timestamps
is an error, or is skipped with `ignore_missing`. With a `FileKey` and the timestamps, `(tier, filekey, timestamps, det)`,
the cycle is given instead of looked up and the rows follow the timestamps.

The `filterby` kwarg takes a `PropertyFunction` selecting rows of the tier being read. Given a `tier => PropertyFunction` pair instead,
the function is evaluated on `tier` and the resulting row selection is applied to the tier being read. Both tiers must hold one row per
trigger of the same detector, so that their rows correspond by position; a row-count mismatch raises a `DimensionMismatch`.

A run, given by category, period and run or through a period, partition or run table, is read from the file keys
listed for it in the metadata, `only(runinfo(data, (period, run, category)).keys)`; a listed file key that is not
on disk is an error. The keywords `search_disk` and `ignore_keys` control which file keys these are.
A run with no file keys left is an error. A per-run file of the detector, `data.tier[tier, category, period, run, det]`,
holds the whole run and is read in place of the file keys whenever it exists.

## Keywords
* `filterby = nothing`: A `PropertyFunction`, or a `tier => PropertyFunction` pair, that selects the rows to read.
* `ignore_missing::Bool = false`: Skip a table that is missing or has no row for a requested timestamp instead of throwing an error.
* `parallel::Bool = false`: Read the file keys, timestamp cycles or runs in parallel on the workers of `wpool`.
* `wpool::WorkerPool = default_worker_pool()`: Worker pool for `parallel = true`.
* `search_disk::Bool = false`: For a run, read the file keys found on disk with `search_disk(FileKey, data.tier[tier, category, period, run])` instead of those listed in the metadata.
* `ignore_keys::Symbol = :all`: For a run, drop the file keys that [`bad_filekeys`](@ref) returns with `load_key = ignore_keys`, i.e. those listed under `ignore_keys` in `datasets/ignored_daq_cycles`, or under any of its keys for `:all`. `:none` keeps every file key.

## Examples
```julia
using LegendDataManagement, LegendHDF5IO, PropertyFunctions, Unitful

l200 = LegendData(:l200)
filekey = FileKey("l200-p03-r000-cal-20230311T235840Z")
det = DetectorId(:V00000A)

dsp = read_ldata(l200, :jldsp, filekey, det)
dsp = read_ldata((:e_cusp, :e_trap, :blmean), l200, :jldsp, filekey, det)
dsp = read_ldata(:e_cusp, l200, :jldsp, filekey, det)
dsp = read_ldata(l200, :jldsp, :cal, :p03, :r000, det)
dsp = read_ldata(l200, :jldsp, :cal, :p03, :r000, det; search_disk = true, ignore_keys = :none)

dsp = read_ldata(l200, :jldsp, :cal, DataPartition(:calgroup001a), det)
dsp = read_ldata(l200, :jldsp, :cal, DataPeriod(3), det)
dsp = read_ldata(l200, :jldsp, :cal, runinfo(l200)[1:3], det)

dsp = read_ldata(l200, :jldsp, [1.6e9u"s", 1.7e9u"s"], det)
dsp = read_ldata(l200, :jldsp, filekey, [1.6e9u"s", 1.7e9u"s"], det)

dsp = read_ldata(l200, :jldsp, filekey, det; filterby = @pf(\$e_cusp > 1000))
raw = read_ldata(l200, :raw, filekey, det; filterby = :jlhit => @pf(\$is_valid_hit))
```

See also [`search_disk`](@ref), [`runinfo`](@ref), [`partitioninfo`](@ref).
"""
function read_ldata end
export read_ldata

read_ldata(data::LegendData, selectors...; kwargs...) = read_ldata(identity, data, selectors; kwargs...)

read_ldata(f, data::LegendData, selectors...; kwargs...) = read_ldata(f, data, selectors; kwargs...)

read_ldata(data::LegendData, selectors::Tuple; kwargs...) = read_ldata(identity, data, selectors; kwargs...)

read_ldata(columns::NTuple{<:Any, Symbol}, data::LegendData, selectors::Tuple; kwargs...) = read_ldata(PropSelFunction(columns...), data, selectors; kwargs...)

read_ldata(column::Symbol, data::LegendData, selectors::Tuple; kwargs...) = read_ldata((column, ), data, selectors; kwargs...)

_lh5_ext_loaded(::Val) = false

function read_ldata(f::Base.Callable, data::LegendData, selectors::Tuple; kwargs...)
    if !_lh5_ext_loaded(Val(true))
        throw(ErrorException("read_ldata requires LegendHDF5IO.jl to be loaded, e.g. via `using LegendHDF5IO`"))
    end
    throw(ArgumentError("read_ldata doesn't support argument types $(typeof.((f, data, selectors))) with keyword arguments $(keys(kwargs))"))
end