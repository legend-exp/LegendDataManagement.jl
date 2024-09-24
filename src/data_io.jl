# This file is a part of jl, licensed under the MIT License (MIT).

"""
    read_ldata(data::LegendData, selectors...; kwargs...)
    read_ldata(f, data::LegendData, selectors...; kwargs...)
    read_ldata(columns::NTuple{<:Any, Symbol}, data::LegendData, selectors::Tuple; kwargs...)
    read_ldata(column::Symbol, data::LegendData, selectors::Tuple; kwargs...)

Read `lh5` data from disk for a given set of `selectors`. After reading in, a PropertyFunction `f` can be applied to the data. 
If a tuple of `Symbol`s is given, the properties from the tuple are selected. If the `n_evts` kwarg is provided, a random selection with `n_evts` number of
events per file is performed. `ch` can be either a `ChannelId` or a `DetectorId`.
# Examples
```julia
dsp = read_ldata(l200, :jldsp, filekey, ch)
dsp = read_ldata((:e_cusp, :e_trap, :blmean), l200, :jldsp, filekey, ch)
dsp = read_ldata(:e_cusp, l200, :jldsp, filekey, ch)
dsp = read_ldata(l200, :jldsp, :cal, :p03, :r000, ch)

dsp = read_ldata(l200, :jldsp, :cal, DataPartition(1), ch)
dsp = read_ldata(l200, :jldsp, :cal, DataPeriod(3), ch)
dsp = read_ldata(l200, :jldsp, :cal, runinfo(l200)[1:3], ch)

dsp = read_ldata(l200, :jldsp, filekey, ch; n_evts=1000)
```
"""
function read_ldata end
export read_ldata

read_ldata(data::LegendData, selectors...; kwargs...) = read_ldata(identity, data, selectors; kwargs...)

read_ldata(f, data::LegendData, selectors...; kwargs...) = read_ldata(f, data, selectors; kwargs...)

read_ldata(data::LegendData, selectors::Tuple; kwargs...) = read_ldata(identity, data, selectors; kwargs...)

read_ldata(columns::NTuple{<:Any, Symbol}, data::LegendData, selectors::Tuple; kwargs...) = read_ldata(PropSelFunction{columns}(), data, selectors; kwargs...)

read_ldata(column::Symbol, data::LegendData, selectors::Tuple; kwargs...) = read_ldata((column, ), data, selectors; kwargs...)

_lh5_ext_loaded(::Val) = false

function read_ldata(f::Base.Callable, data::LegendData, selectors::Tuple; kwargs...)
    if !_lh5_ext_loaded(Val(true))
        throw(ErrorException("read_ldata requires LegendHDF5IO.jl to be loaded, e.g. via `using LegendHDF5IO`"))
    end
    throw(ArgumentError("read_ldata doesn't support argument types $(typeof.((f, data, selectors))) with keyword arguments $(keys(kwargs))"))
end