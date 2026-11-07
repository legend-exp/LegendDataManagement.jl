# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


export DataSet

const _RunCategory = Tuple{DataPeriod,DataRun,DataCategory}

mutable struct DataSet <: AbstractVector{FileKey}
    keys::Vector{FileKey}
    _name::Symbol
    # runinfo livetime of each run and category the keys may come from
    _livetimes::Dict{_RunCategory,typeof(1.0u"s")}
end

DataSet(keys::AbstractVector{FileKey}, name::Symbol) = DataSet(keys, name, Dict{_RunCategory,typeof(1.0u"s")}())
DataSet(keys::AbstractVector{FileKey}) = DataSet(keys, :default)

Base.size(ds::DataSet) = size(ds.keys)
Base.getindex(ds::DataSet, i::Int) = ds.keys[i]
Base.IndexStyle(::Type{DataSet}) = IndexLinear()

"""
    in(ts::TimestampLike, ds::DataSet)

Whether `ts` lies in the time the DAQ cycles of `ds` cover, from the first key of the set to the
last. `ds` must be sorted by time.
"""
Base.in(ts::TimestampLike, ds::DataSet) =
    !isempty(ds) && Timestamp(first(ds)) <= Timestamp(ts) <= Timestamp(last(ds))


const ds_ignore_line_expr = r"^(\s*#.*)?$"

function Base.read(::Type{DataSet}, filename::AbstractString)
    keys = open(filename) do input
        [FileKey(strip(l)) for l in eachline(input) if !occursin(ds_ignore_line_expr, l)]
    end
    DataSet(keys)
end


import Base.==
==(a::DataSet, b::DataSet) = a.keys == b.keys

Base.filter(f, ds::DataSet) = DataSet(filter(f, ds.keys), ds._name, ds._livetimes)


Base.show(io::IO, ds::DataSet) = print(io, "DataSet(:", ds._name, ", ", length(ds.keys), " file keys)")

function Base.show(io::IO, ::MIME"text/plain", ds::DataSet)
    show(io, ds)
    isempty(ds.keys) && return
    days(t) = string(round(ustrip(u"d", t); digits = 2), " d")
    println(io)
    println(io, "  periods:    ", join(sort(unique(key.period for key in ds.keys)), ", "))
    println(io, "  categories: ", join(sort(unique(key.category for key in ds.keys)), ", "))
    println(io, "  first:      ", first(ds.keys))
    print(io,   "  last:       ", last(ds.keys))
    if !isempty(ds._livetimes)
        # A run without a livetime in `_livetimes` makes the sum of its category `NaN`.
        runs = unique((k.period, k.run, k.category) for k in ds.keys)
        livetimes = ["$cat $(days(sum(get(ds._livetimes, r, NaN * u"s") for r in runs if r[3] == cat)))"
                     for cat in sort(unique(last.(runs)))]
        print(io, "\n  livetime:   ", join(livetimes, ", "))
    end
    print(io, "\n  runtime:    ", days((Timestamp(last(ds.keys)).unixtime - Timestamp(first(ds.keys)).unixtime) * u"s"))
end
