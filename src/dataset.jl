# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


export DataSet

mutable struct DataSet
    keys::Vector{FileKey}
    _name::Symbol
end

DataSet(keys::AbstractVector{FileKey}) = DataSet(keys, :default)


const ds_ignore_line_expr = r"^(\s*#.*)?$"

function Base.read(::Type{DataSet}, filename::AbstractString)
    keys = open(filename) do input
        [FileKey(strip(l)) for l in eachline(input) if !occursin(ds_ignore_line_expr, l)]
    end
    DataSet(keys)
end


import Base.==
==(a::DataSet, b::DataSet) = a.keys == b.keys


Base.show(io::IO, ds::DataSet) = print(io, "DataSet(:", ds._name, ", ", length(ds.keys), " file keys)")

function Base.show(io::IO, ::MIME"text/plain", ds::DataSet)
    show(io, ds)
    isempty(ds.keys) && return
    println(io)
    println(io, "  periods:    ", join(sort(unique(key.period for key in ds.keys)), ", "))
    println(io, "  categories: ", join(sort(unique(key.category for key in ds.keys)), ", "))
    println(io, "  first:      ", first(ds.keys))
    print(io,   "  last:       ", last(ds.keys))
end
