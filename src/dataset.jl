# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).


export DataSet

mutable struct DataSet
    keys::Vector{FileKey}
end


const ds_ignore_line_expr = r"^(\s*#.*)?$"

function Base.read(::Type{DataSet}, filename::AbstractString)
    keys = open(filename) do input
        [FileKey(strip(l)) for l in eachline(input) if !occursin(ds_ignore_line_expr, l)]
    end
    DataSet(keys)
end


import Base.==
==(a::DataSet, b::DataSet) = a.keys == b.keys
