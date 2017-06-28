# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).

using JSON
using PropDicts


export optional_calibfor
function optional_calibfor end

export calibfor
function calibfor end


export CalibCatalogEntry

struct CalibCatalogEntry
    key::FileKey
    valid::Dict{Symbol, Int64}
end


function Base.convert(::Type{CalibCatalogEntry}, p::PropDict)
    d = p.dict
    key = d[:key]
    valid = map(e -> e[1] => timestamp2unix(e[2]), d[:valid])
    CalibCatalogEntry(key, valid)
end



export CalibCatalog

mutable struct CalibCatalog
    entries::Vector{CalibCatalogEntry}
    lut::Dict{Symbol,Vector{Pair{Int64, FileKey}}}
end

function CalibCatalog(entries::Vector{CalibCatalogEntry})
    lut = Dict{Symbol,Vector{Pair{Int64, FileKey}}}()

    for e in entries
        for (system, time) in e.valid
            if !haskey(lut, system)
                lut[system] = Vector{Pair{Int64, FileKey}}()
            end
            push!(lut[system], time => e.key)
        end
    end

    for (system, cals) in lut
        sort!(cals, by = x -> x[2].time, alg = Base.Sort.DEFAULT_STABLE)
        sort!(cals, by = x -> x[1], alg = Base.Sort.DEFAULT_STABLE)
    end
    
    CalibCatalog(entries, lut)
end


function Base.read(::Type{CalibCatalog}, filename::AbstractString)
    entries = open(filename) do input
        [convert(CalibCatalogEntry, PropDict(l)) for l in eachline(input)]
    end
    CalibCatalog(entries)
end


function calib_available(calibs::CalibCatalog, key::FileKey, system::Symbol)
    if haskey(calibs.lut, system)
        lut = calibs.lut[system]
        cal_found = searchsortedlast(lut, key.time, by = x -> x[1])
        cal_found != 0
    else
        false
    end
end


function calibfor(calibs::CalibCatalog, key::FileKey, system::Symbol)
    lut = calibs.lut[system]
    cal_found = searchsortedlast(lut, key.time, by = x -> x[1])
    if cal_found == 0
        error("No calibration available for $key-$system")
    else
        lut[cal_found][2]
    end
end
