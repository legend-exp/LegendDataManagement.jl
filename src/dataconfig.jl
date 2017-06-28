# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).

using PropDicts


export DataLocations

mutable struct DataLocations
    raw::String
    gen::String
    meta::String
end


Base.convert(::Type{DataLocations}, dict::Dict) = DataLocations(dict[:raw], dict[:gen], dict[:meta])



export SetupConfig

mutable struct SetupConfig
    data::DataLocations
end


Base.convert(::Type{SetupConfig}, dict::Dict) = SetupConfig(convert(DataLocations, dict[:data]))



export DataConfig

mutable struct DataConfig
    setups::Dict{Symbol,SetupConfig}
end


function Base.convert(::Type{DataConfig}, dict::Dict)
    setups = Dict{Symbol,SetupConfig}()
    for (k, v) in dict[:setups]
        setups[k] = convert(SetupConfig, v)
    end
    DataConfig(setups)
end



function Base.read(::Type{DataConfig}, filename::AbstractString)
    p = read(PropDict, filename, subst_pathvar = true, subst_env = true)
    convert(DataConfig, p.dict)
end
