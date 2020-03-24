# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).


export DataLocations

mutable struct DataLocations
    raw::String
    gen::String
    meta::String
end


DataLocations(p::PropDict) = DataLocations(p.raw, p.gen, p.meta)

Base.convert(::Type{DataLocations}, p::PropDict) = DataLocations(p)



export SetupConfig

mutable struct SetupConfig
    data::DataLocations
end


SetupConfig(p::PropDict) = SetupConfig(convert(DataLocations, p.data))

Base.convert(::Type{SetupConfig}, p::PropDict) = SetupConfig(p)



export DataConfig

mutable struct DataConfig
    setups::Dict{Symbol,SetupConfig}
end


function DataConfig(p::PropDict)
    setups = Dict{Symbol,SetupConfig}()
    for (k, v) in p.setups
        setups[k] = SetupConfig(v)
    end
    DataConfig(setups)
end

Base.convert(::Type{DataConfig}, p::PropDict) = DataConfig(p)


function Base.read(::Type{DataConfig}, filename::AbstractString)
    p = read(PropDict, filename, subst_pathvar = true, subst_env = true)
    convert(DataConfig, p)
end
