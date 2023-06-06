# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


"""
    struct LegendData <: AbstractSetupData

Provides access to LEGEND data and metadata.

Constructors:

* `LegendData(setup_config::SetupConfig)`.

* `LegendData(setup::Symbol)` - requires the `\$$_data_config_envvar_name` environment variable to
  be set.

Examples:

```julia
config_filename = "/path/to/config.json"
config = LegendDataConfig(config_filename)
l200 = LegendData(config.setups.l200)

filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
```

or simply (if `\$$_data_config_envvar_name` is set):

```julia
l200 = LegendData(:l200)
```

`LegendData` has the (virtual) properties `metadata` and `tier`.

The full path to "tier" data files can be retrieved using
```julia
(data::LegendData)[tier::Symbol, filekey::FileKey]
(data::LegendData).tier[tier::Symbol, filekey::AbstractString]
```

Example:

```julia
l200.tier[:raw]
l200.tier[:raw, FileKey("l200-p02-r006-cal-20221226T200846Z")]
```

LegendData comes with an extension for SolidStateDetectors:

```julia
l200 = LegendData(:l200)
SolidStateDetector(l200, :V99000A)
```
"""
struct LegendData <: AbstractSetupData
    # ToDo: Add setup name
    _config::SetupConfig
end
export LegendData

get_setup_config(data::LegendData) = getfield(data, :_config)

@inline function Base.getproperty(d::LegendData, s::Symbol)
    # Include internal fields:
    if s == :_config
        getfield(d, :_config)
    elseif s == :metadata
        AnyProps(data_path(d, ["metadata"]))
    elseif s == :tier
        LegendTierData(d)
    else
        throw(ErrorException("LegendData has no property $s"))
    end
end

@inline function Base.propertynames(d::LegendData)
    (:metadata, :tier)
end

@inline function Base.propertynames(d::LegendData, private::Bool)
    props = propertynames(d)
    private ? (:_config, props...) : props
end


function LegendData(setup::Symbol)
    LegendData(getproperty(LegendDataConfig().setups, setup))
end


# tier/raw/cal/p03/r002/l200-p03-r002-cal-20230324T171017Z-tier_raw.lh5
#  par/hit/cal/p03/r003/l200-p03-r003-cal-20230331T161141Z-par_hit.json


function _key_path_components(filekey::FileKey)
    [string(DataCategory(filekey)), string(DataPeriod(filekey)), string(DataRun(filekey))]
end

function data_filename(data::LegendData, filekey::FileKey, tier::DataTierLike)
    # ToDo: Check that setup matches setup name in data (to be added).
    p = ["tier", string(tier), _key_path_components(filekey)..., "$(filekey)-tier_$(DataTier(tier)).lh5"]
    data_path(data, p)
end
export data_filename

# ToDo:
# par_filename(data::LegendData, key::FileKey, tier::Symbol)


"""
    struct LegendDataManagement.LegendTierData

Constructors:

```julia
(data::LegendData).tier

LegendDataManagement.LegendTierData(data::LegendData)
```

`tier_data::LegendTierData` supports

The full path of data files can be retrieved using

```julia
data[tier::Symbol]
data[tier::Symbol, filekey::FileKey]
data[tier::Symbol, filekey::AbstractString]
```
"""
struct LegendTierData
    data::LegendData
end

function Base.getindex(tier_data::LegendTierData, tier::DataTierLike)
    data_path(get_setup_config(tier_data.data), ["tier", string(DataTier(tier))])
end

function Base.getindex(tier_data::LegendTierData, tier::DataTierLike, filekey::FileKeyLike)
    key = FileKey(filekey)
    data_path(
        get_setup_config(tier_data.data), [
            "tier", string(DataTier(tier)), string(DataCategory(key)),
            string(DataPeriod(key)), string(DataRun(key)),
            "$filekey-tier_$tier.lh5"
        ]
    )
end


"""
    channel_info(data::LegendData, filekey::FileKey)

Get channel information for a given filekey.
"""
function channel_info(data::LegendData, filekey::FileKey)
    chmap = data.metadata(filekey).hardware.configuration.channelmaps
    dpcfg = data.metadata(filekey).dataprod.config.analysis
    
    filtered_keys = Array{Symbol}(filter(k -> haskey(chmap, k), collect(keys(dpcfg))))

    # ToDo: Add this to PropDicts.jl
    _get(d::PropDict, key::Symbol, default) = haskey(d, key) ? d[key] : default

    make_row(k::Symbol) = (
        detector = Symbol(k)::Symbol,
        fcid = Int(_get(chmap[k].daq, :fcid, -1))::Int,
        rawid = Int(chmap[k].daq.rawid)::Int,
        system = Symbol(chmap[k].system)::Symbol,
        processable = Bool(dpcfg[k].processable)::Bool,
        usability = (dpcfg[k].usability == "on")::Bool,
    )

    StructArray(make_row.(filtered_keys))
end
export channel_info
