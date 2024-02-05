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
    _name::Symbol
end
export LegendData

get_setup_config(data::LegendData) = getfield(data, :_config)
get_setup_name(data::LegendData) = getfield(data, :_name)

@inline function Base.getproperty(d::LegendData, s::Symbol)
    # Include internal fields:
    if s == :_config
        getfield(d, :_config)
    elseif s == :name
        getfield(d, :_name)
    elseif s == :metadata
        AnyProps(data_path(d, "metadata"))
    elseif s == :tier
        LegendTierData(d)
    elseif s == :par
        AnyProps(data_path(d, "par"))
    elseif s == :jlpar
        AnyProps(data_path(d, "jlpar"))
    else
        throw(ErrorException("LegendData has no property $s"))
    end
end

@inline function Base.propertynames(d::LegendData)
    (:metadata, :tier, :par, :jlpar)
end

@inline function Base.propertynames(d::LegendData, private::Bool)
    props = propertynames(d)
    private ? (:_config, props...) : props
end


function LegendData(setup::Symbol)
    LegendData(getproperty(LegendDataConfig().setups, setup), setup)
end

Base.@deprecate data_filename(data::LegendData, filekey::FileKey, tier::DataTierLike) data.tier[tier, filekey]
export data_filename


"""
    struct LegendDataManagement.LegendTierData

Constructors:

```julia
(data::LegendData).tier

LegendDataManagement.LegendTierData(data::LegendData)
```

The path to data directories and files can be accessed via `getindex` on
`tier_data::LegendTierData`:

```julia
tier_data[]
tier_data[tier::DataTierLike]
tier_data[tier::DataTierLike, category::DataCategoryLike]
tier_data[tier::DataTierLike, category::DataCategoryLike, period::DataPeriodLike]
tier_data[tier::DataTierLike, category::DataCategoryLike, period::DataPeriodLike, run::DataRunLike]

tier_data[tier::DataTierLike, filekey::FileKeyLike]
```

Examples:

```julia
l200 = LegendData(:l200)

filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
isfile(l200.tier[:raw, filekey])

isdir(l200.tier[:raw, :cal])
isdir(l200.tier[:raw, :cal, "p02"])
isdir(l200.tier[:raw, :cal, "p02", "r006"])
isdir(l200.tier[DataTier(:raw), DataCategory(:cal), DataPeriod(2), DataRun(6)])
"""
struct LegendTierData
    data::LegendData
end


"""
    data_path(tier_data::LegendTierData, path_components::AbstractString...)

Get the full absolute path for the given `path_components` relative to `tier_data`.
"""
data_path(tier_data::LegendTierData, path_components::AbstractString...) = data_path(tier_data.data, "tier", path_components...)

Base.getindex(tier_data::LegendTierData, args...) = _getindex_impl(tier_data, args...)

function _getindex_impl(tier_data::LegendTierData)
    data_path(tier_data)
end

function _getindex_impl(tier_data::LegendTierData, tier::DataTierLike)
    data_path(tier_data, string(DataTier(tier)))
end

function _getindex_impl(tier_data::LegendTierData, tier::DataTierLike, category::Union{Symbol, DataCategory})
    data_path(tier_data, string.((DataTier(tier), DataCategory(category),))...)
end

function _getindex_impl(tier_data::LegendTierData, tier::DataTierLike, category::DataCategoryLike, period::DataPeriodLike)
    data_path(tier_data, string.((
        DataTier(tier), DataCategory(category), DataPeriod(period))
    )...)
end

function _getindex_impl(tier_data::LegendTierData, tier::DataTierLike, category::DataCategoryLike, period::DataPeriodLike, run::DataRunLike)
    data_path(tier_data, string.((
        DataTier(tier), DataCategory(category), DataPeriod(period), DataRun(run))
    )...)
end

function _getindex_impl(tier_data::LegendTierData, tier::DataTierLike, filekey::FileKey)
    key = FileKey(filekey)
    joinpath(
        tier_data[DataTier(tier), DataCategory(key), DataPeriod(key), DataRun(key)],
        "$filekey-tier_$tier.lh5"
    )
end

# Disambiguation between DataCategory and FileKey:
function _getindex_impl(tier_data::LegendTierData, tier::DataTierLike, s::AbstractString)
    if _can_convert_to(DataCategory, s)
        String(tier_data[tier, DataCategory(s)])::String
    else
        String(tier_data[tier, FileKey(s)])::String
    end
end


"""
    search_disk(::Type{<:DataSelector}, path::AbstractString)

Search on-disk data for data categories, periods, runs, and filekeys.

Examples:

```julia
l200 = LegendData(:l200)

search_disk(DataCategory, l200.tier[:raw])
search_disk(DataPeriod, l200.tier[:raw, :cal])
search_disk(DataRun, l200.tier[:raw, :cal, "p02"])
search_disk(FileKey, l200.tier[DataTier(:raw), :cal, DataPeriod(2), "r006"])
```
"""
function search_disk end
export search_disk

function search_disk(::Type{DT}, path::AbstractString) where DT<:DataSelector
    all_files = readdir(path)
    valid_files = filter(filename -> _can_convert_to(DT, filename), all_files)
    return unique(sort(DT.(valid_files)))
end


"""
    channelinfo(data::LegendData, sel::AnyValiditySelection; system::Symbol = :all, only_processable::Bool = false)
    channelinfo(data::LegendData, sel::RunCategorySelLike; system::Symbol = :all, only_processable::Bool = false)

Get all channel information for the given [`LegendData`](@ref) and
[`ValiditySelection`](@ref).
"""
function channelinfo(data::LegendData, sel::AnyValiditySelection; system::Symbol = :all, only_processable::Bool = false)
    chmap = data.metadata(sel).hardware.configuration.channelmaps
    dpcfg = data.metadata(sel).dataprod.config.analysis
    
    #filtered_keys = Array{Symbol}(filter(k -> haskey(chmap, k), collect(keys(dpcfg))))
    channel_keys = collect(keys(chmap))

    _convert_location(l::AbstractString) = (location = Symbol(l), detstring = -1, position = -1, fiber = "")

    function _convert_location(l::PropDict)
        (
            location = :array,
            detstring = get(l, :string, -1),
            position = _convert_pos(get(l, :position, -1)),
            fiber = get(l, :fiber, ""),
        )
    end

    _convert_location(l::PropDicts.MissingProperty) = (location = :unkown, detstring = -1, position = -1, fiber = "")

    _convert_pos(p::Integer) = Int(p)
    _convert_pos(p::AbstractString) = Symbol(p)

    function make_row(k::Symbol)
        fcid::Int = get(chmap[k].daq, :fcid, -1)
        rawid::Int = chmap[k].daq.rawid
        channel::ChannelId = ChannelId(fcid >= 0 ? fcid : rawid)

        detector::DetectorId = DetectorId(k)
        local system::Symbol = Symbol(chmap[k].system)
        processable::Bool = get(dpcfg[k], :processable, false)
        usability::Symbol = Symbol(get(dpcfg[k], :usability, :unkown))
        is_blinded::Bool = get(dpcfg[k], :is_blinded, false)
        aoe_status::Symbol = Symbol(get(get(dpcfg[k], :psd_status, PropDict()), Symbol("A/E"), :unkown))

        location::Symbol, detstring::Int, position::Union{Int,Symbol}, fiber::StaticString{8} = _convert_location(chmap[k].location)

        cc4::StaticString{8} = get(chmap[k].electronics.cc4, :id, "")
        cc4ch::Int = get(chmap[k].electronics.cc4, :channel, -1)
        daqcrate::Int = get(chmap[k].daq, :crate, -1)
        daqcard::Int = chmap[k].daq.card.id
        hvcard::Int = get(chmap[k].voltage.card, :id, -1)
        hvch::Int = get(chmap[k].voltage, :channel, -1)

        return (;
            detector, channel, fcid, rawid, system, processable, usability, is_blinded, aoe_status,
            location, detstring, fiber, position, cc4, cc4ch, daqcrate, daqcard, hvcard, hvch
        )
    end

    chinfo = StructArray(make_row.(channel_keys)) 
    if !(system == :all)
        chinfo = chinfo |> filterby(@pf $system .== system)
    end
    if only_processable
        chinfo = chinfo |> filterby(@pf $processable .== true)
    end
    return chinfo
end
export channelinfo

function channelinfo(data::LegendData, sel::RunCategorySelLike; kwargs...)
    channelinfo(data, start_filekey(data, sel); kwargs...)
end


"""
    channelinfo(data::LegendData, sel::AnyValiditySelection, channel::ChannelIdLike)
    channelinfo(data::LegendData, sel::AnyValiditySelection, detector::DetectorIdLike)

Get channel information validitiy selection and [`DetectorId`](@ref) resp.
[`ChannelId`](@ref).
"""
function channelinfo(data::LegendData, sel::Union{AnyValiditySelection, RunCategorySelLike}, channel::ChannelIdLike; kwargs...)
    chinfo = channelinfo(data, sel; kwargs...)
    idxs = findall(x -> ChannelId(x) == ChannelId(channel), chinfo.channel)
    if isempty(idxs)
        throw(ArgumentError("No channel information found for channel $channel"))
    elseif length(idxs) > 1
        throw(ArgumentError("Multiple channel information entries for channel $channel"))
    else
        return chinfo[only(idxs)]
    end
end

function channelinfo(data::LegendData, sel::Union{AnyValiditySelection, RunCategorySelLike}, detector::DetectorIdLike; kwargs...)
    chinfo = channelinfo(data, sel; kwargs...)
    idxs = findall(x -> DetectorId(x) == DetectorId(detector), chinfo.detector)
    if isempty(idxs)
        throw(ArgumentError("No channel information found for detector $detector"))
    elseif length(idxs) > 1
        throw(ArgumentError("Multiple channel information entries for detector $detector"))
    else
        return chinfo[only(idxs)]
    end
end


function channel_info(data::LegendData, sel::AnyValiditySelection)
    Base.depwarn(
        "`channel_info(data::LegendData, sel::AnyValiditySelection)` is deprecated, use `channelinfo(data, sel)` instead (note: output format differs).",
        ((Base.Core).Typeof(channel_info)).name.mt.name
    )

    chinfo = channelinfo(data, sel)

    return StructArray(
        detector = Symbol.(chinfo.detector),
        channel = Int.(chinfo.channel),
        fcid = chinfo.fcid,
        rawid = chinfo.rawid,
        system = chinfo.system,
        processable = chinfo.processable,
        usability = chinfo.usability,
        location = chinfo.location,
        string = chinfo.detstring,
        fiber = chinfo.fiber,
        position = chinfo.position,
        cc4 = chinfo.cc4,
        cc4ch = chinfo.cc4ch,
        daqcrate = chinfo.daqcrate,
        daqcard = chinfo.daqcard,
        hvcard = chinfo.hvcard,
        hvch = chinfo.hvch,
    )
end
export channel_info
