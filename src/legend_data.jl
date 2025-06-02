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
        _ldata_propsdb(d, :metadata)
    elseif s == :tier
        LegendTierData(d)
    elseif s == :par
        _ldata_propsdb(d, :par)
    elseif s == :jlpar
        _ldata_propsdb(d, :jlpar)
    else
        throw(ErrorException("LegendData has no property $s"))
    end
end

function _ldata_propsdb(d::LegendData, dbsym::Symbol)
    dbname = string(dbsym)
    base_path = data_path(d, dbname)
    override_base = joinpath(data_path(d, "metadata"), "jldataprod", "overrides", dbname)
    AnyProps(base_path, override_base = override_base)
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
tier_data[tier::DataTierLike, category::DataCategoryLike, period::DataPeriodLike, run::DataRunLike, ch::ChannelIdLike]

tier_data[tier::DataTierLike, filekey::FileKeyLike]
tier_data[tier::DataTierLike, filekey::FileKeyLike, ch::ChannelIdLike]
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

function _getindex_impl(tier_data::LegendTierData, tier::DataTierLike, category::DataCategoryLike, period::DataPeriodLike, run::DataRunLike, ch::ChannelIdLike)
    joinpath(
        tier_data[DataTier(tier), DataCategory(category), DataPeriod(period), DataRun(run)],
        "$(get_setup_name(tier_data.data))-$period-$run-$category-$ch-tier_$(first(split(string(tier), "ch"))).lh5"
    )
end

_getindex_impl(tier_data::LegendTierData, tier::DataTierLike, filekey::FileKey, ch::ChannelIdLike) = _getindex_impl(tier_data, tier, filekey.category, filekey.period, filekey.run, ch)


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
    search_disk(::Type{DataSet}, data::LegendData; search_categories::Vector{<:DataCategoryLike} = DataCategory.([:cal, :phy]), search_tier::DataTierLike = DataTier(:raw), only_analysis_runs::Bool=true, save_filekeys::Bool=true, ignore_save_tier::Bool=false, save_tier::DataTierLike=DataTier(:jlfks))

Search on-disk data for data categories, periods, runs, and filekeys or whole datasets
If you want to search for a whole `DataSet`, you have the following keyword options:
    - `search_categories` (default: `[:cal, :phy]`): The categories to search on disk.
    - `search_tier` (default: `DataTier(:raw)`): The tier to search on disk.
    - `only_analysis_runs` (default: `true`): Only include for analysis runs as defined in the metadata
    - `save_filekeys` (default: `true`): Save the filekeys to a file in the `save_tier` directory.
    - `ignore_save_tier` (default: `false`): Ignore the `save_tier` and do not save the filekeys.
    - `save_tier` (default: `DataTier(:jlfks)`): The tier to save the filekeys to.

Examples:

```julia
l200 = LegendData(:l200)

search_disk(DataCategory, l200.tier[:raw])
search_disk(DataPeriod, l200.tier[:raw, :cal])
search_disk(DataRun, l200.tier[:raw, :cal, "p02"])
search_disk(FileKey, l200.tier[DataTier(:raw), :cal, DataPeriod(2), "r006"])
search_disk(DataSet, l200)
```
"""
function search_disk end
export search_disk

function search_disk(::Type{DT}, path::AbstractString; kwargs...) where DT<:DataSelector
    all_files = readdir(path)
    valid_files = filter(filename -> _can_convert_to(DT, filename), all_files)
    return unique(sort(DT.(valid_files)))
end

const _cached_dataset = LRU{Tuple{UInt, Vector{<:DataCategoryLike}, DataTierLike, DataTierLike}, DataSet}(maxsize = 10^3)

function search_disk(::Type{DataSet}, data::LegendData; search_categories::Vector{<:DataCategoryLike} = DataCategory.([:cal, :phy]), search_tier::DataTierLike = DataTier(:raw), only_analysis_runs::Bool=true, save_filekeys::Bool=true, ignore_save_tier::Bool=false, save_tier::DataTierLike=DataTier(:jlfks))
    key = (objectid(data), search_categories, search_tier, save_tier)
    if ignore_save_tier && haskey(_cached_dataset, key)
        delete!(_cached_dataset, key)
    end
    get!(_cached_dataset, key) do
        DataSet(let rinfo = runinfo(data)
            sort(tmapreduce(vcat, rinfo) do ri
                vcat([let keylist_filename = joinpath(data.tier[save_tier, cat, ri.period, ri.run], "filekeys.txt"), search_path = data.tier[search_tier, cat, ri.period, ri.run]
                    if !ispath(search_path)
                        Vector{FileKey}()
                    elseif only_analysis_runs && !is_analysis_run(data, ri.period, ri.run, cat)
                        Vector{FileKey}()
                    elseif isfile(keylist_filename) && !ignore_save_tier
                        read_filekeys(keylist_filename)
                    else
                        let fks = search_disk(FileKey, search_path)
                            if save_filekeys
                                mkpath(dirname(keylist_filename))
                                write_filekeys(keylist_filename, fks)
                            end
                            fks
                        end
                    end
                end for cat in search_categories]...)
            end)
        end)
    end
end

"""
    find_filekey(ds::DataSet, ts::TimestampLike)
    find_filekey(data::LegendData, ts::TimestampLike; kwargs...)
Find the filekey in a dataset that is closest to a given timestamp.
The kwargs are passed to `search_disk` to generate the `DataSet`.
"""
function find_filekey end
export find_filekey

function find_filekey(ds::DataSet, ts::TimestampLike)
    last(filter(fk -> fk.time < Timestamp(ts), ds.keys))
end

function find_filekey(data::LegendData, ts; kwargs...)
    find_filekey(search_disk(DataSet, data; kwargs...), ts)
end


const _cached_channelinfo = LRU{Tuple{UInt, AnyValiditySelection, Bool}, StructVector}(maxsize = 10^3)

"""
    channelinfo(data::LegendData, sel::AnyValiditySelection; system::Symbol = :all, only_processable::Bool = false, only_usability::Symbol = :all, extended::Bool = false)
    channelinfo(data::LegendData, sel::RunCategorySelLike; system::Symbol = :all, only_processable::Bool = false, only_usability::Symbol = :all, extended::Bool = false)

Get all channel information for the given [`LegendData`](@ref) and
[`ValiditySelection`](@ref).
"""
function channelinfo(data::LegendData, sel::AnyValiditySelection; system::Symbol = :all, only_processable::Bool = false, only_usability::Symbol = :all, sort_by::Symbol=:detector, extended::Bool = false, verbose::Bool = true)
    key = (objectid(data), sel, extended)
    chinfo = get!(_cached_channelinfo, key) do
        chmap = data.metadata(sel).hardware.configuration.channelmaps
        diodmap = data.metadata.hardware.detectors.germanium.diodes
        dpcfg = data.metadata(sel).dataprod.config.analysis
        
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

        _convert_location(l::PropDicts.MissingProperty) = (location = :unknown, detstring = -1, position = -1, fiber = "")

        _convert_pos(p::Integer) = Int(p)
        function _convert_pos(p::AbstractString)
            if p == "top"
                1
            elseif p == "bottom"
                0
            else
                -1
            end
        end

        function make_row(k::Symbol)

            fcid::Int = get(chmap[k].daq, :fcid, -1)
            rawid::Int = chmap[k].daq.rawid
            channel::ChannelId = ChannelId(rawid)

            detector::DetectorId = DetectorId(k)
            det_type::Symbol = Symbol(ifelse(haskey(diodmap, k), diodmap[k].type, :unknown))
            local system::Symbol = Symbol(chmap[k].system)
            processable::Bool = get(dpcfg[k], :processable, false)
            usability::Symbol = Symbol(get(dpcfg[k], :usability, :unknown))
            is_blinded::Bool = get(dpcfg[k], :is_blinded, false)
            low_aoe_status::Symbol = Symbol(get(get(get(dpcfg[k], :psd, PropDict()), :status, PropDict()), Symbol("low_aoe"), :unknown))
            high_aoe_status::Symbol = Symbol(get(get(get(dpcfg[k], :psd, PropDict()), :status, PropDict()), Symbol("high_aoe"), :unknown))
            lq_status::Symbol = Symbol(get(get(get(dpcfg[k], :psd, PropDict()), :status, PropDict()), Symbol("lq"), :unknown))
            ann_status::Symbol = Symbol(get(get(get(dpcfg[k], :psd, PropDict()), :status, PropDict()), Symbol("ann"), :unknown))
            coax_rt_status::Symbol = Symbol(get(get(get(dpcfg[k], :psd, PropDict()), :status, PropDict()), Symbol("coax_rt"), :unknown))
            is_bb_like::String = replace(get(get(dpcfg[k], :psd, PropDict()), :is_bb_like, ""), "&" => "&&") 
            psd_usability::Symbol = if !(is_bb_like == "missing") &&
                                    ifelse(occursin("low_aoe", is_bb_like), low_aoe_status == :valid, true) && 
                                    ifelse(occursin("high_aoe", is_bb_like), high_aoe_status == :valid, true) &&
                                    ifelse(occursin("lq", is_bb_like), lq_status == :valid, true) &&
                                    ifelse(occursin("ann", is_bb_like), ann_status == :valid, true) &&
                                    ifelse(occursin("coax_rt", is_bb_like), coax_rt_status == :valid, true)
                                    :on
                                else
                                    :off
                                end

            location::Symbol, detstring::Int, position::Int, fiber::StaticString{8} = _convert_location(chmap[k].location)

            c = (;
                detector, channel, fcid, rawid, system, processable, usability, is_blinded, psd_usability, low_aoe_status, high_aoe_status, lq_status, ann_status, coax_rt_status, is_bb_like, det_type,
                location, detstring, fiber, position
            )

            if extended
                cc4::StaticString{8} = get(chmap[k].electronics.cc4, :id, "")
                cc4ch::Int = get(chmap[k].electronics.cc4, :channel, -1)
                daqcrate::Int = get(chmap[k].daq, :crate, -1)
                daqcard::Int = chmap[k].daq.card.id
                hvcard::Int = get(chmap[k].voltage.card, :id, -1)
                hvch::Int = get(chmap[k].voltage, :channel, -1)

                enrichment::Unitful.Quantity{<:Measurement{Float64}} = if haskey(diodmap, k) && haskey(diodmap[k].production, :enrichment) measurement(diodmap[k].production.enrichment.val, diodmap[k].production.enrichment.unc) else measurement(Float64(NaN), Float64(NaN)) end *100u"percent"
                mass::Unitful.Mass{Float64} = if haskey(diodmap, k) && haskey(diodmap[k].production, :mass_in_g) diodmap[k].production.mass_in_g else Float64(NaN) end *1e-3*u"kg"
            
                total_volume::Unitful.Volume{Float64} = if haskey(diodmap, k) get_active_volume(diodmap[k], 0.0) else Float64(NaN) * u"cm^3" end
                fccds = diodmap[k].characterization.combined_0vbb_analysis
                fccd::Unitful.Length{<:Measurement{Float64}} = if isa(fccds, NoSuchPropsDBEntry) ||
                                   isa(fccds, PropDicts.MissingProperty) || 
                                   !haskey(fccds, :fccd_in_mm)
                    verbose && haskey(diodmap, k) && @warn "No FCCD value given for detector $(detector)"
                    measurement(0.0, 0.0) * u"mm"
                else 
                    measurement(fccds.fccd_in_mm.value, maximum(values(fccds.fccd_in_mm.uncertainty))) * u"mm"
                end
                active_volume::Unitful.Volume{<:Measurement{Float64}} = if haskey(diodmap, k) get_active_volume(diodmap[k], ustrip(u"mm", fccd)) else measurement(NaN, NaN) * u"cm^3" end
                c = merge(c, (; cc4, cc4ch, daqcrate, daqcard, hvcard, hvch, enrichment, mass, total_volume, active_volume, fccd))
            end
            c
        end

        StructVector(make_row.(channel_keys))
    end
    # apply filters and masks
    if !(system == :all)
        chinfo = chinfo |> filterby(@pf $system .== system)
    end
    if only_processable
        chinfo = chinfo |> filterby(@pf $processable .== true)
    end
    if !(only_usability == :all)
        chinfo = chinfo |> filterby(@pf $usability .== only_usability)
    end
    # apply sorting
    if sort_by == :string
        chinfo = chinfo |> sortby(@pf $detstring * maximum(chinfo.position) + $position)
    elseif hasproperty(chinfo, sort_by)
        chinfo = chinfo |> sortby(ljl_propfunc("string($sort_by)"))
    end
    return Table(chinfo)
end
export channelinfo

function channelinfo(data::LegendData, sel::RunCategorySelLike; kwargs...)
    channelinfo(data, start_filekey(data, sel); kwargs...)
end
channelinfo(data::LegendData, sel...; kwargs...) = channelinfo(data, sel; kwargs...)
function channelinfo(data::LegendData, sel::Tuple{<:DataPeriodLike, <:DataRunLike, <:DataCategoryLike, Union{ChannelIdLike, DetectorIdLike}}; kwargs...)
    channelinfo(data, ((sel[1:3]), sel[4]); kwargs...)
end

# TODO: Fix LRU cache and make work with channelinfo and channelname
const _cached_channelinfo_detector_idx = LRU{Tuple{UInt, AnyValiditySelection, Symbol}, Int}(maxsize = 10^3)

"""
    channelinfo(data::LegendData, sel::AnyValiditySelection, channel::Union{ChannelIdLike, DetectorIdLike})
    channelinfo(data::LegendData, sel::AnyValiditySelection, detector::DetectorIdLike)

Get channel information validitiy selection and [`DetectorId`](@ref) resp.
[`ChannelId`](@ref).
"""
# function channelinfo(data::LegendData, sel::Union{AnyValiditySelection, RunCategorySelLike}, channel::Union{ChannelIdLike, DetectorIdLike}; kwargs...)
function channelinfo(data::LegendData, sel::Tuple{Union{AnyValiditySelection, RunCategorySelLike}, Union{ChannelIdLike, DetectorIdLike}}; kwargs...)
    sel, channel = sel[1], sel[2]
    key = (objectid(data), sel, Symbol(channel))
    chinfo = channelinfo(data, sel; kwargs...)
    idxs = if _can_convert_to(ChannelId, channel)
        findall(x -> ChannelId(x) == ChannelId(channel), chinfo.channel)
    elseif _can_convert_to(DetectorId, channel)
        findall(x -> DetectorId(x) == DetectorId(channel), chinfo.detector)
    else
        throw(ArgumentError("Invalid channel: $channel"))
    end
    if isempty(idxs)
        throw(ArgumentError("No channel information found for $channel"))
    elseif length(idxs) > 1
        throw(ArgumentError("Multiple channel information entries for $channel"))
    end
    chinfo[only(idxs)]
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

"""
    period_channelinfo(data::LegendData, period::DataPeriod; kwargs...)
Get channel information for a given period, combining all runs in that period.

this channel info takes all runs in this period and creates a combined channelinfo
for :usability, :is_blinded, :psd_usability, :low_aoe_status, :high_aoe_status, :lq_status, :ann_status, :coax_rt_status only the 'best' value is taken, i.e. the one with the highest priority in the hierarchy
all the other column entries should remain the same over different runs

all kwargs and filterby can be used as in the usual channelinfo

make sure to include "Tables" by "using Tables" before using this function
"""

function period_channelinfo(data::LegendData, period::DataPeriod; kwargs...)
    filekey_array = get_filekey_array(data, period)
    chinfo_array = get_chinfo_array(data, filekey_array; kwargs...)
    merged_chinfo = vcat(chinfo_array...)
    red_chinfo = merge_and_reduce_chinfo(merged_chinfo)
    sorted_chinfo = sort_chinfo(red_chinfo)
    extended = get(kwargs, :extended, false)
    hierarchies = get_hierarchies(extended)
    column_order = get_column_order(extended)
    final_chinfo = Table(apply_hierarchies(sorted_chinfo, hierarchies, column_order))
    return final_chinfo
end

function get_filekey_array(l200, period)
    rinfo = runinfo(l200, period) |> filterby(@pf $cal.is_analysis_run)
    return [i.cal.startkey for i in rinfo]
end


function get_chinfo_array(l200, filekey_array; kwargs...)
    extended = get(kwargs, :extended, false)
    return [channelinfo(l200, fk; kwargs...) for fk in filekey_array]
end

function merge_and_reduce_chinfo(merged_chinfo)
    grouped = Dict{Any, Dict{Symbol, Vector}}()
    for row in Tables.rows(merged_chinfo)
        detector = row.detector
        detector_group = get!(grouped, detector, Dict(col => [] for col in propertynames(row) if col != :detector))
        for col in propertynames(row)
            if col != :detector
                push!(detector_group[col], getproperty(row, col))
            end
        end
    end
    red_chinfo = (; detector = collect(keys(grouped)))
    for col in propertynames(first(Tables.rows(merged_chinfo)))
        if col != :detector
            red_chinfo = merge(red_chinfo, (; (col => [grouped[d][col] for d in keys(grouped)])))
        end
    end
    return red_chinfo
end

function sort_chinfo(red_chinfo)
    sorted_indices = sortperm(red_chinfo.detector)
    sorted_chinfo = (; detector = red_chinfo.detector[sorted_indices])
    for col in propertynames(red_chinfo)
        if col != :detector
            sorted_chinfo = merge(sorted_chinfo, (; (col => red_chinfo[col][sorted_indices])))
        end
    end
    return sorted_chinfo
end

function apply_hierarchies(sorted_chinfo, hierarchies, column_order)
    final_chinfo = (;)
    for col in column_order
        if col in keys(hierarchies)
            hierarchy = hierarchies[col]
            if isempty(hierarchy)
                # Skip applying hierarchy if it's empty
                final_chinfo = merge(final_chinfo, (; (col => sorted_chinfo[col])))
                continue
            end
            hierarchy_index = Dict(x => i for (i, x) in enumerate(hierarchy))
            final_chinfo = merge(final_chinfo, (; (col => [
                begin
                    valid_values = filter(x -> haskey(hierarchy_index, x), sorted_chinfo[col][i])
                    valid_values == [] ? nothing : valid_values[argmin(hierarchy_index[x] for x in valid_values)]
                end for i in 1:length(sorted_chinfo.detector)
            ])))
        else
            final_chinfo = merge(final_chinfo, (; (col => [
                sorted_chinfo[col][i] isa AbstractVector ? first(unique(sorted_chinfo[col][i])) : sorted_chinfo[col][i]
                for i in 1:length(sorted_chinfo.detector)
            ])))
        end
    end
    return final_chinfo
end

function get_hierarchies(extended::Bool)
    base_hierarchies = Dict(
        :usability => [:on, :ac, :off],
        :is_blinded => [true, false],
        :psd_usability => [:on, :off],
        :low_aoe_status => [:valid, :present, :missing],
        :high_aoe_status => [:valid, :present, :missing],
        :lq_status => [:valid, :present, :missing],
        :ann_status => [:valid, :present, :missing],
        :coax_rt_status => [:valid, :present, :missing]
    )
    return base_hierarchies
end

function get_column_order(extended::Bool)
    base_columns = [
        :detector, :channel, :fcid, :rawid, :system, :processable,
        :usability, :is_blinded, :psd_usability, :low_aoe_status, :high_aoe_status,
        :lq_status, :ann_status, :coax_rt_status, :is_bb_like,
        :det_type, :location, :detstring, :fiber, :position
    ]
    if extended
        extended_columns = [
            :cc4ch, :daqcrate, :daqcard, :hvcard, :hvch,
            :enrichment, :mass, :total_volume, :active_volume, :fccd
        ]
        return vcat(base_columns, extended_columns)
    else
        return base_columns
    end
end
export period_channelinfo