# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


"""
    struct LegendData <: AbstractSetupData

Provides access to LEGEND data and metadata.

Constructors:

* `LegendData(setup_config::SetupConfig)`.

* `LegendData(setup::Symbol; dataset::Symbol = :default)` - requires the `\$$_data_config_envvar_name`
  environment variable to be set. `dataset` names the run list in the metadata `datasets/runlists`
  that `runinfo` and `analysis_runs` read; `:default` selects no run list, so `runinfo` holds every
  run the metadata has.

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
    _dataset::Symbol
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
    elseif s == :dataset
        getfield(d, :_dataset)
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
    run_override = dbsym in (:par, :jlpar) ? _load_run_override(d) : nothing
    AnyProps(base_path; override_base, run_override)
end

# `datasets/run_override.yaml` maps the time of runs whose calibration is not usable to the filekey whose calibration applies:
function _load_run_override(d::LegendData)
    primary = joinpath(data_path(d, "metadata"), "datasets", "run_override.yaml")
    override = joinpath(data_path(d, "metadata"), "jldataprod", "overrides", "metadata", "datasets", "run_override.yaml")
    _merge_validity_info!!(_load_validity(primary), _load_validity(override))
end

@inline function Base.propertynames(d::LegendData)
    (:metadata, :tier, :par, :jlpar, :dataset)
end

@inline function Base.propertynames(d::LegendData, private::Bool)
    props = propertynames(d)
    private ? (:_config, props...) : props
end

function LegendData(setup::Symbol; dataset::Symbol = :default)
    ldata = getproperty(LegendDataConfig().setups, setup)
    LegendData(ldata, setup, dataset)
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

function _getindex_impl(tier_data::LegendTierData, tier::DataTierLike, category::DataCategoryLike, period::DataPeriodLike, run::DataRunLike, det::DetectorIdLike)
    joinpath(
        tier_data[DataTier(tier), DataCategory(category), DataPeriod(period), DataRun(run)],
        "$(get_setup_name(tier_data.data))-$period-$run-$category-$det-tier_$(first(split(string(tier), "ch"))).lh5"
    )
end

_getindex_impl(tier_data::LegendTierData, tier::DataTierLike, filekey::FileKey, ch::ChannelIdLike) = _getindex_impl(tier_data, tier, filekey.category, filekey.period, filekey.run, ch)

_getindex_impl(tier_data::LegendTierData, tier::DataTierLike, filekey::FileKey, det::DetectorIdLike) = _getindex_impl(tier_data, tier, filekey.category, filekey.period, filekey.run, det)


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
        end, data.dataset)
    end
end

"""
    find_filekey(ds::DataSet, ts::TimestampLike)
    find_filekey(data::LegendData, ts::TimestampLike)
Find the filekey of the DAQ cycle that contains a given timestamp, i.e. the last key
that starts at or before `ts`. `ds.keys` must be sorted by time; for `data` the
`DataSet` is built (and cached) from the cycle keys of `runinfo(data)`.
"""
function find_filekey end
export find_filekey

function find_filekey(ds::DataSet, ts::TimestampLike)
    i = searchsortedlast(ds.keys, Timestamp(ts); by = Timestamp)
    i < firstindex(ds.keys) && throw(ArgumentError("Timestamp $(Timestamp(ts)) is before the first DAQ cycle key of the data set"))
    ds.keys[i]
end

const _cached_runinfo_dataset = LRU{UInt, DataSet}(maxsize = 300)

function find_filekey(data::LegendData, ts::TimestampLike)
    find_filekey(get!(() -> DataSet(data), _cached_runinfo_dataset, objectid(data)), ts)
end


"""
    DataSet(data::LegendData)
    DataSet(data::LegendData, period::DataPeriodLike)
    DataSet(data::LegendData, period::DataPeriodLike, run::DataRunLike)
    DataSet(data::LegendData, rinfo::Table)

The DAQ cycle keys of every category of the given runs, sorted by time and named after
`data.dataset`. Without a selection the set holds the keys of every run of `runinfo(data)`.
"""
DataSet(data::LegendData, rinfo::Table) = DataSet(sort(reduce(vcat, rinfo.keys; init = FileKey[]); by = Timestamp), data.dataset)
DataSet(data::LegendData) = DataSet(data, runinfo(data))
DataSet(data::LegendData, period::DataPeriodLike) = DataSet(data, runinfo(data, DataPeriod(period)))
DataSet(data::LegendData, period::DataPeriodLike, run::DataRunLike) = DataSet(data, runinfo(data, (DataPeriod(period), DataRun(run))))


const _cached_channelinfo = LRU{Tuple{UInt, Tuple, Bool, Bool}, StructVector}(maxsize = 10^3)
const _cached_channelinfo_inputs = LRU{Tuple{UInt, AnyValiditySelection, Bool}, Tuple}(maxsize = 10^3)

# Validity entries and run that the channel info of `sel` is built from:
function _channelinfo_inputs(data::LegendData, sel::AnyValiditySelection, extended::Bool)
    get!(_cached_channelinfo_inputs, (objectid(data), sel, extended)) do
        fk = sel isa FileKey ? sel : find_filekey(data, sel.timestamp)
        geometry = data.metadata.hardware.geometry
        geometry_files = if extended && !isa(geometry, NoSuchPropsDBEntry)
            (_validity_sel_filelist(geometry.hpge, sel), _validity_sel_filelist(geometry.spms, sel))
        else
            ()
        end
        (_validity_sel_filelist(data.metadata.hardware.configuration.channelmaps, sel),
         _validity_sel_filelist(data.metadata.datasets.statuses, sel),
         geometry_files..., DataCategory(sel.category), fk.period, fk.run)
    end
end
const _bf862_expr = r"^BF862-\d{2}$"

# Status columns whose value may change from run to run, each ordered from best to worst.
# The channel info of a period takes the best value over its runs.
const _channelinfo_status_ranking = (;
    processable = (true, false),
    usability = (:on, :ac, :off, :unknown),
    is_blinded = (true, false),
    psd_usability = (:on, :off),
    low_aoe_status = (:valid, :present, :missing, :unknown),
    high_aoe_status = (:valid, :present, :missing, :unknown),
    lq_status = (:valid, :present, :missing, :unknown),
    ann_status = (:valid, :present, :missing, :unknown),
    coax_rt_status = (:valid, :present, :missing, :unknown),
)

"""
    channelinfo(data::LegendData, sel::AnyValiditySelection; system::Symbol = :all, only_processable::Bool = false, only_usability::Symbol = :all, extended::Bool = false)
    channelinfo(data::LegendData, sel::RunCategorySelLike; system::Symbol = :all, only_processable::Bool = false, only_usability::Symbol = :all, extended::Bool = false)
    channelinfo(data::LegendData, sel::RunSelLike; system::Symbol = :all, only_processable::Bool = false, only_usability::Symbol = :all, extended::Bool = false)
    channelinfo(data::LegendData, sel::PeriodSelLike; only_analysis_runs::Bool = true, explode_partitions::Bool = false, kwargs...)
    channelinfo(data::LegendData, period::DataPeriodLike; only_analysis_runs::Bool = true, explode_partitions::Bool = false, kwargs...)

Get all channel information for the given [`LegendData`](@ref) and
[`ValiditySelection`](@ref). A selection that names no category uses `:cal`.

For a period selection `(period, category)` the channel information of all
runs of that category in the period (only the analysis runs if
`only_analysis_runs`) is merged into one table: the status columns
`processable`, `usability`, `is_blinded`, `psd_usability` and the PSD
`*_status` columns take the best value over the runs, all other columns must
be identical in every run.

The table has the column `partition` after `is_bb_like`, from
[`partitioninfo`](@ref). For a period selection `(period, category)` it holds
the sorted [`DataPartition`](@ref)s of each detector that contain the period,
empty for a detector in no partition. For every other selection it holds the
one `DataPartition` of each detector that contains the run, `missing` for a
detector in no partition. If the metadata has no groupings of the category
(`datasets/<category>_groupings`), `partition` is `missing` for every
detector. With `explode_partitions = true` the table has one row per detector
and partition, with a single `DataPartition` in `partition`; detectors in no
partition are left out.
"""
function channelinfo(data::LegendData, sel::Union{AnyValiditySelection, RunSelLike, PeriodSelLike}; system::Symbol = :all, only_processable::Bool = false, only_usability::Symbol = :all, sort_by::Symbol=:detector, extended::Bool = false, verbose::Bool = true, only_analysis_runs::Bool = true, explode_partitions::Bool = false)
    if sel isa Tuple
        # Both two-element selections are a pair of symbols or strings, so the run and
        # category names tell `(period, run)` and `(period, category)` apart.
        _can_convert_to(DataRun, sel[2]) && return channelinfo(data, (sel[1], sel[2], :cal); system, only_processable, only_usability, sort_by, extended, verbose, explode_partitions)
        sel = (DataPeriod(sel[1]), DataCategory(sel[2]))
    end
    key = (objectid(data), sel isa Tuple ? sel : _channelinfo_inputs(data, sel, extended), extended, only_analysis_runs)
    chinfo = get!(_cached_channelinfo, key) do
        if sel isa Tuple
            period, category = sel
            rinfo = runinfo(data, period)
            hasproperty(rinfo, Symbol(category)) || throw(ArgumentError("No runs of category $category in the run information of $(data.name)"))
            fks = [r.startkey for r in getproperty(rinfo, Symbol(category)) if !ismissing(r.startkey) && (!only_analysis_runs || r.is_analysis_run)]
            isempty(fks) && throw(ArgumentError("No $(only_analysis_runs ? "analysis " : "")runs of category $category in period $period"))
            run_rows = reduce(vcat, [channelinfo(data, fk; extended, verbose) for fk in fks])
            cols = filter(!=(:partition), propertynames(run_rows))
            function merge_column(col::Symbol, det::DetectorId)
                vals = getproperty(run_rows, col)[run_rows.detector .== det]
                if haskey(_channelinfo_status_ranking, col)
                    ranking = _channelinfo_status_ranking[col]
                    all(in(ranking), vals) || throw(ArgumentError("Unknown $col $(setdiff(vals, ranking)) of detector $det: expected one of $ranking"))
                    ranking[minimum(v -> findfirst(==(v), ranking), vals)]
                else
                    all(isequal(first(vals)), vals) || throw(ArgumentError("$col of detector $det differs between the runs of period $period: $(unique(vals))"))
                    first(vals)
                end
            end
            merged = Table(StructVector([NamedTuple{cols}(map(col -> merge_column(col, det), cols)) for det in unique(run_rows.detector)]))
            partition = if _has_groupings(data, category)
                Vector{DataPartition}[partitioninfo(data, det, category, period) for det in merged.detector]
            else
                fill(missing, length(merged))
            end
            return StructVector(columns(_with_partitions(merged, partition)))
        end
        chmap = data.metadata(sel).hardware.configuration.channelmaps
        diodmap = data.metadata.hardware.detectors.germanium.diodes
        dpcfg = data.metadata(sel).datasets.statuses
        geometry = data.metadata.hardware.geometry
        geomap = if extended && !isa(geometry, NoSuchPropsDBEntry) merge(geometry.hpge(sel), geometry.spms(sel)) else PropDict() end
        
        channel_keys = filter(s -> isnothing(match(_bf862_expr, string(s))), keys(chmap))

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

                enrichment::Unitful.Quantity{<:Measurement{Float64}} = if haskey(diodmap, k)  && haskey(diodmap[k].production, :enrichment) && !isnothing(diodmap[k].production.enrichment.val)
                    measurement(diodmap[k].production.enrichment.val, isnothing(diodmap[k].production.enrichment.unc) ? NaN : diodmap[k].production.enrichment.unc)
                else
                    measurement(Float64(NaN), Float64(NaN))
                end * 100u"percent"
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

                coordinates = get(get(geomap, k, PropDict()), :coordinates, PropDict())
                x::Unitful.Length{Float64} = get(coordinates, :x, NaN * u"mm")
                y::Unitful.Length{Float64} = get(coordinates, :y, NaN * u"mm")
                z::Unitful.Length{Float64} = get(coordinates, :z, NaN * u"mm")
                radius::Unitful.Length{Float64} = get(coordinates, :radius, NaN * u"mm")
                angle::typeof(1.0u"°") = get(coordinates, :angle, NaN * u"°")
                barrel::Symbol = Symbol(get(get(geomap, k, PropDict()), :barrel, ""))
                c = merge(c, (; cc4, cc4ch, daqcrate, daqcard, hvcard, hvch, enrichment, mass, total_volume, active_volume, fccd, x, y, z, radius, angle, barrel))
            end
            c
        end

        rows = Table(StructVector(make_row.(channel_keys)))
        fk = sel isa FileKey ? sel : find_filekey(data, sel.timestamp)
        partition = if _has_groupings(data, sel.category)
            Union{Missing, DataPartition}[_run_partition(data, det, DataCategory(sel.category), fk.period, fk.run) for det in rows.detector]
        else
            fill(missing, length(rows))
        end
        StructVector(columns(_with_partitions(rows, partition)))
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
    if explode_partitions
        sel isa Tuple && return _explode_partitions(Table(chinfo))
        rows = findall(!ismissing, chinfo.partition)
        return Table(merge(columns(Table(chinfo)[rows]), (; partition = DataPartition[p for p in chinfo.partition[rows]])))
    end
    return Table(chinfo)
end
export channelinfo

_has_groupings(data::LegendData, category::DataCategoryLike) = haskey(data.metadata.datasets, Symbol("$(DataCategory(category))_groupings"))

# The data partition of category `category` of detector `det` that contains the run, `missing`
# if none does.
function _run_partition(data::LegendData, det::DetectorIdLike, category::DataCategory, period::DataPeriod, run::DataRun)
    parts = partitioninfo(data, det, category, period, run)
    length(parts) <= 1 || throw(ArgumentError("Run $run of period $period is in more than one $category partition of detector $det: $parts"))
    isempty(parts) ? missing : only(parts)
end

# The channel information `chinfo` with the column `partition` after `is_bb_like`.
function _with_partitions(chinfo::Table, partition::AbstractVector)
    cols = columnnames(chinfo)
    i = findfirst(==(:is_bb_like), cols)
    Table(merge(columns(chinfo)[cols[1:i]], (; partition), columns(chinfo)[cols[i+1:end]]))
end

# The channel information `chinfo` with a column `partition` of partition vectors as one row
# per detector and partition, in the order of the detectors and of their partitions.
function _explode_partitions(chinfo::Table)
    rows = Int[i for (i, parts) in pairs(chinfo.partition) if !ismissing(parts) for _ in parts]
    partition = DataPartition[p for parts in chinfo.partition if !ismissing(parts) for p in parts]
    Table(merge(columns(chinfo[rows]), (; partition)))
end

function channelinfo(data::LegendData, sel::RunCategorySelLike; kwargs...)
    # `(period, category, channel)` has the same element types as `(period, run, category)`
    _can_convert_to(DataRun, sel[2]) || return channelinfo(data, ((sel[1], sel[2]), sel[3]); kwargs...)
    channelinfo(data, start_filekey(data, sel); kwargs...)
end
channelinfo(data::LegendData, period::DataPeriodLike; kwargs...) = channelinfo(data, (DataPeriod(period), DataCategory(:cal)); kwargs...)
function channelinfo(data::LegendData, sel::Vararg{Any,N}; kwargs...) where {N}
    N == 1 && throw(MethodError(channelinfo, (data, sel[1])))
    channelinfo(data, sel; kwargs...)
end
function channelinfo(data::LegendData, sel::Tuple{<:DataPeriodLike, <:DataCategoryLike, AbstractVector}; kwargs...)
    channelinfo(data, ((sel[1], sel[2]), sel[3]); kwargs...)
end
function channelinfo(data::LegendData, sel::Tuple{<:DataPeriodLike, <:DataRunLike, <:DataCategoryLike, Union{ChannelIdLike, DetectorIdLike, AbstractVector}}; kwargs...)
    channelinfo(data, ((sel[1:3]), sel[4]); kwargs...)
end

# TODO: Fix LRU cache and make work with channelinfo and channelname
const _cached_channelinfo_detector_idx = LRU{Tuple{UInt, AnyValiditySelection, Symbol}, Int}(maxsize = 10^3)

# Indices of the rows of `channel` (a channel or a detector) in the channel information `chinfo`.
function _channelinfo_rows(chinfo, channel)
    idxs = if _can_convert_to(ChannelId, channel)
        findall(x -> ChannelId(x) == ChannelId(channel), chinfo.channel)
    elseif _can_convert_to(DetectorId, channel)
        findall(x -> DetectorId(x) == DetectorId(channel), chinfo.detector)
    else
        throw(ArgumentError("Invalid channel: $channel"))
    end
    isempty(idxs) && throw(ArgumentError("No channel information found for $channel"))
    return idxs
end

"""
    channelinfo(data::LegendData, sel, channel::Union{ChannelIdLike, DetectorIdLike}; kwargs...)
    channelinfo(data::LegendData, sel, channels::AbstractVector; kwargs...)

Get the channel information of the selection `sel` (see above, e.g. a validity selection,
`(period, run, category)` or `(period, category)`) for one [`ChannelId`](@ref) or
[`DetectorId`](@ref), or for a vector of them. One channel gives its row, or with
`explode_partitions = true` a table of its rows, one per partition. A vector gives a table
with the rows of the channels in the given order. The selection and the channel can also be
passed as separate arguments, e.g. `channelinfo(data, period, category, detector)`.
"""
function channelinfo(data::LegendData, sel::Tuple{Union{AnyValiditySelection, RunSelLike, PeriodSelLike, RunCategorySelLike}, Union{ChannelIdLike, DetectorIdLike}}; explode_partitions::Bool = false, kwargs...)
    sel, channel = sel[1], sel[2]
    chinfo = channelinfo(data, sel; explode_partitions, kwargs...)
    idxs = _channelinfo_rows(chinfo, channel)
    explode_partitions && return chinfo[idxs]
    length(idxs) > 1 && throw(ArgumentError("Multiple channel information entries for $channel"))
    chinfo[only(idxs)]
end

function channelinfo(data::LegendData, sel::Tuple{Union{AnyValiditySelection, RunSelLike, PeriodSelLike, RunCategorySelLike}, AbstractVector}; explode_partitions::Bool = false, kwargs...)
    sel, channels = sel[1], sel[2]
    chinfo = channelinfo(data, sel; explode_partitions, kwargs...)
    idxs = Int[]
    for channel in channels
        rows = _channelinfo_rows(chinfo, channel)
        !explode_partitions && length(rows) > 1 && throw(ArgumentError("Multiple channel information entries for $channel"))
        append!(idxs, rows)
    end
    chinfo[idxs]
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
