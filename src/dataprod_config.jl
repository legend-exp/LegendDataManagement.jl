# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

"""
    dataprod_config(data::LegendData)

Get the Julia data production configuration for `data`.

Use `dataprod_config(data)(valsel::AnyValiditySelection)` to also set the
time/category validity selection for the configuration.

Examples:

```julia
l200 = LegendData(:l200)
filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
dataprod_config(l200)(filekey)
```

or

```
l200 = LegendData(:l200)
vsel = ValiditySelection("20221226T200846Z", :cal)
dataprod_config(l200)(vsel)
```
"""
function dataprod_config(data::LegendData)
    metadata = data.metadata
    # ToDo: Remove fallback to `data.dataprod` when no longer required.
    dataprod_metadata = hasproperty(metadata, :jldataprod) ? metadata.jldataprod : metadata.dataprod
    dataprod_metadata.config
end
export dataprod_config


"""
    dataprod_parameters(data::LegendData)

Get the Julia data production parameters `data`.

Examples:

```julia
l200 = LegendData(:l200)
dataprod_config(l200)
```
"""
function dataprod_parameters(data::LegendData)
    data.par
end
export dataprod_parameters


"""
    pydataprod_config(data::LegendData)

Get the Python data production configuration for `data`.

Use `pydataprod_config(data)(valsel::AnyValiditySelection)` to also set the
time/category validity selection for the configuration.

Examples:

```julia
l200 = LegendData(:l200)
filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
pydataprod_config(l200)(filekey)
```

or

```
l200 = LegendData(:l200)
vsel = ValiditySelection("20221226T200846Z", :cal)
pydataprod_config(l200)(vsel)
```
"""
function pydataprod_config(data::LegendData)
    metadata = data.metadata
    # ToDo: Remove fallback to `data.dataprod` when no longer required.
    dataprod_metadata = hasproperty(metadata, :pydataprod) ? metadata.pydataprod : metadata.dataprod
    dataprod_metadata.config
end
export pydataprod_config


"""
    pydataprod_parameters(data::LegendData)

Get the Julia data production parameters `data`.

Examples:

```julia
l200 = LegendData(:l200)
dataprod_config(l200)
```
"""
function pydataprod_parameters(data::LegendData)
    data.par
end
export pydataprod_parameters


const _cached_partitioninfo = LRU{Tuple{UInt, Symbol, Symbol}, IdDict{DataPartition, Table}}(maxsize = 300)
function _get_partitions(data::LegendData, label::Symbol, category::DataCategoryLike)
    category = Symbol(category)
    rinfo = runinfo(data)
    pd = IdDict{Symbol, Vector{Tuple{DataPeriod, DataRun}}}()

    # Merge default and Detector-specific partitions
    if !haskey(data.metadata.datasets, Symbol("$(category)_groupings"))
        throw(ArgumentError("Groupings for category \"$category\" not found"))
    end
    partitions = merge(
        (v1, v2) -> v2,
        data.metadata.datasets[Symbol("$(category)_groupings")].default,
        get(data.metadata.datasets[Symbol("$(category)_groupings")], label, PropDict()),
    )

    # Convert partition definitions into a mapping from detector name to (period, run) tuples
    for (n, pr) in partitions
        pd[n] = Tuple{DataPeriod, DataRun}[]
        for (p, r) in pr
            append!(pd[n], tuple.(Ref(DataPeriod(p)), parse_runs(r)))
        end
    end

    # Keep only period, run, and the selected category
    IdDict(DataPartition.(keys(pd)) .=> [
        Table(NamedTuple{(:period, :run, category)}((row.period, row.run, getproperty(row, category)))
            for row in rinfo if (row.period, row.run) in pr && getproperty(row, category).is_analysis_run)
        for pr in values(pd)
    ])
end


"""
    partitioninfo(data::LegendData, det::DetectorIdLike, cat::DataCategoryLike)
    partitioninfo(data, det, part::DataPartition)
    partitioninfo(data, det, cat, period::DataPeriod)
    partitioninfo(data, det, cat, period, run)

    partitioninfo(data::LegendData, ch::ChannelId, cat::DataCategoryLike = :cal)


Return cross-period data partitions.

# Arguments
- `data::LegendData`: The LegendData object containing the data.
- `det::DetectorIdLike`: The ID of the detector.
- `category::DataCategoryLike`: Analysis category to select groupings, e.g. `:cal`, `:phy`.

# Returns
- `IdDict{DataPartition, Table}`: A dictionary mapping data partitions to tables.
"""
function partitioninfo end
export partitioninfo

partitioninfo(data::LegendData, det::DetectorIdLike, cat::DataCategoryLike) = _get_partitions(data, Symbol(DetectorId(det)), cat)
partitioninfo(data::LegendData, det::DetectorIdLike, part::DataPartition) = partitioninfo(data, det, part.cat)[part]
partitioninfo(data::LegendData, det::DetectorIdLike, cat::DataCategoryLike, period::DataPeriod) = sort(Vector{DataPartition}([p for (p, pinfo) in partitioninfo(data, det, cat) if period in pinfo.period]))
function partitioninfo(data, det, p::Union{Symbol, AbstractString}, cat::DataCategoryLike)
    if _can_convert_to(DataPartition, p)
        partitioninfo(data, det, DataPartition(p))
    elseif _can_convert_to(DataPeriod, p)
        partitioninfo(data, det, cat, DataPeriod(p))
    else 
        throw(ArgumentError("Invalid specification \"$p\". Must be of type DataPartition or DataPeriod"))
    end
end
partitioninfo(data, det, cat, period::DataPeriodLike, run::DataRunLike) = sort(Vector{DataPartition}([p for (p, pinfo) in partitioninfo(data, det, cat) if any(map(row -> row.period == DataPeriod(period) && row.run == DataRun(run), pinfo))]))

Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, det::DetectorId, p::Vector{<:DataPeriod}, cat::DataCategoryLike) = unique(vcat(f.(Ref(data), Ref(det), Ref(cat), p)...))
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, det::DetectorId, p::Vector{<:DataPartition}) = vcat(f.(Ref(data), Ref(det), p)...)
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, det::Vector{DetectorId}, p::DataPeriod, cat::DataCategoryLike) = f.(Ref(data), det, Ref(cat), Ref(p))

# support old method where the ChannelId was passed
function partitioninfo(data::LegendData, ch::ChannelId, args...)
    det = channelinfo(data, first(filter(!ismissing, runinfo(data).cal.startkey)), ch).detector
    partitioninfo(data, det, args...)
end

Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::ChannelId, p::Vector{<:DataPeriod}, cat::DataCategoryLike) = unique(vcat(f.(Ref(data), Ref(ch), Ref(cat), p)...))
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::ChannelId, p::Vector{<:DataPartition}) = vcat(f.(Ref(data), Ref(ch), p)...)
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::Vector{ChannelId}, p::DataPeriod, cat::DataCategoryLike) = f.(Ref(data), ch, Ref(cat), Ref(p))


"""
    get_partition_combined_periods(data::LegendData, period::DataPeriodLike; chs::Vector{<:ChannelIdLike}=ChannelIdLike[])
    get_partition_combined_periods(data::LegendData, period::DataPeriodLike; dets::Vector{<:DetectorIdLike}=DetectorIdLike[])

Get a list periods which are combined in any partition for the given period and list of channels.
"""
function get_partition_combined_periods(data::LegendData, period::DataPeriodLike; chs::Vector{<:ChannelIdLike}=ChannelIdLike[], dets::Vector{<:DetectorIdLike}=DetectorIdLike[])
    if !isempty(chs) && !isempty(dets) 
        throw(ArgumentError("The keyword argument `chs` is deprecated, please pass a `Vector{<:DetectorIdLike}` using the keyword argument `dets`."))
    elseif !isempty(chs)
        @warn "The keyword argument `chs` is deprecated, please pass a `Vector{<:DetectorIdLike}` using the keyword argument `dets`."
        rinfo = runinfo(data, period)
        chinfo = channelinfo(data, first(rinfo.cal.startkey))
        dets = chinfo.detector[broadcast(ch -> findfirst(r -> r.channel == ch, chinfo), chs)]
    end
    _get_partition_combined_periods(data, period, dets)
end
export get_partition_combined_periods

const _cached_combined_partitions = LRU{Tuple{UInt, Symbol, Vector{Symbol}}, Vector{DataPeriod}}(maxsize = 300)
function _get_partition_combined_periods(data::LegendData, period::DataPeriodLike, dets::Vector{<:DetectorIdLike})
    period = Symbol(DataPeriod(period))
    dets   = Symbol[Symbol(DetectorId(d)) for d in dets]
    get!(_cached_combined_partitions, (objectid(data), period, dets)) do
        # load partition information
        parts = data.metadata.datasets.cal_groupings
        # if chs is empty, check for all keys
        if isempty(dets)
            dets = collect(keys(parts))
        end
        # add default
        push!(dets, :default)
        # get all combined periods
        result::Vector{DataPeriod} = unique([DataPeriod(p) for (det, det_parts) in parts if det in dets for (_, detp) in det_parts if period in keys(detp) for p in keys(detp) if period != p])
        result
    end
end

@deprecate data_partitions(data::LegendData, label::Symbol = :default) IdDict([k.no => v for (k, v) in partitioninfo(data, label)])
export data_partitions

"""
    parse_runs(::AbstractVector{<:AbstractString})::Vector{DataRun}
    parse_runs(::AbstractString})::Vector{DataRun}

Parse a `String` or a `Vector{String}` of runs in the format "rXXX" (single run) or "rXXX..rYYY" (range of runs).
"""
function parse_runs(rs::AbstractVector{<:AbstractString})::Vector{DataRun}
    runs::Vector{DataRun} = DataRun[]
    for r in strip.(rs)
        m = match(r"^r(\d{3})..r(\d{3})$", r) # format "rXXX..rYYY"
        if !isnothing(m)
            append!(runs, DataRun.(range(parse.(Int, m.captures)...)))
            continue
        end
        m = match(r"^(r\d{3})$", r) # format "rXXX"
        if !isnothing(m)
            push!(runs, DataRun(Symbol(first(m.captures))))
            continue
        end
        throw(ArgumentError("Invalid syntax to describe run ranges: $(r)"))   
    end
    runs
end
@inline parse_runs(s::AbstractString) = parse_runs([s])


const _cached_analysis_runs = LRU{Tuple{UInt, DataCategoryLike}, StructVector{@NamedTuple{period::DataPeriod, run::DataRun}}}(maxsize = 100)

"""
    analysis_runs(data::LegendData)

Return cross-period analysis runs. Picks the dataset specified in data.dataset.
"""
function analysis_runs(data::LegendData, cat::DataCategoryLike)
    Table(sort(get!(_cached_analysis_runs, (objectid(data), cat)) do
        haskey(data.metadata.datasets.runlists, Symbol(data.dataset)) || error("Requested dataset '$(data.dataset)' not found in runlists.")
        aruns::PropDict = get(getproperty(data.metadata.datasets.runlists, Symbol(data.dataset)), Symbol(cat), PropDict())
        periods_and_runs = Vector{@NamedTuple{period::DataPeriod, run::DataRun}}[
            map(run -> (period = DataPeriod(p), run = run), parse_runs(rs))
            for (p, rs) in aruns
        ]
        flat_pr = collect(Iterators.flatten(periods_and_runs))::Vector{@NamedTuple{period::DataPeriod, run::DataRun}}
        StructArray(flat_pr)
    end))
end
export analysis_runs

@deprecate analysis_runs(data::LegendData) analysis_runs(data, :phy)



const _cached_part_groupings = LRU{Tuple{UInt, DataCategoryLike}, StructVector{@NamedTuple{period::DataPeriod, run::DataRun}}}(maxsize = 100)
function _groupings_runs(data::LegendData, group_category::DataCategoryLike)
    Table(sort(get!(_cached_part_groupings, (objectid(data), group_category)) do
        groupings = data.metadata.datasets[Symbol("$(group_category)_groupings")].default

        flat_pr = Vector{@NamedTuple{period::DataPeriod, run::DataRun}}()
        for group in values(groupings)  
            for (p, rs) in group       
                for r in parse_runs(rs) 
                    push!(flat_pr, (period = DataPeriod(p), run = r))
                end
            end
        end

        StructArray(flat_pr)
    end))
end

"""
    phy_groupings_default(data::LegendData)

Returns default phy_groupings runs.
"""
phy_groupings_default(data::LegendData) = _groupings_runs(data, :phy)
export phy_groupings_default


"""
    cal_groupings_default(data::LegendData)

Returns default cal_groupings runs.
"""
cal_groupings_default(data::LegendData) = _groupings_runs(data, :cal)
export cal_groupings_default


const MaybeFileKey = Union{FileKey, Missing}
const _cached_runinfo = LRU{UInt, Table}(maxsize = 300)

"""
    runinfo(data::LegendData)::Table
    runinfo(data::LegendData, runsel::RunSelLike)::NamedTuple
    runinfo(data::LegendData, filekey::FileKey)::NamedTuple

Get the run information for `data` based on various selection criteria.

# Arguments
- `data::LegendData`: The dataset to query run information from.

# Returns
A table of run information with one named tuple per category (e.g. `:cal`, `:phy`), each containing `startkey`, `livetime`, and `is_analysis_run`

# Example
runinfo(data)                                   # full table of valid runs
runinfo(data, :p03)                             # all runs in period p03
runinfo(data, (:p03, :r005))                    # single-row Table for that run
runinfo(data, (:p03, :r005, :phy))              # only the :phy entry
runinfo(data, fk::FileKey)                      # same as above via FileKey
"""
function runinfo(data::LegendData)
    get!(_cached_runinfo, objectid(data)) do
        rinfo = PropDict(data.metadata.datasets.runinfo)

        # Detect categories dynamically (as Symbols)
        categories = unique(Symbol.(reduce(vcat, (collect(keys(ri)) for (_, runs) in rinfo for (_, ri) in runs))))
        nttype = @NamedTuple{startkey::MaybeFileKey, livetime::typeof(1.0u"s"), is_analysis_run::Bool}

        function make_row(p, r, ri)
            period, run = DataPeriod(p), DataRun(r)
            function get_cat_entry(cat)
                if haskey(ri, cat)
                    fk = ifelse(haskey(ri[cat], :start_key), FileKey(data.name, period, run, cat, Timestamp(get(ri[cat], :start_key, 1))), missing)
                    livetime = get(ri[cat], :livetime_in_s, NaN) * u"s"
                    is_ana_run::Bool = !ismissing(fk) && (!(cat in (:phy, :cal)) || any(row.period == period && row.run == run for row in analysis_runs(data, cat)))
                    nttype((fk, livetime, is_ana_run))
                else
                    nttype((missing, NaN*u"s", false))
                end
            end
            (; period, run, NamedTuple{Tuple(categories)}(Tuple(get_cat_entry(cat) for cat in categories))...)
        end

        # Build rows
        flat_pr = sort(StructArray(vcat([[make_row(p, r, ri) for (r, ri) in rs] for (p, rs) in rinfo]...)))
        merged_cols = merge(columns(flat_pr), (; [(cat => Table(StructArray(getproperty(flat_pr, cat)))) for cat in categories]...))
        Table(merged_cols)
    end
end
export runinfo


runinfo(data::LegendData, period::DataPeriodLike) = runinfo(data) |> filterby(@pf $period == DataPeriod(period))

function runinfo(data::LegendData, runsel::RunSelLike)
    period, run = runsel
    period, run = DataPeriod(period), DataRun(run)
    t = runinfo(data) |> filterby(@pf $period == period && $run == run)
    if isempty(t)
        throw(ArgumentError("No run information found for period $period run $run"))
    else
        Table(t)
    end
end

function runinfo(data::LegendData, runsel::RunCategorySelLike)
    period, run, category = runsel
    period, run, category = DataPeriod(period), DataRun(run), DataCategory(category)
    getproperty(runinfo(data, (period, run)), Symbol(category))
end
runinfo(data, fk::FileKey) = runinfo(data, (fk.period, fk.run, fk.category))
runinfo(data, selectors...) = runinfo(data, selectors)

"""
    start_filekey(data::LegendData, runsel::RunCategorySelLike)

Get the starting filekey for `data` in `period`, `run`, `category`.
"""
function start_filekey end
export start_filekey
start_filekey(data::LegendData, runsel::RunCategorySelLike) = only(runinfo(data, runsel).startkey)
start_filekey(data::LegendData, fk::FileKey) = start_filekey(data, (fk.period, fk.run, fk.category))
start_filekey(data::LegendData, selectors...) = start_filekey(data, selectors)


"""
    livetime(data::LegendData, runsel::RunSelLike)

Get the livetime for `data` in physics data taking of `run` in `period`.
"""
function livetime end
export livetime
livetime(data::LegendData, runsel::RunCategorySelLike) = only(runinfo(data, runsel).livetime)
livetime(data, selectors...) = livetime(data, selectors)

"""
    is_lrun(data::LegendData, runsel::RunSelLike)

Return `true` if `runsel` is a valid run for `data` and therefore appears in the metadata.
"""
function is_lrun(data::LegendData, runsel::RunCategorySelLike)::Bool
    period, run, category = runsel
    !isempty(runinfo(data) |> filterby(@pf $period == DataPeriod(period) && $run == DataRun(run))) && !ismissing(start_filekey(data, runsel))
end
is_lrun(data::LegendData, fk::FileKey) = is_lrun(data, (fk.period, fk.run, fk.category))
is_lrun(data::LegendData, selectors...) = is_lrun(data, selectors)
export is_lrun

"""
    is_analysis_phy_run(data::LegendData, (period::DataPeriodLike, run::DataRunLike))

Return `true` if `run` is an analysis run for `data` in `period`. 
# ATTENTION: This is only valid for `phy` runs.
"""
is_analysis_phy_run(data::LegendData, runsel::RunSelLike) = only(runinfo(data, runsel).phy.is_analysis_run)

"""
    is_analysis_cal_run(data::LegendData, (period::DataPeriodLike, run::DataRunLike))

Return `true` if `run` is an analysis run for `data` in `period`. 
# ATTENTION: This is only valid for `cal` runs.
"""
is_analysis_cal_run(data::LegendData, runsel::RunSelLike) = only(runinfo(data, runsel).cal.is_analysis_run)

"""
    is_analysis_run(data::LegendData, (period::DataPeriodLike, run::DataRunLike, cat::DataCategoryLike))

Return `true` if `run` is an `cat` analysis run for `data` in `period`.
"""
function is_analysis_run end
export is_analysis_run
function is_analysis_run(data::LegendData, runsel::RunCategorySelLike)
    # first check if it is a legend run at all
    if !is_lrun(data, runsel)
        return false
    end
    # unpack runsel
    period, run, category = runsel
    period, run, category = DataPeriod(period), DataRun(run), DataCategory(category)
    if !(hasproperty(runinfo(data), Symbol(category)))
        throw(ArgumentError("Invalid category $category for analysis run"))
    end
    only(runinfo(data, runsel).is_analysis_run)
end
is_analysis_run(data::LegendData, fk::FileKey) = is_analysis_run(data, (fk.period, fk.run, fk.category))
is_analysis_run(data::LegendData, selectors...) = is_analysis_run(data, selectors)

const _cached_bad_filekeys = LRU{UInt, Set{FileKey}}(maxsize = 10^3)

"""
    bad_filekeys(data::LegendData)

Get the list of filekeys to ignore for `data`.
"""
function bad_filekeys(data::LegendData)
    get!(_cached_bad_filekeys, objectid(data)) do
        # Access ignored_daq_cycles directly from metadata.datasets
        if haskey(data.metadata.datasets, :ignored_daq_cycles)
            ignored_data = data.metadata.datasets.ignored_daq_cycles
            bad_keys = Set{FileKey}()
            
            # Process both 'unprocessable' and 'removed' categories
            for category in ["unprocessable", "removed"]
                if haskey(ignored_data, Symbol(category))
                    for entry in ignored_data[Symbol(category)]
                        # Extract FileKey from string like "l200-p15-r005-phy-20250817T223510Z"
                        if isa(entry, String) && startswith(entry, "l200-")
                            try
                                # Parse the FileKey string
                                parsed_fk = FileKey(entry)
                                push!(bad_keys, parsed_fk)
                            catch e
                                @debug "Could not parse FileKey from '$entry': $e"
                            end
                        end
                    end
                end
            end
            bad_keys
        else
            # Fallback to old format if available
            ignore_keys_path = joinpath(data_path(pydataprod_config(data)), "ignore_keys.keylist")
            if isfile(ignore_keys_path)
                Set(read_filekeys(ignore_keys_path))
            else
                @debug "No ignore files found in metadata.datasets.ignored_daq_cycles or $ignore_keys_path"
                Set{FileKey}()
            end
        end
    end
end
export bad_filekeys
