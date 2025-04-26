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

function _get_partitions(data::LegendData, label::Symbol; category::DataCategoryLike = :all)
    
    rinfo = runinfo(data)
    rinfo_type = typeof(first(runinfo(data)))

    pd = IdDict{Symbol, Vector{Tuple{DataPeriod, DataRun}}}()
    
    # parse default (and possibly detector-specific) partitions
    partitions = merge(
        data.metadata.datasets.cal_groupings.default,
        get(data.metadata.datasets.cal_groupings, label, PropDict())
    )
    for (n, pr) in partitions
        pd[n] = Tuple{DataPeriod, DataRun}[]
        for (p, r) in pr
            append!(pd[n], tuple.(Ref(DataPeriod(p)), parse_runs(r)))
        end
    end

    # create the Tables
    IdDict(DataPartition.(keys(pd)) .=> Table.((filter(row -> (row.period, row.run) in pr && (category == :all || getproperty(row, category).is_analysis_run), rinfo) for pr in values(pd))))
end


"""
    partitioninfo(data::LegendData, det::DetectorId)::IdDict{DataPartition, Table}
    partitioninfo(data::LegendData, det::DetectorIdLike, part::DataPartitionLike; category::DataCategoryLike=:all)
    partitioninfo(data::LegendData, det::DetectorIdLike, period::DataPeriodLike; category::DataCategoryLike=:all)
    partitioninfo(data, det::DetectorIdLike, period::DataPeriodLike, run::DataRunLike; category::DataCategoryLike=:all)

    partitioninfo(data::LegendData, ch::ChannelId)

Return cross-period data partitions.

# Arguments
- `data::LegendData`: The LegendData object containing the data.
- `det::DetectorIdLike`: The ID of the detector.

# Returns
- `IdDict{DataPartition, Table}`: A dictionary mapping data partitions to tables.
"""
function partitioninfo end
export partitioninfo

partitioninfo(data::LegendData, det::DetectorIdLike; kwargs...) = _get_partitions(data, Symbol(DetectorId(det)); kwargs...)
partitioninfo(data, det, part::DataPartition; kwargs...) = partitioninfo(data, det; kwargs...)[part]
partitioninfo(data, det, period::DataPeriod; kwargs...) = sort(Vector{DataPartition}([p for (p, pinfo) in partitioninfo(data, det; kwargs...) if period in pinfo.period]))
function partitioninfo(data, det, p::Union{Symbol, AbstractString}; kwargs...)
    if _can_convert_to(DataPartition, p)
        partitioninfo(data, det, DataPartition(p); kwargs...)
    elseif _can_convert_to(DataPeriod, p)
        partitioninfo(data, det, DataPeriod(p); kwargs...)
    else 
        throw(ArgumentError("Invalid specification \"$p\". Must be of type DataPartition or DataPeriod"))
    end
end
partitioninfo(data, det, period::DataPeriodLike, run::DataRunLike; kwargs...) = sort(Vector{DataPartition}([p for (p, pinfo) in partitioninfo(data, det; kwargs...) if any(map(row -> row.period == DataPeriod(period) && row.run == DataRun(run), pinfo))]))

# support old method where the ChannelId was passed
function partitioninfo(data::LegendData, ch::ChannelId; kwargs...)
    det = channelinfo(data, first(filter(!ismissing, runinfo(data).cal.startkey)), ch).detector
    _get_partitions(data, Symbol(ChannelId(ch)); kwargs...)
end

Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::ChannelId, p::Vector{<:DataPeriod}) = unique(vcat(f.(Ref(data), Ref(ch), p)...))
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::ChannelId, p::Vector{<:DataPartition}) = vcat(f.(Ref(data), Ref(ch), p)...)
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::Vector{ChannelId}, p::DataPeriod) = f.(Ref(data), ch, Ref(p))

const _cached_combined_partitions = LRU{Tuple{UInt, Symbol, Vector{Symbol}}, Vector{DataPeriod}}(maxsize = 300)

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

function _get_partition_combined_periods(data::LegendData, period::DataPeriodLike, dets::Vector{<:DetectorIdLike})
    period, dets = Symbol(DataPeriod(period)), Symbol.(DetectorId.(dets))
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


const _cached_analysis_runs = LRU{Tuple{UInt, DataCategoryLike}, StructVector{@NamedTuple{period::DataPeriod, run::DataRun}}}(maxsize = 10)
function _analysis_runs(data::LegendData, cat::DataCategoryLike)
    Table(sort(get!(_cached_analysis_runs, (objectid(data), cat)) do
        aruns = data.metadata.datasets.runlists.valid[Symbol(cat)]
        periods_and_runs = [
            map(run -> (period = DataPeriod(p), run = run), parse_runs(rs))
            for (p, rs) in aruns
        ]
        flat_pr = vcat(periods_and_runs...)::Vector{@NamedTuple{period::DataPeriod, run::DataRun}}
        StructArray(flat_pr)
    end))
end

"""
    analysis_runs(data::LegendData)

Return cross-period physics analysis runs.
"""
analysis_phy_runs(data::LegendData) = _analysis_runs(data, :phy)
export analysis_phy_runs


"""
    analysis_runs(data::LegendData)

Return cross-period calibration analysis runs.
"""
analysis_cal_runs(data::LegendData) = _analysis_runs(data, :cal)
export analysis_cal_runs

"""
    analysis_runs(data::LegendData)

Return cross-period analysis runs.
"""
function analysis_runs end
@deprecate analysis_runs(data::LegendData) analysis_phy_runs(data)
export analysis_runs


"""
    cal_groupings_default(data::LegendData)::PropDict

Get all default calibration groupings ("partitions").
"""
function cal_groupings_default(data::LegendData)::PropDict
    cal_analysis_runs = PropDict()
    for (part, runs) in data.metadata.datasets.cal_groupings.default
        for (p, rs) in runs
            cal_analysis_run_p = get!(cal_analysis_runs, p, DataRun[])
            append!(cal_analysis_run_p, parse_runs(rs))
        end
    end
    cal_analysis_runs
end


const MaybeFileKey = Union{FileKey, Missing}

const _cached_runinfo = LRU{UInt, Table}(maxsize = 300)

"""
    runinfo(data::LegendData)::Table
    runinfo(data::LegendData, runsel::RunSelLike)::NamedTuple
    runinfo(data::LegendData, filekey::FileKey)::NamedTuple

Get the run information for `data`.
"""
function runinfo(data::LegendData)
    get!(_cached_runinfo, objectid(data)) do
        # load runinfo
        rinfo = PropDict(data.metadata.datasets.runinfo)
        parts_default = cal_groupings_default(data)
        nttype = @NamedTuple{startkey::MaybeFileKey, livetime::typeof(1.0u"s"), is_analysis_run::Bool}
        function make_row(p, r, ri)
            period::DataPeriod = DataPeriod(p)
            run::DataRun = DataRun(r)
            function get_cat_entry(cat)
                if haskey(ri, cat)
                    fk = ifelse(haskey(ri[cat], :start_key), FileKey(data.name, period, run, cat, Timestamp(get(ri[cat], :start_key, 1))), missing)
                    is_ana_run = if cat == :phy
                        (; period, run) in analysis_phy_runs(data) && !ismissing(fk)
                    elseif cat == :cal 
                        run in get(parts_default, period, [])
                    else
                        false
                    end
                    nttype((fk, get(ri[cat], :livetime_in_s, NaN)*u"s", Bool(is_ana_run)))
                else
                    nttype((missing, NaN*u"s", Bool(false)))
                end
            end
            # is_ana_phy_run = (; period, run) in analysis_runs(data) && !ismissing(get_cat_entry(:phy).startkey)
            # is_ana_cal_run = "$run" in get(parts_default, period, [])
            @NamedTuple{period::DataPeriod, run::DataRun, cal::nttype, phy::nttype, fft::nttype}((period, run, get_cat_entry(:cal), get_cat_entry(:phy), get_cat_entry(:fft)))
        end
        periods_and_runs = [[make_row(p, r, ri) for (r, ri) in rs] for (p, rs) in rinfo]
        flat_pr = sort(StructArray(vcat(periods_and_runs...)::Vector{@NamedTuple{period::DataPeriod, run::DataRun, cal::nttype, phy::nttype, fft::nttype}}))
        Table(merge(columns(flat_pr), (cal = Table(StructArray(flat_pr.cal)), phy = Table(StructArray(flat_pr.phy)), fft = Table(StructArray(flat_pr.fft)))))
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
        Set(read_filekeys(joinpath(data_path(pydataprod_config(data)), "ignore_keys.keylist")))
    end
end
export bad_filekeys
