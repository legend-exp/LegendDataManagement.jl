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

function _get_partitions(data::LegendData, label::Symbol; category::DataCategoryLike=:all)
    let cat = Symbol(DataCategory(category))
        get!(_cached_partitioninfo, (objectid(data), label, cat)) do
            parts = pydataprod_config(data).partitions.default
            parts_label = get(pydataprod_config(data).partitions, label, PropDict())
            for k in keys(parts_label)
                parts[k] = parts_label[k]
            end
            # type for live time
            rinfo_type = typeof(first(runinfo(data)))
            result::IdDict{
                DataPartition,
                Table{rinfo_type}
            } = IdDict([
                let
                    periods_and_runs = [
                        let period = DataPeriod(string(p))
                            filter(row -> row.run in Vector{DataRun}(rs), runinfo(data, period))
                        end
                        for (p,rs) in part
                    ]
                    flat_pr = vcat(periods_and_runs...)::Table{rinfo_type}
                    tab = if cat == :all
                        Table(flat_pr)
                    else
                        Table(filter(row -> getproperty(row, cat).is_analysis_run, Table(flat_pr)))
                    end
                    DataPartition(pidx)::DataPartition => sort(tab)
                end
                for (pidx, part) in parts
            ])

            IdDict{DataPartition, typeof(Table(result[first(keys(result))]))}(keys(result) .=> Table.(values(result)))
        end
    end
end

"""
    partitioninfo(data::LegendData, ch::ChannelId)::IdDict{DataPartition, Table}

    partitioninfo(data::LegendData, ch::ChannelId, part::DataPartitionLike; category::DataCategoryLike=:all)
    partitioninfo(data::LegendData, ch::ChannelId, period::DataPeriodLike; category::DataCategoryLike=:all)
    partitioninfo(data, ch, period::DataPeriodLike, run::DataRunLike; category::DataCategoryLike=:all)

    Return cross-period data partitions.

    # Arguments
    - `data::LegendData`: The LegendData object containing the data.
    - `ch::ChannelId`: The channel identifier.

    # Returns
    - `IdDict{DataPartition, Table}`: A dictionary mapping data partitions to tables.
"""
function partitioninfo end
export partitioninfo
function partitioninfo(data::LegendData, ch::ChannelId; kwargs...)
    _get_partitions(data, Symbol(ChannelId(ch)); kwargs...)
end
function partitioninfo(data::LegendData, det::DetectorIdLike; kwargs...)
    ch = channelinfo(data, first(filter(!ismissing, runinfo(data).cal.startkey)), det).channel
    partitioninfo(data, ch; kwargs...)
end
partitioninfo(data, ch, part::DataPartition; kwargs...) = partitioninfo(data, ch; kwargs...)[part]
partitioninfo(data, ch, period::DataPeriod; kwargs...) = sort(Vector{DataPartition}([p for (p, pinfo) in partitioninfo(data, ch; kwargs...) if period in pinfo.period]))
function partitioninfo(data, ch, p::Union{Symbol, AbstractString}; kwargs...)
    if _can_convert_to(DataPartition, p)
        partitioninfo(data, ch, DataPartition(p); kwargs...)
    elseif _can_convert_to(DataPeriod, p)
        partitioninfo(data, ch, DataPeriod(p); kwargs...)
    else 
        throw(ArgumentError("Invalid specification \"$p\". Must be of type DataPartition or DataPeriod"))
    end
end
partitioninfo(data, ch, period::DataPeriodLike, run::DataRunLike; kwargs...) = sort(Vector{DataPartition}([p for (p, pinfo) in partitioninfo(data, ch; kwargs...) if any(map(row -> row.period == DataPeriod(period) && row.run == DataRun(run), pinfo))]))



Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::ChannelId, p::Vector{<:DataPeriod}) = unique(vcat(f.(Ref(data), Ref(ch), p)...))
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::ChannelId, p::Vector{<:DataPartition}) = vcat(f.(Ref(data), Ref(ch), p)...)
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::Vector{ChannelId}, p::DataPeriod) = f.(Ref(data), ch, Ref(p))

const _cached_combined_partitions = LRU{Tuple{UInt, Symbol, Vector{Symbol}}, Vector{DataPeriod}}(maxsize = 300)

"""
    get_partition_combined_periods(data::LegendData, period::DataPeriodLike; chs::Vector{ChannelIdLike}=ChannelIdLike[])

Get a list periods which are combined in any partition for the given period and list of channels.
"""
function get_partition_combined_periods(data::LegendData, period::DataPeriodLike; chs::Vector{ChannelIdLike}=ChannelIdLike[])
    period, chs = Symbol(DataPeriod(period)), Symbol.(ChannelId.(chs))
    get!(_cached_combined_partitions, (objectid(data), period, chs)) do
        # load partition information
        parts = pydataprod_config(data).partitions
        # if chs is empty, check for all keys
        if isempty(chs)
            chs = collect(keys(parts))
        end
        # add default
        push!(chs, :default)
        # get all combined periods
        result::Vector{DataPeriod} = unique([DataPeriod(p) for (ch, ch_parts) in parts if ch in chs for (_, chp) in ch_parts if period in keys(chp) for p in keys(chp) if period != p])
        result
    end
end
export get_partition_combined_periods

@deprecate data_partitions(data::LegendData, label::Symbol = :default) IdDict([k.no => v for (k, v) in partitioninfo(data, label)])
export data_partitions

const _cached_analysis_runs = LRU{UInt, StructVector{@NamedTuple{period::DataPeriod, run::DataRun}}}(maxsize = 10)

"""
    analysis_runs(data::LegendData)

Return cross-period analysis runs.
"""
function analysis_runs(data::LegendData)
    Table(sort(get!(_cached_analysis_runs, objectid(data)) do
        aruns = pydataprod_config(data).analysis_runs
        periods_and_runs = [
            let period = DataPeriod(string(p))
                map(run -> (period = period, run = run), Vector{DataRun}(rs))
            end
            for (p,rs) in aruns
        ]
        flat_pr = vcat(periods_and_runs...)::Vector{@NamedTuple{period::DataPeriod, run::DataRun}}
        StructArray(flat_pr)
    end))
end
export analysis_runs

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
        rinfo = PropDict(data.metadata.dataprod.runinfo)
        parts_default = merge(values(pydataprod_config(data).partitions.default)...)
        nttype = @NamedTuple{startkey::MaybeFileKey, livetime::typeof(1.0u"s"), is_analysis_run::Bool}
        function make_row(p, r, ri)
            period::DataPeriod = DataPeriod(p)
            run::DataRun = DataRun(r)
            function get_cat_entry(cat)
                if haskey(ri, cat)
                    fk = ifelse(haskey(ri[cat], :start_key), FileKey(data.name, period, run, cat, Timestamp(get(ri[cat], :start_key, 1))), missing)
                    is_ana_run = if cat == :phy
                        (; period, run) in analysis_runs(data) && !ismissing(fk)
                    elseif cat == :cal 
                        "$run" in get(parts_default, period, [])
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
    period, run, category = DataPeriod(period), DataRun(run), DataCategory(category)
    if !isempty(runinfo(data) |> filterby(@pf $period == period && $run == run)) && !ismissing(start_filekey(data, runsel))
        true
    else
        false
    end
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
