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


const _cached_partitioninfo = LRU{Tuple{UInt, Symbol}, IdDict{DataPartition, Table}}(maxsize = 300)

function _get_partitions(data::LegendData, label::Symbol)
    get!(_cached_partitioninfo, (objectid(data), label)) do
        parts = pydataprod_config(data).partitions.default
        parts_label = get(pydataprod_config(data).partitions, label, PropDict())
        for k in keys(parts_label)
            parts[k] = parts_label[k]
        end
        # type for live time
        rinfo_type = typeof(first(runinfo(data)))
        result::IdDict{
            DataPartition,
            StructVector{rinfo_type}
        } = IdDict([
            let
                periods_and_runs = [
                    let period = DataPeriod(string(p))
                        map(run -> runinfo(data, (period, run)), _resolve_partition_runs(data, period, rs))
                    end
                    for (p,rs) in part
                ]
                # @info periods_and_runs
                flat_pr = vcat(periods_and_runs...)::Vector{rinfo_type}
                DataPartition(pidx)::DataPartition => sort(StructArray(flat_pr))
            end
            for (pidx, part) in parts
        ])

        IdDict{DataPartition, typeof(Table(result[first(keys(result))]))}(keys(result) .=> Table.(values(result)))
    end
end

_resolve_partition_runs(data::LegendData, period::DataPeriod, runs::AbstractVector) = Vector{DataRun}(runs)
function _resolve_partition_runs(data::LegendData, period::DataPeriod, runs::AbstractString)
    if runs == "all"
        search_disk(DataRun, data.tier[:raw, :cal, period])
    else
        throw(ArgumentError("Invalid specification \"$runs\" for runs in data partition"))
    end
end

"""
    partitioninfo(data::LegendData, s::ChannelIdLike)

Return cross-period data partitions.
"""
function partitioninfo(data::LegendData, ch::ChannelIdLike)
    _get_partitions(data, Symbol(ChannelId(ch)))
end
partitioninfo(data, ch, part::DataPartition) = partitioninfo(data, ch)[part]
partitioninfo(data, ch, period::DataPeriod) = sort(Vector{DataPartition}([p for (p, pinfo) in partitioninfo(data, ch) if period in pinfo.period]))
export partitioninfo


Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::ChannelId, p::Vector{<:DataPeriod}) = unique(vcat(f.(Ref(data), Ref(ch), p)...))
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::ChannelId, p::Vector{<:DataPartition}) = vcat(f.(Ref(data), Ref(ch), p)...)
Base.Broadcast.broadcasted(f::typeof(partitioninfo), data::LegendData, ch::Vector{ChannelId}, p::DataPeriod) = f.(Ref(data), ch, Ref(p))


@deprecate data_partitions(data::LegendData, label::Symbol = :default) IdDict([k.no => v for (k, v) in partitioninfo(data, label)])
export data_partitions

const _cached_analysis_runs = LRU{UInt, StructVector{@NamedTuple{period::DataPeriod, run::DataRun}}}(maxsize = 10)

"""
    analysis_runs(data::LegendData)

Return cross-period analysis runs.
"""
function analysis_runs(data::LegendData)
    get!(_cached_analysis_runs, objectid(data)) do
        aruns = pydataprod_config(data).analysis_runs
        periods_and_runs = [
            let period = DataPeriod(string(p))
                map(run -> (period = period, run = run), _resolve_partition_runs(data, period, rs))
            end
            for (p,rs) in aruns
        ]
        flat_pr = vcat(periods_and_runs...)::Vector{@NamedTuple{period::DataPeriod, run::DataRun}}
        StructArray(flat_pr)
    end
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
        nttype = @NamedTuple{startkey::MaybeFileKey, livetime::typeof(1.0u"s")}
        function make_row(p, r, ri)
            period::DataPeriod = DataPeriod(p)
            run::DataRun = DataRun(r)
            function get_cat_entry(cat)
                if haskey(ri, cat)
                    fk = ifelse(haskey(ri[cat], :start_key), FileKey(data.name, period, run, cat, Timestamp(get(ri[cat], :start_key, 1))), missing)
                    nttype((fk, get(ri[cat], :livetime_in_s, NaN)*u"s"))
                else
                    nttype((missing, NaN*u"s"))
                end
            end
            is_ana_phy_run = (; period, run) in analysis_runs(data) && !ismissing(get_cat_entry(:phy).startkey)
            @NamedTuple{period::DataPeriod, run::DataRun, is_analysis_phy_run::Bool, cal::nttype, phy::nttype, fft::nttype}((period, run, Bool(is_ana_phy_run), get_cat_entry(:cal), get_cat_entry(:phy), get_cat_entry(:fft)))
        end
        periods_and_runs = [[make_row(p, r, ri) for (r, ri) in rs] for (p, rs) in rinfo]
        flat_pr = vcat(periods_and_runs...)::Vector{@NamedTuple{period::DataPeriod, run::DataRun, is_analysis_phy_run::Bool, cal::nttype, phy::nttype, fft::nttype}}
        Table(sort(StructArray(flat_pr)))
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
        only(t)
    end
end

function runinfo(data::LegendData, runsel::RunCategorySelLike)
    period, run, category = runsel
    period, run, category = DataPeriod(period), DataRun(run), DataCategory(category)
    getproperty(runinfo(data, (period, run)), Symbol(category))
end
runinfo(data, fk::FileKey) = runinfo(data, (fk.period, fk.run, fk.category))


"""
    start_filekey(data::LegendData, runsel::RunCategorySelLike)

Get the starting filekey for `data` in `period`, `run`, `category`.
"""
start_filekey(data::LegendData, runsel::RunCategorySelLike) = runinfo(data, runsel).startkey
export start_filekey


"""
    livetime(data::LegendData, runsel::RunSelLike)

Get the livetime for `data` in physics data taking of `run` in `period`.
"""
livetime(data::LegendData, runsel::RunCategorySelLike) = runinfo(data, runsel).livetime
export livetime


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
export is_lrun

"""
    is_analysis_phy_run(data::LegendData, period::DataPeriod, run::DataRun)

Return `true` if `run` is an analysis run for `data` in `period`. 
# ATTENTION: This is only valid for `phy` runs.
"""
is_analysis_phy_run(data::LegendData, runsel::RunSelLike) = runinfo(data, runsel).is_analysis_phy_run
export is_analysis_phy_run

const _cached_bad_filekeys = LRU{UInt, Set{FileKey}}(maxsize = 10)

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
