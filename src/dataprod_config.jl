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
    data.jlpar
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


"""
    partitioninfo(data::LegendData, label::Symbol = :default)

Return cross-period data partitions.
"""
function partitioninfo(data::LegendData, label::Symbol = :default)
    parts = pydataprod_config(data).partitions[label]
    pidxs = Int.(keys(parts))
    result::IdDict{
        DataPartition,
        StructVector{
            @NamedTuple{period::DataPeriod, run::DataRun},
            @NamedTuple{period::Vector{DataPeriod}, run::Vector{DataRun}},
            Int
        }
    } = IdDict([
        let
            periods_and_runs = [
                let period = DataPeriod(string(p))
                    map(run -> (period = period, run = run), _resolve_partition_runs(data, period, rs))
                end
                for (p,rs) in part
            ]
            flat_pr = vcat(periods_and_runs...)::Vector{@NamedTuple{period::DataPeriod, run::DataRun}}
            DataPartition(pidx)::DataPartition => StructArray(flat_pr)
        end
        for (pidx, part) in parts
    ])

    return result
end
export partitioninfo


@deprecate data_partitions(data::LegendData, label::Symbol = :default) IdDict([k.no => v for (k, v) in partitioninfo(data, label)])
export data_partitions


_resolve_partition_runs(data::LegendData, period::DataPeriod, runs::AbstractVector) = Vector{DataRun}(runs)
function _resolve_partition_runs(data::LegendData, period::DataPeriod, runs::AbstractString)
    if runs == "all"
        search_disk(DataRun, data.tier[:raw, :cal, period])
    else
        throw(ArgumentError("Invalid specification \"$runs\" for runs in data partition"))
    end
end

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

"""
    is_analysis_run(data::LegendData, period::DataPeriod, run::DataRun)

Return `true` if `run` is an analysis run for `data` in `period`.
"""
function is_analysis_run(data::LegendData, period::DataPeriodLike, run::DataRunLike)
    (period = DataPeriod(period), run = DataRun(run)) in analysis_runs(data)
end
export is_analysis_run


const _cached_runinfo = LRU{Tuple{UInt, DataPeriod, DataRun, DataCategory}, typeof((startkey = FileKey("l200-p02-r006-cal-20221226T200846Z"), livetime = 0.0u"s"))}(maxsize = 30)

"""
    runinfo(data::LegendData, runsel::RunSelLike)::NamedTuple

Get the run information for `data` in `runsel`.
"""
function runinfo(data::LegendData, runsel::RunCategorySelLike)::NamedTuple
    # unpack runsel
    period_in, run_in, category_in = runsel
    period, run, category = DataPeriod(period_in), DataRun(run_in), DataCategory(category_in)
    get!(_cached_runinfo, (objectid(data), period, run, category)) do
        # check if run, period and category is available
        if !haskey(data.metadata.dataprod.runinfo, Symbol(period))
            throw(ArgumentError("Invalid period $period"))
        elseif !haskey(data.metadata.dataprod.runinfo[Symbol(period)], Symbol(run))
            throw(ArgumentError("Invalid run $run in period $period"))
        elseif !haskey(data.metadata.dataprod.runinfo[Symbol(period)][Symbol(run)], Symbol(category))
            throw(ArgumentError("Invalid category $category in period $period and run $run"))
        end
        (
            startkey = FileKey(data.name, period, run, category, Timestamp(data.metadata.dataprod.runinfo[Symbol(period)][Symbol(run)][Symbol(category)][:start_key])), 
            livetime =  if haskey(data.metadata.dataprod.runinfo[Symbol(period)][Symbol(run)][Symbol(category)], :livetime_in_s)
                            Unitful.Quantity(data.metadata.dataprod.runinfo[Symbol(period)][Symbol(run)][Symbol(category)][:livetime_in_s], u"s") 
                        else 
                            Unitful.Quantity(NaN, u"s")
                        end
        )
    end
end
export runinfo


"""
    start_filekey(data::LegendData, runsel::RunCategorySelLike)

Get the starting filekey for `data` in `period`, `run`, `category`.
"""
function start_filekey(data::LegendData, runsel::RunCategorySelLike)
    runinfo(data, runsel).startkey
end
export start_filekey


"""
    phy_livetime(data::LegendData, runsel::RunSelLike)

Get the livetime for `data` in physics data taking of `run` in `period`.
"""
function livetime(data::LegendData, runsel::RunCategorySelLike)
    runinfo(data, runsel).livetime
end
export phy_livetime

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
