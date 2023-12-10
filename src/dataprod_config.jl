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
    data_partitions(data::LegendData, label::Symbol = :default)

Return cross-period data partitions.
"""
function data_partitions(data::LegendData, label::Symbol = :default)
    parts = pydataprod_config(data).partitions[label]
    pidxs = Int.(keys(parts))
    result::IdDict{
        Int,
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
            pidx::Int => StructArray(flat_pr)
        end
        for (pidx, part) in parts
    ])

    return result
end
export data_partitions

_resolve_partition_runs(data::LegendData, period::DataPeriod, runs::AbstractVector) = Vector{DataRun}(runs)
function _resolve_partition_runs(data::LegendData, period::DataPeriod, runs::AbstractString)
    if runs == "all"
        search_disk(DataRun, data.tier[:raw, :cal, period])
    else
        throw(ArgumentError("Invalid specification \"$runs\" for runs in data partition"))
    end
end