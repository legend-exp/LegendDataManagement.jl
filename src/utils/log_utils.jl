##############################
# Log Folder and File Handling
##############################

"""
    get_rreportfolder(data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike)
    get_rreportfolder(data::LegendData, filekey::FileKey)

Returns the folder for the run reports of a given period, run and category,
`data.tier[:jlrep]/rreport/<category>/<period>/<run>`, and creates it if
necessary.

See also [`get_rreportfilename`](@ref).
"""
function get_rreportfolder end
export get_rreportfolder
get_rreportfolder(data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike) = mkpath(joinpath(data.tier[:jlrep], "rreport", string(category), string(period), string(run)))
get_rreportfolder(data::LegendData, filekey::FileKey) = get_rreportfolder(data, filekey.period, filekey.run, filekey.category)


"""
    get_preportfolder(data::LegendData, period::DataPeriodLike, category::DataCategoryLike)

Returns the folder for the period reports of a given period and category,
`data.tier[:jlrep]/preport/<period>/<category>`, and creates it if necessary.

See also [`get_preportfilename`](@ref).
"""
function get_preportfolder end
export get_preportfolder
get_preportfolder(data::LegendData, period::DataPeriodLike, category::DataCategoryLike) = mkpath(joinpath(data.tier[:jlrep], "preport", string(period), string(category)))

"""
    get_rreportfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, process::Symbol)
    get_rreportfilename(data::LegendData, filekey::FileKey, process::Symbol)

Returns the file name of the run report of a given setup, period, run,
category and process.

The file is `<setup>-<period>-<run>-<category>-<process>.md` in the folder
given by [`get_rreportfolder`](@ref).
"""
function get_rreportfilename end
export get_rreportfilename
get_rreportfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, process::Symbol) = joinpath(get_rreportfolder(data, period, run, category), format("{}-{}-{}-{}-{}.md", string(setup), string(period), string(run), string(category), string(process)))
get_rreportfilename(data::LegendData, filekey::FileKey, process::Symbol) = get_rreportfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, process)

"""
    get_preportfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, category::DataCategoryLike, process::Symbol)
    get_preportfilename(data::LegendData, filekey::FileKey, process::Symbol)

Returns the file name of the period report of a given setup, period, category
and process.

The file is `<setup>-<period>-<category>-<process>.md` in the folder given by
[`get_preportfolder`](@ref).
"""
function get_preportfilename end
export get_preportfilename
get_preportfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, category::DataCategoryLike, process::Symbol) = joinpath(get_preportfolder(data, period, category), format("{}-{}-{}-{}.md", string(setup), string(period), string(category), string(process)))
get_preportfilename(data::LegendData, filekey::FileKey, process::Symbol) = get_preportfilename(data, filekey.setup, filekey.period, filekey.category, process)

"""
    create_metadatatbl(filekey::FileKey)
    create_metadatatbl(filekey::FileKey, part::DataPartitionLike)

Creates a one-row table with the metadata of a file key, which can be added to
a report.

The table has the columns `Setup`, `Period`, `Run` and `Category`; with a
partition it has the columns `Setup`, `Partition` and `Category`.
"""
function create_metadatatbl end
export create_metadatatbl
create_metadatatbl(filekey::FileKey) = StructArray(Setup = [filekey.setup], Period = [filekey.period], Run = [filekey.run], Category = [filekey.category])
create_metadatatbl(filekey::FileKey, part::DataPartitionLike) = StructArray(Setup = [filekey.setup], Partition = [part], Category = [filekey.category])

"""
    create_logtbl(result)

Creates a log table from a result of the parallel processing, which can be
added to a report.

`result` is a collection of pairs `(iteration, res)`, where `res.log` is a
`Dict` of log entries or one log entry (a `NamedTuple`-like). The table has
one row per log entry and one column for each key found; missing values are
`"-"`. If there are no entries the table has one row with the column `Status`.
"""
function create_logtbl(result)
    tbl = vcat([collect(values(res.log)) for (itr, res) in result if res.log isa Dict]...)
    append!(tbl, [res.log for (itr, res) in result if !(res.log isa Dict)])
    # empty result (nothing processable): readable one-row table instead of reduce() throwing
    isempty(tbl) && return StructArray([(Status = "no entries - nothing was processed",)])
    unique_keys = unique(reduce(vcat, collect.(keys.(tbl))))
    StructArray([NamedTuple{Tuple(unique_keys)}([get(nt, k, "-") for k in unique_keys]...) for nt in tbl])
end
export create_logtbl


"""
    get_totalTimer(result::Vector)

Returns the combined timer of a result vector.

`result` is a vector of pairs `(iteration, res)`. The timers `res.timer` of
the entries that have one are merged into the first of them, which is
returned; the result is `nothing` if no entry has a timer.
"""
function get_totalTimer(result::Vector)
    totalTimer = nothing
    for (itr, res) in result
        if haskey(res, :timer)
            if isnothing(totalTimer)
                totalTimer = res.timer
            else
                merge!(totalTimer, res.timer)
            end
        end
    end
    totalTimer
end
export get_totalTimer