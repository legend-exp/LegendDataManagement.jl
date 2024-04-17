##############################
# Log Folder and File Handling
##############################

"""
    get_logfolder(data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike)
Get the folder for the log files for a given period, run and category.
"""
function get_logfolder end
export get_logfolder
get_logfolder(data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike) = mkpath(joinpath(data.tier[:jllog], "rlog", string(category), string(period), string(run)))
get_logfolder(data::LegendData, filekey::FileKey) = get_logfolder(data, filekey.period, filekey.run, filekey.category)
get_logfolder(data::LegendData, partition::DataPartitionLike, category::DataCategoryLike) = mkpath(joinpath(data.tier[:jllog], "plog", string(partition), string(category)))

"""
    get_logfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, process::Symbol)
Get the filename for the log file for a given setup, period, run, category and process.
"""
function get_logfilename end
export get_logfilename
get_logfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, process::Symbol) = joinpath(get_logfolder(data, period, run, category), format("{}-{}-{}-{}-{}.md", string(setup), string(period), string(run), string(category), string(process)))
get_logfilename(data::LegendData, filekey::FileKey, process::Symbol) = get_logfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, process)
get_logfilename(data::LegendData, setup::ExpSetupLike, partition::DataPartitionLike, category::DataCategoryLike, process::Symbol) = joinpath(get_logfolder(data, partition, category), format("{}-{}-{}-{}.md", string(setup), string(partition), string(category), string(process)))

"""
    create_metadatatbl(filekey::FileKey)
    create_metadatatbl(filekey::FileKey, part::DataPartitionLike)
Create a metadata table for a given filekey which can be added in a report.
"""
function create_metadatatbl end
export create_metadatatbl
create_metadatatbl(filekey::FileKey) = StructArray(Setup = [filekey.setup], Period = [filekey.period], Run = [filekey.run], Category = [filekey.category])
create_metadatatbl(filekey::FileKey, part::DataPartitionLike) = StructArray(Setup = [filekey.setup], Partition = [part], Category = [filekey.category])

"""
    create_logtbl(result)
Create a log table for a given result which can be added in a report.
"""
function create_logtbl(result)
    tbl = vcat([collect(values(res.log)) for (itr, res) in result if res.log isa Dict]...)
    append!(tbl, [res.log for (itr, res) in result if !(res.log isa Dict)])
    unique_keys = unique(reduce(vcat, collect.(keys.(tbl))))
    StructArray([NamedTuple{Tuple(unique_keys)}([get(nt, k, "-") for k in unique_keys]...) for nt in tbl])
end
export create_logtbl


"""
    get_totalTimer(result::Vector)
Get the total timer from a result vector.
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