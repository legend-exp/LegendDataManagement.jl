#################################
# Figure Folder and File Handling
#################################

"""
    get_pltfolder(data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, process::Symbol)
    get_pltfolder(data::LegendData, filekey::FileKey, process::Symbol)
    get_pltfolder(data::LegendData, partition::DataPartitionLike, category::DataCategoryLike, process::Symbol)
Get the folder for the plot files for a given period, run, category and process.
"""
function get_pltfolder end
export get_pltfolder
get_pltfolder(data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, process::Symbol) = mkpath(joinpath(data.tier[:jlplt], "rplt", string(category), string(period), string(run), string(process)))
get_pltfolder(data::LegendData, filekey::FileKey, process::Symbol) = get_pltfolder(data, filekey.period, filekey.run, filekey.category, process)
get_pltfolder(data::LegendData, partition::DataPartitionLike, category::DataCategoryLike, process::Symbol) = mkpath(joinpath(data.tier[:jlplt], "pplt", string(partition), string(category), string(process)))

"""
    get_pltfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol)
    get_pltfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike, process::Symbol)
    get_pltfilename(data::LegendData, partition::DataPartitionLike, setup::ExpSetupLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol)
Get the filename for the plot file for a given setup, period, run, category, channel and process.
"""
function get_pltfilename end
export get_pltfilename
get_pltfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol) = joinpath(get_pltfolder(data, period, run, category, process), format("{}-{}-{}-{}-{}-{}.png", string(setup), string(period), string(run), string(category), string(ch), string(process)))
get_pltfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike, process::Symbol) = get_pltfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch, process)
get_pltfilename(data::LegendData, partition::DataPartitionLike, setup::ExpSetupLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol) = joinpath(get_pltfolder(data, partition, category, process), format("{}-{}-{}-{}-{}.png", string(setup), string(partition), string(category), string(ch), string(process)))

"""
    get_plottitle(setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, det::DetectorIdLike, process::String; additiional_type::String="")
    get_plottitle(filekey::FileKey, det::DetectorIdLike, process::String; kwargs...)
    get_plottitle(setup::ExpSetupLike, partition::DataPartitionLike, category::DataCategoryLike, det::DetectorIdLike, process::String; additiional_type::String="")
Get the title for a plot.
"""
function get_plottitle end
export get_plottitle
get_plottitle(setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, det::DetectorIdLike, process::String; additiional_type::String="") = "$(string(det)) $additiional_type $process  ($(string(setup))-$(string(period))-$(string(run))-$(string(category)))"
get_plottitle(filekey::FileKey, det::DetectorIdLike, process::String; kwargs...) = get_plottitle(filekey.setup, filekey.period, filekey.run, filekey.category, det, process; kwargs...)
get_plottitle(setup::ExpSetupLike, partition::DataPartitionLike, category::DataCategoryLike, det::DetectorIdLike, process::String; additiional_type::String="") = "$(string(det)) $additiional_type $process  ($(string(setup))-$(string(partition))-$(string(category)))"


"""
    savelfig(save_func::Function, p, data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol; kwargs...)
    savelfig(save_func::Function, p, data::LegendData, filekey::FileKey, ch::ChannelIdLike, process::Symbol; kwargs...)
    savelfig(save_func::Function, p, data::LegendData, partition::DataPartitionLike, setup::ExpSetupLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol; kwargs...)
Save a lplot.
"""
function savelfig end
export savelfig
savelfig(save_func::Function, p, data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol; kwargs...) = save_func(p, get_pltfilename(data, setup, period, run, category, ch, process); kwargs...)
savelfig(save_func::Function, p, data::LegendData, filekey::FileKey, ch::ChannelIdLike, process::Symbol; kwargs...) = save_func(p, get_pltfilename(data, filekey, ch, process); kwargs...)
savelfig(save_func::Function, p, data::LegendData, partition::DataPartitionLike, setup::ExpSetupLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol; kwargs...) = save_func(p, get_pltfilename(data, partition, setup, category, ch, process); kwargs...)