#################################
# Figure Folder and File Handling
#################################

ChannelIdOrDetectorIDLike = Union{ChannelIdLike, DetectorIdLike}

"""
    get_pltfolder(data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, process::Symbol)
    get_pltfolder(data::LegendData, filekey::FileKey, process::Symbol)
    get_pltfolder(data::LegendData, partition::DataPartitionLike, category::DataCategoryLike, process::Symbol, ch::ChannelIdOrDetectorIDLike)
Get the folder for the plot files for a given period, run, category and process.
"""
function get_pltfolder end
get_pltfolder(data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, process::Symbol) = mkpath(joinpath(data.tier[:jlplt], "rplt", string(category), string(period), string(run), string(process)))
get_pltfolder(data::LegendData, filekey::FileKey, process::Symbol) = get_pltfolder(data, filekey.period, filekey.run, filekey.category, process)
get_pltfolder(data::LegendData, partition::DataPartitionLike, category::DataCategoryLike, process::Symbol, ch::ChannelIdOrDetectorIDLike) = mkpath(joinpath(data.tier[:jlplt], "pplt", string(ch), string(partition), string(category), string(process)))

"""
    get_pltfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol)
    get_pltfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike, process::Symbol)
    get_pltfilename(data::LegendData, partition::DataPartitionLike, setup::ExpSetupLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol)
Get the filename for the plot file for a given setup, period, run, category, channel and process.
"""
function get_pltfilename end
get_pltfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdOrDetectorIDLike, process::Symbol) = joinpath(get_pltfolder(data, period, run, category, process), format("{}-{}-{}-{}-{}-{}.png", string(setup), string(period), string(run), string(category), string(ch), string(process)))
get_pltfilename(data::LegendData, filekey::FileKey, ch::ChannelIdOrDetectorIDLike, process::Symbol) = get_pltfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch, process)
get_pltfilename(data::LegendData, partition::DataPartitionLike, setup::ExpSetupLike, category::DataCategoryLike, ch::ChannelIdOrDetectorIDLike, process::Symbol) = joinpath(get_pltfolder(data, partition, category, process, ch), format("{}-{}-{}-{}-{}.png", string(setup), string(partition), string(category), string(ch), string(process)))

"""
    get_plottitle(setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, det::DetectorIdLike, process::AbstractString; additional_type::AbstractString="")
    get_plottitle(filekey::FileKey, det::DetectorIdLike, process::AbstractString; kwargs...)
    get_plottitle(setup::ExpSetupLike, partition::DataPartitionLike, category::DataCategoryLike, det::DetectorIdLike, process::AbstractString; additional_type::AbstractString="")
    get_plottitle(filekey::FileKey, partition::DataPartitionLike, det::DetectorIdLike, process::AbstractString; kwargs...)

Get the title for a plot.
"""
function get_plottitle end
export get_plottitle
_get_plottitle(setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, det::DetectorIdLike, process::AbstractString; additional_type::AbstractString="") = join((i for i in strip.((string(det), additional_type, process, "($(string(setup))-$(string(period))-$(string(run))-$(string(category)))")) if !isempty(i)), " ")
_get_plottitle(filekey::FileKey, det::DetectorIdLike, process::AbstractString; kwargs...) = _get_plottitle(filekey.setup, filekey.period, filekey.run, filekey.category, det, process; kwargs...)
_get_plottitle(setup::ExpSetupLike, partition::DataPartitionLike, category::DataCategoryLike, det::DetectorIdLike, process::AbstractString; additional_type::AbstractString="") = join((i for i in strip.((string(det), additional_type, process, "($(string(setup))-$(string(partition))-$(string(category)))")) if !isempty(i)), " ")
_get_plottitle(filekey::FileKey, partition::DataPartitionLike, det::DetectorIdLike, process::AbstractString; kwargs...) = _get_plottitle(filekey.setup, partition, filekey.category, det, process; kwargs...)

# deprecate typo in old keyword argument (additiional_type)
function get_plottitle(args...; additiional_type::Union{Nothing, <:AbstractString} = nothing, kwargs...) 
    additional_type = if !isnothing(additiional_type)
        Base.depwarn("The keyword argument `additiional_type` is deprecated, use `additional_type` instead.", :get_plottitle, force=true)
        additiional_type
    else
        get(kwargs, :additional_type, "")
    end
    _get_plottitle(args...; additional_type, kwargs...)
end


"""
    savelfig(save_func::Function, p, data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol; kwargs...)
    savelfig(save_func::Function, p, data::LegendData, filekey::FileKey, ch::ChannelIdLike, process::Symbol; kwargs...)
    savelfig(save_func::Function, p, data::LegendData, partition::DataPartitionLike, setup::ExpSetupLike, category::DataCategoryLike, ch::ChannelIdLike, process::Symbol; kwargs...)
Save a lplot.
"""
function savelfig end
export savelfig
savelfig(save_func::Function, p, data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdOrDetectorIDLike, process::Symbol; kwargs...) = save_func(p, get_pltfilename(data, setup, period, run, category, ch, process); kwargs...)
savelfig(save_func::Function, p, data::LegendData, filekey::FileKey, ch::ChannelIdOrDetectorIDLike, process::Symbol; kwargs...) = save_func(p, get_pltfilename(data, filekey, ch, process); kwargs...)
savelfig(save_func::Function, p, data::LegendData, partition::DataPartitionLike, setup::ExpSetupLike, category::DataCategoryLike, ch::ChannelIdOrDetectorIDLike, process::Symbol; kwargs...) = save_func(p, get_pltfilename(data, partition, setup, category, ch, process); kwargs...)
savelfig(save_func::Function, p, data::LegendData, partition::DataPartitionLike, filekey::FileKey, ch::ChannelIdOrDetectorIDLike, process::Symbol; kwargs...) = savelfig(save_func, p, data, partition, filekey.setup, filekey.category, ch, process; kwargs...)