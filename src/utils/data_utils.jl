###################
# Data Access utils
###################

"""
    get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
Get the filename for the peaks data for a given channel.
"""
function get_peaksfilename end
export get_peaksfilename
get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike) = joinpath(data.tier[:peaks, :cal, period, run], format("{}-{}-{}-{}-{}-tier_peaks.lh5", string(setup), string(period), string(run), string(category), string(ch)))
get_peaksfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) = get_peaksfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch)

"""
    get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
Get the filename for the hitch data for a given channel.
"""
function get_hitchfilename end
export get_hitchfilename
get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike) = joinpath(data.tier[:jlhitch, category, period, run], format("{}-{}-{}-{}-{}-tier_jlhit.lh5", string(setup), string(period), string(run), string(category), string(ch)))
get_hitchfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) = get_hitchfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch)

"""
    get_mltrainfilename(data::LegendData, period::DataPeriodLike, category::DataCategoryLike)
Get the filename for the machine learning training data.
"""
function get_mltrainfilename end
export get_mltrainfilename
function get_mltrainfilename(data::LegendData, period::DataPeriodLike, category::DataCategoryLike)
    first_run = first(sort(filter(x -> x.period == DataPeriod(3), analysis_runs(data)).run, by=x->x.no))
    fk = start_filekey(data, (period, first_run, category))
    data.tier[:jlml, fk]
end
get_mltrainfilename(data::LegendData, filekey::FileKey) = get_mltrainfilename(data, filekey.period, filekey.category)



"""
    load_runch(open_func::Function, flatten_func::Function, data::LegendData, filekeys::Vector{FileKey}, tier::DataTierLike, ch::ChannelIdLike; check_filekeys::Bool=true)

Load data for a channel from a list of filekeys in a given tier.
# Arguments
- `open_func::Function`: function to open a file
- `flatten_func::Function`: function to flatten data
- `data::LegendData`: data object
- `filekeys::Vector{FileKey}`: list of filekeys
- `tier::DataTierLike`: tier to load data from
- `ch::ChannelIdLike`: channel to load data for
- `check_filekeys::Bool=true`: check if filekeys are valid
"""
function load_runch(open_func::Function, flatten_func::Function, data::LegendData, filekeys::Vector{FileKey}, tier::DataTierLike, ch::ChannelIdLike; check_filekeys::Bool=true)
    ch_filekeys = if check_filekeys
        @info "Check Filekeys"
        ch_filekeys = Vector{FileKey}()
        for fk in filekeys
            if !isfile(data.tier[tier, fk])
                @warn "File $(basename(data.tier[tier, fk])) does not exist, skip"
                continue
            end
            if !haskey(open_func(data.tier[tier, fk], "r"), "$ch")
                @warn "Channel $ch not found in $(basename(data.tier[tier, fk])), skip"
                continue
            end
            push!(ch_filekeys, fk)
        end
        ch_filekeys
    else
        filekeys
    end

    # check if any valid filekeys found
    if isempty(ch_filekeys)
        @error "No valid filekeys found, skip"
        throw(LoadError("FileKeys", 154,"No filekeys found for channel"))
    end

    @info "Read data for channel $ch from $(length(ch_filekeys)) files"
    # return fast-flattened data
    flatten_func([
            open_func(
                ds -> begin
                    # @debug "Reading from \"$(basename(data.tier[tier, fk]))\""
                    ds["$ch"][:]
                end,
                data.tier[tier, fk]
            ) for fk in ch_filekeys
        ])
end
export load_runch