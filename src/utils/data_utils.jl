###################
# Data Access utils
###################

"""
    get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    get_peaksfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) 
Get the filename for the peaks data for a given channel.
"""
function get_peaksfilename end
export get_peaksfilename
get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike) = joinpath(data.tier[:peaks, :cal, period, run], format("{}-{}-{}-{}-{}-tier_peaks.lh5", string(setup), string(period), string(run), string(category), string(ch)))
get_peaksfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) = get_peaksfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch)

"""
    get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    get_hitchfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike)
Get the filename for the hitch data for a given channel.
"""
function get_hitchfilename end
export get_hitchfilename
get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike) = joinpath(data.tier[:jlhitch, category, period, run], format("{}-{}-{}-{}-{}-tier_jlhit.lh5", string(setup), string(period), string(run), string(category), string(ch)))
get_hitchfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) = get_hitchfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch)

"""
    get_mltrainfilename(data::LegendData, period::DataPeriodLike, category::DataCategoryLike)
    get_mltrainfilename(data::LegendData, filekey::FileKey)
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


"""
    load_hitchfile(open_func::Function, data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike; append_filekeys::Bool=true, calibrate_energy::Bool=false, load_level::String="dataQC")
    load_hitchfile(open_func::Function, data::LegendData, filekey::FileKey, ch::ChannelIdLike; kwargs...)
Load data from a hitch file for a given channel.
# Arguments
- `open_func::Function`: function to open a file
- `data::LegendData`: data object
- `setup::ExpSetupLike`: setup
- `period::DataPeriodLike`: period
- `run::DataRunLike`: run
- `category::DataCategoryLike`: category
- `ch::ChannelIdLike`: channel
- `append_filekeys::Bool=true`: append filekey to data for each event
- `calibrate_energy::Bool=false`: calibrate energy with given energy calibration parameters
- `load_level::String="dataQC"`: load level
# Return
- `Table`: data table for given hit file
"""
function load_hitchfile(open_func::Function, data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike; append_filekeys::Bool=true, calibrate_energy::Bool=false, load_level::String="dataQC")
    # load hit file at DataQC level
    data_ch_hit = open_func(get_hitchfilename(data, setup, period, run, category, ch))["$ch/$load_level"][:]
    # append filekeys to data for each event
    data_ch_hit = if append_filekeys
        fks = search_disk(FileKey, data.tier[:jldsp, category, period, run])
        fk_timestamps = [f.time.unixtime*u"s" for f in fks]
        data_ch_hit_fks = broadcast(data_ch_hit.timestamp) do ts
            idx_fk = findfirst(x -> x > ts, fk_timestamps)
            if isnothing(idx_fk)
                fks[end]
            else
                fks[idx_fk-1]
            end
        end
        Table(StructVector(merge((filekey = data_ch_hit_fks,), columns(data_ch_hit))))
    else
        data_ch_hit
    end
    if calibrate_energy
        # get detector name 
        det = channelinfo(data, (period, run, category), ch).detector
        ecal_pars = data.par.rpars.ecal[period, run][det]
        # calibrate energy
        e_names = Symbol.(["$(string(k))_cal" for k in keys(ecal_pars)])
        Table(StructVector(merge(columns(data_ch_hit), columns(ljl_propfunc(Dict{Symbol, String}(
            e_names .=> [ecal_pars[k].cal.func for k in keys(ecal_pars)]
        )).(data_ch_hit)))))
    end
end
load_hitchfile(open_func::Function, data::LegendData, filekey::FileKey, ch::ChannelIdLike; kwargs...) = load_hitchfile(open_func, data, filekey.setup, filekey.period, filekey.run, filekey.category, ch; kwargs...)
export load_hitchfile


"""
    load_rawevt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::Int)
    load_rawevt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::UnitRange{Int})
Load data for a channel from a hitch file for a given selected event index or index range.
# Arguments
- `open_func::Function`: function to open a file
- `data::LegendData`: data object
- `ch::ChannelIdLike`: channel
- `data_hit::Table`: hitch data
- `sel_evt::Int/UnitRange{Int}`: selected event index
# Return
- `Table`: data table of raw events
"""
function load_rawevt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::Int)
    data_ch_evtIDs = open_func(data.tier[:raw, data_hit.filekey[sel_evt]])[ch].raw.eventnumber[:]
    open_func(data.tier[:raw, data_hit.filekey[sel_evt]])[ch].raw[findall(data_hit.eventID_fadc[sel_evt] .== data_ch_evtIDs)]
end

function load_rawevt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::UnitRange{Int})
    tbl_vec = map(unique(data_hit.filekey[sel_evt])) do fk
        data_ch_evtIDs = open_func(data.tier[:raw, fk])[ch].raw.eventnumber[:]
        idxs = reduce(vcat, broadcast(data_hit.eventID_fadc[sel_evt]) do x
            findall(x .== data_ch_evtIDs)
        end)
        open_func(data.tier[:raw, fk])[ch].raw[idxs]
    end
    Table(StructVector(vcat(tbl_vec...)))
end
export load_rawevt