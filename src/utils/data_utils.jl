###################
# Data Access utils
###################

"""
    get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    get_peaksfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) 
Get the filename for the peaks data for a given channel.
"""
function get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    Base.depwarn(
        "`get_peaksfilename(data, setup, period, run, category, ch)` is deprecated, use `l200.tier[:peaks, category, period, run, ch]` instead`.",
        ((Base.Core).Typeof(get_peaksfilename)).name.mt.name, force=true
    )
    # joinpath(data.tier[:peaks, :cal, period, run], format("{}-{}-{}-{}-{}-tier_peaks.lh5", string(setup), string(period), string(run), string(category), string(ch)))
    data.tier[:peaks, category, period, run, ch]
end
export get_peaksfilename
get_peaksfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) = get_peaksfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch)

"""
    get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    get_hitchfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike)
Get the filename for the hitch data for a given channel.
"""
function get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    Base.depwarn(
        "`get_hitchfilename(data, setup, period, run, category, ch)` is deprecated, use `l200.tier[:jlhitch, category, period, run, ch]` instead`.",
        ((Base.Core).Typeof(get_hitchfilename)).name.mt.name, force=true
    )
    # joinpath(data.tier[:jlhitch, category, period, run], format("{}-{}-{}-{}-{}-tier_jlhit.lh5", string(setup), string(period), string(run), string(category), string(ch)))
    data.tier[:jlhitch, category, period, run, ch]
end
export get_hitchfilename

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
    load_hitchfile(open_func::Function, data::LegendData, (period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike), ch::ChannelIdLike; append_filekeys::Bool=true, calibrate_energy::Bool=false, load_level::String="dataQC")
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
function load_hitchfile(open_func::Function, data::LegendData, runsel::RunCategorySelLike, ch::ChannelIdLike; append_filekeys::Bool=true, calibrate_energy::Bool=false, load_level::String="dataQC")
    # unpack runsel
    period, run, category = runsel
    # load hit file at DataQC level
    data_ch_hit = open_func(data.tier[:jlhitch, category, period, run, ch])["$ch/$load_level"][:]
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
load_hitchfile(open_func::Function, data::LegendData, filekey::FileKey, ch::ChannelIdLike; kwargs...) = load_hitchfile(open_func, data, (filekey.period, filekey.run, filekey.category), ch; kwargs...)
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
function load_rawevt end
export load_rawevt

function load_rawevt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::Int)
    data_ch_evtIDs = open_func(data.tier[:raw, data_hit.filekey[sel_evt]])[ch].raw.eventnumber[:]
    open_func(data.tier[:raw, data_hit.filekey[sel_evt]])[ch].raw[findall(data_hit.eventID_fadc[sel_evt] .== data_ch_evtIDs)]
end

function load_rawevt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::Union{UnitRange{Int}, Vector{Int}})
    tbl_vec = map(unique(data_hit.filekey[sel_evt])) do fk
        data_ch_evtIDs = open_func(data.tier[:raw, fk])[ch].raw.eventnumber[:]
        idxs = reduce(vcat, broadcast(data_hit.eventID_fadc[sel_evt]) do x
            findall(x .== data_ch_evtIDs)
        end)
        open_func(data.tier[:raw, fk])[ch].raw[idxs]
    end
    Table(StructVector(vcat(tbl_vec...)))
end


"""
    get_partitionfilekeys(data::LegendData, part::DataPartitionLike, tier::DataTierLike, category::DataCategoryLike; only_good::Bool=true)
Get filekeys for a given partition.
# Arguments
- `data::LegendData`: data object
- `part::DataPartitionLike`: partition to be searched in
- `tier::DataTierLike`: tier
- `category::DataCategoryLike`: category
- `only_good::Bool=true`: only get good filekeys
# Return
- `Vector{FileKey}`: filekeys
"""
function get_partitionfilekeys(data::LegendData, part::DataPartitionLike, tier::DataTierLike, category::DataCategoryLike; only_good::Bool=true)
    part = DataPartition(part)
    # get partition info
    partinfo = partitioninfo(data)[part]
    found_filekeys = [filekey for (period, run) in partinfo if is_analysis_run(data, period, DataRun(run.no +1)) for filekey in search_disk(FileKey, data.tier[tier, category, period, run])]
    found_filekeys = if only_good
        filter(Base.Fix2(!in, bad_filekeys(data)), found_filekeys)
    else
        found_filekeys
    end
    found_filekeys
end
export get_partitionfilekeys


"""
    get_partition_firstRunPeriod(data::LegendData, part::DataPartitionLike)
Get the first run and period for a given partition.
    # Returns 
- `partinfo::Table`: partition info
- `run::DataRun`: first run
- `period::DataPeriod`: first period
"""
function get_partition_firstRunPeriod(data::LegendData, part::DataPartitionLike)
    part = DataPartition(part)
    # get partition info
    partinfo = partitioninfo(data)[part]
    period = filter(row -> row.period == minimum(partinfo.period), partinfo).period[1]
    partition_period = partinfo[[p == period for p in partinfo.period]]
    run = filter(row -> row.run == minimum(partition_period.run), partition_period).run[1]
    partinfo, run, period
end
export get_partition_firstRunPeriod