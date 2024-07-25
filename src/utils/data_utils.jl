###################
# Data Access utils
###################

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
function load_runch end
export load_runch

function load_runch(open_func::Function, flatten_func::Function, data::LegendData, filekeys::Vector{FileKey}, tier::DataTierLike, ch::ChannelIdLike; check_filekeys::Bool=true, keys::Tuple=())
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

    if isempty(keys)
        @info "Read data for channel $ch from $(length(ch_filekeys)) files"
        # return fast-flattened data
        flatten_func([
                open_func(
                    ds -> begin
                        # @debug "Reading from \"$(basename(data.tier[tier, fk]))\""
                        ds[ch, tier][:]
                    end,
                    data.tier[tier, fk]
                ) for fk in ch_filekeys
            ])
    else
        @info "Read $keys for channel $ch from $(length(ch_filekeys)) files"
        # return fast-flattened data
        flatten_func([
                open_func(
                    ds -> begin
                        # @debug "Reading from \"$(basename(data.tier[tier, fk]))\""
                        Table(NamedTuple{keys}(map(k -> ds[ch, tier, k][:], keys)))
                    end,
                    data.tier[tier, fk]
                ) for fk in ch_filekeys
            ])
    end
end
function load_runch(open_func::Function, flatten_func::Function, data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, tier::DataTierLike, ch::ChannelIdLike; kwargs...)
    filekeys = search_disk(FileKey, data.tier[tier, category, period, run])
    load_runch(open_func, flatten_func, data, filekeys, tier, ch; kwargs...)
end
load_runch(open_func::Function, flatten_func::Function, data::LegendData, start_filekey::FileKey, tier::DataTierLike, ch::ChannelIdLike; kwargs...) = load_runch(open_func, flatten_func, data, start_filekey.period, start_filekey.run, start_filekey.category, tier, ch; kwargs...)

"""
    load_hitchfile(open_func::Function, data::LegendData, (period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike), ch::ChannelIdLike; append_filekeys::Bool=true, calibrate_energy::Bool=false, load_level::String=:dataQC)
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
function load_hitchfile(open_func::Function, data::LegendData, runsel::RunCategorySelLike, ch::ChannelIdLike; append_filekeys::Bool=true, calibrate_energy::Bool=false, load_level::Symbol=:dataQC)
    # unpack runsel
    period, run, category = runsel
    # load hit file at DataQC level
    data_ch_hit = open_func(data.tier[:jlhitch, category, period, run, ch])[ch, load_level][:]
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
    load_partitionch(open_func::Function, flatten_func::Function, data::LegendData, partinfo::StructVector, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; data_keys::Tuple=(), n_evts::Int=-1, select_random::Bool=false)
    load_partitionch(open_func::Function, flatten_func::Function, data::LegendData, part::DataPartition, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; kwargs...)
Load data for a channel from a partition. 
# Arguments
- `open_func::Function`: function to open a file
- `flatten_func::Function`: function to flatten data
- `data::LegendData`: data object
- `partinfo::StructVector`: partition info
- `tier::DataTierLike`: tier
- `cat::DataCategoryLike`: category
- `ch::ChannelIdLike`: channel
- `data_keys::Tuple=()`: data keys, empty tuple selects all keys
- `n_evts::Int=-1`: number of events, -1 selects all events
- `select_random::Bool=false`: select events randomly
# Return
- `Table`: data table with flattened events
"""
function load_partitionch(open_func::Function, flatten_func::Function, data::LegendData, partinfo::Table, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; data_keys::Tuple=(), n_evts::Int=-1, select_random::Bool=false)
    @assert !isempty(partinfo) "No partition info found"
    @assert n_evts > 0 || n_evts == -1 "Number of events must be positive"
    if isempty(data_keys)
        data_keys = keys(open_func(data.tier[tier, cat, partinfo.period[1], partinfo.run[1], ch])[ch, tier])
    end
    # check length of partinfo files
    run_length = Dict([
        open_func(
            ds -> begin
                @debug "Reading n-events from \"$(basename(data.tier[tier, cat, period, run, ch]))\""
                (period, run) => NamedTuple{data_keys}(map(k -> length(first(ds[ch, tier, k])), data_keys))
            end,
            data.tier[tier, cat, period, run, ch]
        ) for (period, run) in partinfo
    ])

    # function to select range for a given run length rl
    function get_key_evt_range(rl::Int)
        if n_evts == -1 || n_evts > rl
            1:rl
        elseif select_random
            rand(1:rl, n_evts)
        else
            1:n_evts
        end
    end
    
    # load event range to read
    evt_range = Dict([ 
        (period, run) => NamedTuple{data_keys}(map(k -> get_key_evt_range(run_length[(period, run)][k]), data_keys))
        for (period, run) in partinfo
    ])
    
    function get_key_evt_tbl(ds, period, run, k)
        if select_random
            t = Table(ds[ch, tier, k])[:]
            t[evt_range[(period, run)][k]]
        else
            Table(ds[ch, tier, k])[evt_range[(period, run)][k]]
        end
    end
    # load data with given range
    flatten_func([
            open_func(
                ds -> begin
                    @debug "Reading from \"$(basename(data.tier[tier, cat, period, run, ch]))\""
                    Table(NamedTuple{data_keys}(map(k -> get_key_evt_tbl(ds, period, run, k), data_keys)))
                end,
                data.tier[tier, cat, period, run, ch]
            ) for (period, run) in partinfo
        ])
end
load_partitionch(open_func::Function, flatten_func::Function, data::LegendData, part::DataPartition, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; kwargs...) = load_partitionch(open_func, flatten_func, data, partitioninfo(data, ch, part), tier, cat, ch; kwargs...)
export load_partitionch


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
    partinfo = partitioninfo(data, :default)[part]
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
function get_partition_firstRunPeriod(data::LegendData, part::DataPartitionLike, label::Union{Symbol, DataSelector}=:default)
    part = DataPartition(part)
    # get partition info
    partinfo = partitioninfo(data, label)[part]
    period = filter(row -> row.period == minimum(partinfo.period), partinfo).period[1]
    partition_period = partinfo[[p == period for p in partinfo.period]]
    run = filter(row -> row.run == minimum(partition_period.run), partition_period).run[1]
    partinfo, run, period
end
export get_partition_firstRunPeriod