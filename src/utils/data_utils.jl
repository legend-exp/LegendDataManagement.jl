###################
# Data Access utils
###################

"""
    load_run_ch(open_func::Function, flatten_func::Function, data::LegendData, filekeys::Vector{FileKey}, tier::DataTierLike, ch::ChannelIdLike; check_filekeys::Bool=true)

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
function load_run_ch end
export load_run_ch

function load_run_ch(open_func::Function, flatten_func::Function, data::LegendData, filekeys::Vector{FileKey}, tier::DataTierLike, ch::ChannelIdLike; check_filekeys::Bool=true, keys::Tuple=())
    Base.depwarn(
        "`load_run_ch` is deprecated, use `read_ldata` instead`.",
        ((Base.Core).Typeof(load_run_ch)).name.mt.name, force=true
    )
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
function load_run_ch(open_func::Function, flatten_func::Function, data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, tier::DataTierLike, ch::ChannelIdLike; kwargs...)
    filekeys = search_disk(FileKey, data.tier[tier, category, period, run])
    load_run_ch(open_func, flatten_func, data, filekeys, tier, ch; kwargs...)
end
load_run_ch(open_func::Function, flatten_func::Function, data::LegendData, start_filekey::FileKey, tier::DataTierLike, ch::ChannelIdLike; kwargs...) = load_run_ch(open_func, flatten_func, data, start_filekey.period, start_filekey.run, start_filekey.category, tier, ch; kwargs...)

"""
    load_raw_evt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::Int)
    load_raw_evt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::UnitRange{Int})
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
function load_raw_evt end
export load_raw_evt

function load_raw_evt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::Int)
    Base.depwarn(
        "`load_raw_evt` is deprecated, use `read_ldata` instead`.",
        ((Base.Core).Typeof(load_raw_evt)).name.mt.name, force=true
    )
    data_ch_evtIDs = open_func(data.tier[:raw, data_hit.filekey[sel_evt]])[ch].raw.eventnumber[:]
    open_func(data.tier[:raw, data_hit.filekey[sel_evt]])[ch].raw[findall(data_hit.eventID_fadc[sel_evt] .== data_ch_evtIDs)]
end

function load_raw_evt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::Union{UnitRange{Int}, Vector{Int}})
    Base.depwarn(
        "`load_raw_evt` is deprecated, use `read_ldata` instead`.",
        ((Base.Core).Typeof(load_raw_evt)).name.mt.name, force=true
    )
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
    load_partition_ch(open_func::Function, flatten_func::Function, data::LegendData, partinfo::StructVector, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; data_keys::Tuple=(), n_evts::Int=-1, select_random::Bool=false)
    load_partition_ch(open_func::Function, flatten_func::Function, data::LegendData, part::DataPartition, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; kwargs...)
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
function load_partition_ch(open_func::Function, flatten_func::Function, data::LegendData, partinfo::Table, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; data_keys::Tuple=(), n_evts::Int=-1, select_random::Bool=false)
    Base.depwarn(
        "`load_partition_ch` is deprecated, use `read_ldata` instead`.",
        ((Base.Core).Typeof(load_partition_ch)).name.mt.name, force=true
    )
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
load_partition_ch(open_func::Function, flatten_func::Function, data::LegendData, part::DataPartition, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; kwargs...) = load_partition_ch(open_func, flatten_func, data, partitioninfo(data, ch, part), tier, cat, ch; kwargs...)
export load_partition_ch


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
function get_partitionfilekeys(data::LegendData, part::DataPartitionLike, tier::DataTierLike; only_good::Bool=true)
    part = DataPartition(part)
    # get partition info
    partinfo = partitioninfo(data, :default, part.cat)[part]
    found_filekeys = [filekey for (period, run) in partinfo if is_analysis_run(data, period, DataRun(run.no +1)) for filekey in search_disk(FileKey, data.tier[tier, part.cat, period, run])]
    found_filekeys = if only_good
        filter(Base.Fix2(!in, bad_filekeys(data)), found_filekeys)
    else
        found_filekeys
    end
    found_filekeys
end
export get_partitionfilekeys
@deprecate get_partitionfilekeys(data::LegendData, part::DataPartitionLike, tier::DataTierLike, category::DataCategoryLike; only_good::Bool=true) get_partitionfilekeys(data, part, tier; only_good)