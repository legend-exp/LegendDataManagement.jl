###################
# Data Access utils
###################

"""
    load_run_ch(open_func::Function, flatten_func::Function, data::LegendData, filekeys::Vector{FileKey}, tier::DataTierLike, ch::ChannelIdLike; check_filekeys::Bool = true, keys::Tuple = ())
    load_run_ch(open_func::Function, flatten_func::Function, data::LegendData, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, tier::DataTierLike, ch::ChannelIdLike; kwargs...)
    load_run_ch(open_func::Function, flatten_func::Function, data::LegendData, start_filekey::FileKey, tier::DataTierLike, ch::ChannelIdLike; kwargs...)

Deprecated. Loads the data of a channel from a list of file keys in a given
tier; use [`read_ldata`](@ref) instead.

The method for a period, run and category (or the start file key of a run)
uses the file keys that [`search_disk`](@ref) finds in the directory of the
run.

## Arguments
* `open_func::Function`: Function to open a file.
* `flatten_func::Function`: Function to flatten the data of the files.
* `data::LegendData`: Data that holds the files.
* `filekeys::Vector{FileKey}`: File keys of the files to read.
* `tier::DataTierLike`: Tier to load data from.
* `ch::ChannelIdLike`: Channel to load data for.

## Keywords
* `check_filekeys::Bool = true`: Skip file keys whose file does not exist or does not contain the channel.
* `keys::Tuple = ()`: Names of the columns to read; an empty tuple reads the whole table.
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
    load_raw_evt(open_func::Function, data::LegendData, ch::ChannelIdLike, data_hit::Table, sel_evt::Union{UnitRange{Int}, Vector{Int}})

Deprecated. Loads the raw data of a channel for the selected events of a hit
table; use [`read_ldata`](@ref) instead.

## Arguments
* `open_func::Function`: Function to open a file.
* `data::LegendData`: Data that holds the files.
* `ch::ChannelIdLike`: Channel to read.
* `data_hit::Table`: Hit data with the columns `filekey` and `eventID_fadc`.
* `sel_evt`: Index or indices of the selected events in `data_hit`.

Returns the table of the raw events.
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
    load_partition_ch(open_func::Function, flatten_func::Function, data::LegendData, partinfo::Table, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; data_keys::Tuple = (), n_evts::Int = -1, select_random::Bool = false)
    load_partition_ch(open_func::Function, flatten_func::Function, data::LegendData, part::DataPartition, tier::DataTierLike, cat::DataCategoryLike, ch::ChannelIdLike; kwargs...)

Deprecated. Loads the data of a channel from a partition; use
[`read_ldata`](@ref) instead.

## Arguments
* `open_func::Function`: Function to open a file.
* `flatten_func::Function`: Function to flatten the data of the files.
* `data::LegendData`: Data that holds the files.
* `partinfo::Table`: Partition information with the columns `period` and `run`, as returned by [`partitioninfo`](@ref).
* `part::DataPartition`: Partition to read; its partition information is taken from `partitioninfo(data, ch, part)`.
* `tier::DataTierLike`: Tier to read.
* `cat::DataCategoryLike`: Category to read.
* `ch::ChannelIdLike`: Channel to read.

## Keywords
* `data_keys::Tuple = ()`: Names of the columns to read; an empty tuple selects all.
* `n_evts::Int = -1`: Number of events to read per run; `-1` selects all.
* `select_random::Bool = false`: Select the events randomly.

Returns the table with the flattened events.
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
    get_partitionfilekeys(data::LegendData, part::DataPartitionLike, tier::DataTierLike; only_good::Bool = true)

Returns the file keys of a partition that are found on disk, as a
`Vector{FileKey}`.

The file keys are searched in the directories of the runs of the partition
(see [`search_disk`](@ref)).

## Arguments
* `data::LegendData`: Data that holds the files.
* `part::DataPartitionLike`: Partition to search in.
* `tier::DataTierLike`: Tier to search in.

## Keywords
* `only_good::Bool = true`: Leave out the file keys of [`bad_filekeys`](@ref).

The method with an additional `category` argument after `tier` is deprecated;
the category is that of the partition.
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