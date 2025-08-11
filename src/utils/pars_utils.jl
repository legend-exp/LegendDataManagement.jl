"""
    create_pars(pd::PropDict, result::Dict{ChannelInfo, ChannelResult}) -> PropDict
Create a PropDict from a result of the parallel processing
"""
function create_pars(pd::PropDict, result)
    for (chinfo_ch, res_ch) in result
        if haskey(res_ch, :result)
            if  res_ch.processed isa Dict || res_ch.processed
                det = chinfo_ch.detector
                pd_det = ifelse(haskey(pd, det), pd[det], PropDict())
                pd_det = nt2pd(pd_det, res_ch.result)
                pd[det] = pd_det
            end
        end
    end
    pd
end
export create_pars

# convert NamedTuple to PropDict
function nt2pd(pd::PropDict, nt::Union{NamedTuple, Dict, PropDict})
    for k in keys(nt)
        if nt[k] isa NamedTuple || nt[k] isa Dict || nt[k] isa PropDict
            pd[k] = if !(haskey(pd, k)) || isnothing(pd[k]) PropDict() else pd[k] end
            nt2pd(pd[k], nt[k])
        else
            pd[k] = nt[k]
        end
    end
    pd
end

"""
    writevalidity(props_db::LegendDataManagement.PropsDB, filekey::FileKey; category::Symbol=:all)
    writevalidity(props_db::LegendDataManagement.PropsDB, filekey::FileKey, part::DataPartitionLike; category::Symbol=:all)
Write validity for a given filekey.
"""
function writevalidity end
export writevalidity
function writevalidity(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, apply::Vector{String}; category::DataCategoryLike=:all, mode::AbstractString = "reset")
    Distributed.remotecall_fetch(_writevalidity_impl, 1, props_db, filekey, apply; category=category, mode=mode, write_from_master= (Distributed.myid() == 1))
end
const _writevalidity_lock = ReentrantLock()
function _writevalidity_impl(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, apply::Vector{String};
    category::DataCategoryLike = :all, mode::AbstractString = "reset", write_from_master::Bool = true)

    @lock _writevalidity_lock begin
        # locate & create file if missing
        dst = joinpath(data_path(props_db), "validity.yaml")
        mkpath(dirname(dst))
        if !isfile(dst)
            open(dst, "w") do io
                println(io, "[]")
            end
        end

        # load or initialize the vector of entries
        raw     = YAML.load_file(dst, dicttype = OrderedDict{String, Any})
        entries::Vector{OrderedDict{String,Any}} = raw isa Vector ? raw : OrderedDict{String,Any}[]
        ts = string(filekey.time)
        # drop any previous entry with the same timestamp
        filter!(e -> !(e["valid_from"] == ts && e["mode"] == mode), entries)
        # build an OrderedDict containing the validity to be saved
        push!(entries, OrderedDict{String,Any}(
            "valid_from" => ts,
            "apply" => apply,
            "category" => category,
            "mode" => mode
        ))
        
        write_from_master && @info "Write validity for $ts"
        # sort the entries by timestamp
        sort!(entries, by = e -> e["valid_from"])
        
        # create a new file with the same filename with updated entries
        ParallelProcessingTools.write_files(dst; mode=CreateOrReplace()) do tmpfile
            open(tmpfile, "w") do io
                YAML.write(io, entries)
            end
        end
        return nothing
    end
end
writevalidity(props_db, filekey, apply::String; kwargs...) = writevalidity(props_db, filekey, [apply]; kwargs...)
writevalidity(props_db, filekey, rsel::Tuple{DataPeriod, DataRun}; kwargs...) = writevalidity(props_db, filekey, "$(first(rsel))/$(last(rsel)).yaml"; kwargs...)
writevalidity(props_db, filekey, part::DataPartition; kwargs...) = writevalidity(props_db, filekey, "$(part).yaml"; kwargs...)
function writevalidity(
    props_db::LegendDataManagement.MaybePropsDB,
    validity_with_flag::NamedTuple{(:result, :skipped)};
    category::DataCategoryLike = :all
)
    validity = validity_with_flag.result

    # Group by FileKey timestamp
    ts_to_validities = Dict{Timestamp, Set{String}}()
    ts_to_filekey = Dict{Timestamp, FileKey}()

    for row in validity
        ts = row.filekey.time
        val = row.validity
        push!(get!(ts_to_validities, ts, Set{String}()), val)
        ts_to_filekey[ts] = row.filekey
    end

    # Sort timestamps
    timestamps = sort(collect(keys(ts_to_validities)))

    prev_validities = Set{String}()
    first = true

    for ts in timestamps
        current_validities = ts_to_validities[ts]
        fk = ts_to_filekey[ts]

        if first # First timestamp: reset
            mode = validity_with_flag.skipped ? "append" : "reset"
            writevalidity(props_db, fk, sort(collect(current_validities)); category=category, mode=mode)
            prev_validities = deepcopy(current_validities)
            first = false
            continue
        end

        if current_validities == prev_validities # No change -> skip writing validity
            continue
        end

        added = setdiff(current_validities, prev_validities)
        removed = setdiff(prev_validities, current_validities)

        if !isempty(added) && isempty(removed)
            writevalidity(props_db, fk, sort(collect(added)); category=category, mode="append")
        elseif isempty(added) && !isempty(removed)
            writevalidity(props_db, fk, sort(collect(removed)); category=category, mode="remove")
        elseif length(added) == 1 && length(removed) == 1
            writevalidity(props_db, fk, [only(removed), only(added)]; category=category, mode="replace")
        elseif length(added) + length(removed) <= 10
            # split state into small append and remove block instead of using multiple "replace" or big "reset"
            writevalidity(props_db, fk, sort(collect(removed)); category=category, mode="remove")
            writevalidity(props_db, fk, sort(collect(added)); category=category, mode="append")
        else
            # Fallback to reset: write full current state
            writevalidity(props_db, fk, sort(collect(current_validities)); category=category, mode="reset")
        end

        prev_validities = deepcopy(current_validities)
    end
end

"""
    create_validity(result) -> StructArray
Create a StructArray from a result of the parallel processing
"""
function create_validity(result)
    validity_all = Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}()
    skipped = false
    for (_, res_ch) in result
        skipped |= get(res_ch, :skipped, false)  # Set to true if any channel was skipped
        if haskey(res_ch, :validity) && !get(res_ch, :skipped, false)
            append!(validity_all, res_ch.validity)
        end
    end
    return (result = StructArray(validity_all), skipped = skipped)
end
export create_validity

"""
    get_partitionvalidity(data::LegendData, part::DataPartitionLike) -> Vector{@NamedTuple{period::DataPeriod, run::DataRun, validity::String}}

Get partition validity for a given channel and partition.
"""
function get_partitionvalidity(data::LegendData, det::DetectorIdLike, part::DataPartitionLike)
    # unpack
    det, part = DetectorId(det), DataPartition(part)
    # get partition validity
    partinfo = partitioninfo(data, det, part)
    Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}([(period = pinf.period, run = pinf.run, filekey = start_filekey(data, (pinf.period, pinf.run, part.cat)), validity = "$det/$(part).yaml") for pinf in partinfo])
end
export get_partitionvalidity
# dropped unecessaty ch argument, keep old function functionality for now
@deprecate get_partitionvalidity(data::LegendData, ch::ChannelIdLike, det::DetectorIdLike, part::DataPartitionLike, cat::DataCategoryLike=:cal) get_partitionvalidity(data, det, part)

"""
    detector2channel(data::LegendData, sel::Union{AnyValiditySelection, RunCategorySelLike}, detector::DetectorIdLike)
Get the ChannelId for a given detectorId 
input: 
* `data`, e.g. `LegendData(:l200)``
* `runsel`: runselection, e.g. `(DataPeriod(3), DataRun(0), :cal)`
* `detector`: DetectorID e.g. `DetectorId(:P00573A)`` OR ChannelID e.g. `ChannelId(1080005)``
output:
* `ChannelId` of corresponding detector
"""
function detector2channel(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike}, detector::DetectorIdLike)
    return channelinfo(data, runsel, detector).channel
end
export detector2channel

"""
    channel2detector(data::LegendData, sel::Union{AnyValiditySelection, RunCategorySelLike}, channel::ChannelIdLike)
Get the DetectorId for a given ChannelId
input: 
* `data`, e.g. `LegendData(:l200)``
* `runsel`: runselection, e.g. `(DataPeriod(3), DataRun(0), :cal)`
* `channel`: ChannelId e.g. `ChannelId(1080005)``
output:
* `DetectorId` of corresponding channel
"""
function channel2detector(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike}, channel::ChannelIdLike)
    return channelinfo(data, runsel, channel).detector
end
export channel2detector

"""
    get_det_type(data::LegendData, det::DetectorIdLike)
Looks up the detector type for a given DetectorID.
"""
function detector_type(data::LegendData, det::DetectorIdLike)
    det_type = Symbol(data.metadata.hardware.detectors.germanium.diodes[det].type)
    return det_type
end
export detector_type

"""
    data_starttime(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike})
Extract startime as DateTime from file for a given run selection
    Input:
    * data: LegendData, e.g. LegendData(:l200)
    * runsel: runselection, e.g. (DataPeriod(3), DataRun(0), :cal)
"""
function data_starttime(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike})
    filekey = start_filekey(data, runsel)
    startdate = DateTime(filekey.time)
    return startdate
end
export data_starttime
