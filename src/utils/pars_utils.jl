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
function writevalidity(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, apply::Vector{String}; category::DataCategoryLike=:all)
    Distributed.remotecall_fetch(_writevalidity_impl, 1, props_db, filekey, apply; category=category, write_from_master= (Distributed.myid() == 1))
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
        raw     = YAML.load_file(dst)
        entries = raw isa Vector ? raw : Any[]

        ts = string(filekey.time)

        # drop any previous entry with the same timestamp
        filter!(e -> e["valid_from"] != ts, entries)

        # build a Dict containing the validity to be saved
        entry = Dict{String,Any}()
        entry["valid_from"] = ts
        entry["apply"]      = apply
        entry["category"]   = category
        entry["mode"]       = mode
        push!(entries, entry)
        
        write_from_master && @info "Write validity for $ts"

        # sort the entries by timestamp
        sort!(entries, by = e -> e["valid_from"])

        function write_entries_ordered(dst, entries)
            open(dst, "w") do io
                for e in entries
                    println(io, "- valid_from: ", e["valid_from"])

                    println(io, "  apply:")
                    for path in e["apply"]
                        println(io, "    - ", path)
                    end

                    println(io, "  category: ", e["category"])
                    println(io, "  mode: ", e["mode"])
                    println(io)  # newline between entries
                end
            end
        end

        # emit valid YAML, with each Dict’s keys in insertion order
        write_entries_ordered(dst, entries)
    end
end
writevalidity(props_db, filekey, apply::String; kwargs...) = writevalidity(props_db, filekey, [apply]; kwargs...)
writevalidity(props_db, filekey, rsel::Tuple{DataPeriod, DataRun}; kwargs...) = writevalidity(props_db, filekey, "$(first(rsel))/$(last(rsel)).yaml"; kwargs...)
writevalidity(props_db, filekey, part::DataPartition; kwargs...) = writevalidity(props_db, filekey, "$(part).yaml"; kwargs...)
function writevalidity(props_db::LegendDataManagement.MaybePropsDB, validity::StructVector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}; kwargs...)
    # Dictionary to store the best (earliest) row per unique validity string
    best_by_validity = Dict{String, NamedTuple}()

    # Iterate over all validity entries; keep smallest timestamp for each unique "apply"
    for row in validity
        key = row.validity
        if haskey(best_by_validity, key)
            if row.filekey.time < best_by_validity[key].filekey.time
                best_by_validity[key] = row
            end
        else
            best_by_validity[key] = row
        end
    end

    # Sort the selected rows by timestamp for chronological writing
    sorted_rows = sort(collect(values(best_by_validity)), by = r -> r.filekey.time)

    # Write validity for each unique validity string with smallest timestamp
    for row in sorted_rows
        writevalidity(props_db, row.filekey, row.validity; kwargs...)
    end
end

"""
    create_validity(result) -> StructArray
Create a StructArray from a result of the parallel processing
"""
function create_validity(result)
    validity_all = Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}()
    for (_, res_ch) in result
        if haskey(res_ch, :validity) && !get(res_ch, :skipped, false)
            append!(validity_all, res_ch.validity)
        end
    end
    StructArray(validity_all)
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
    Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}([(period = pinf.period, run = pinf.run, filekey = start_filekey(data, (pinf.period, pinf.run, cat)), validity = "$det/$(part).yaml") for pinf in partinfo])
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
