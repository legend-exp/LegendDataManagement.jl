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
function _writevalidity_impl(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, apply::Vector{String}; category::DataCategoryLike=:all, write_from_master::Bool=true)
    # write validity
    @lock _writevalidity_lock begin
        # get timestamp from filekey
        pars_validTimeStamp = string(filekey.time)
        # get validity filename and check if exists
        validity_filename = joinpath(data_path(props_db), validity_filename)
        mkpath(dirname(validity_filename))
        touch(validity_filename)
        # check if validity already written
        validity_lines = readlines(validity_filename)
        # check if given validity already exists
        is_validity = findall(x -> contains(x, "$pars_validTimeStamp") && contains(x, "$(string(category))"), validity_lines)
        if isempty(is_validity)
            if write_from_master @info "Write new validity for $pars_validTimeStamp" end
            push!(validity_lines, "{\"valid_from\":\"$pars_validTimeStamp\", \"category\":\"$(string(category))\", \"apply\":[\"$(join(sort(apply), "\", \""))\"]}")
        elseif length(is_validity) == 1
            if write_from_master @info "Merge old $pars_validTimeStamp $(string(category)) validity entry" end
            apply = unique(append!(Vector{String}(JSON.parse(validity_lines[first(is_validity)])["apply"]), apply))
            validity_lines[first(is_validity)] = "{\"valid_from\":\"$pars_validTimeStamp\", \"category\":\"$(string(category))\", \"apply\":[\"$(join(sort(apply), "\", \""))\"]}"
        end
        # write validity
        open(validity_filename, "w") do io
            for line in sort(validity_lines)
                println(io, line)
            end
        end
    end
end
writevalidity(props_db, filekey, apply::String; kwargs...) = writevalidity(props_db, filekey, [apply]; kwargs...)
writevalidity(props_db, filekey, rsel::Tuple{DataPeriod, DataRun}; kwargs...) = writevalidity(props_db, filekey, "$(first(rsel))/$(last(rsel)).yaml"; kwargs...)
writevalidity(props_db, filekey, part::DataPartition; kwargs...) = writevalidity(props_db, filekey, "$(part).yaml"; kwargs...)
function writevalidity(props_db::LegendDataManagement.MaybePropsDB, validity::StructVector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}; kwargs...)
    # get unique runs and periods for the individual entries 
    runsel = unique([(row.period, row.run) for row in validity])
    # write validity for each run and period
    for (period, run) in runsel
        val = filter(row -> row.period == period && row.run == run, validity)
        writevalidity(props_db, first(val.filekey), val.validity; kwargs...)
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
    get_partitionvalidity(data::LegendData, ch::ChannelIdLike, part::DataPartitionLike, cat::DataCategoryLike=:cal) -> Vector{@NamedTuple{period::DataPeriod, run::DataRun, validity::String}}

Get partition validity for a given channel and partition.
"""
function get_partitionvalidity(data::LegendData, ch::ChannelIdLike, det::DetectorIdLike, part::DataPartitionLike, cat::DataCategoryLike=:cal)
    # unpack
    ch, det, part = ChannelId(ch), DetectorId(det), DataPartition(part)
    # get partition validity
    partinfo = partitioninfo(data, ch, part; category=cat)
    Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}([(period = pinf.period, run = pinf.run, filekey = start_filekey(data, (pinf.period, pinf.run, cat)), validity = "$det/$(part).yaml") for pinf in partinfo])
end
export get_partitionvalidity

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
