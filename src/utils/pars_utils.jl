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
function nt2pd(pd::PropDict, nt::Union{NamedTuple, Dict})
    for k in keys(nt)
        if nt[k] isa NamedTuple || nt[k] isa Dict
            pd[k] = if !(haskey(pd, k)) || isnothing(pd[k]) PropDict() else pd[k] end
            nt2pd(pd[k], nt[k])
        else
            pd[k] = nt[k]
        end
    end
    pd
end

"""
    writevalidity(props_db::LegendDataManagement.PropsDB, filekey::FileKey; apply_to::Symbol=:all)
    writevalidity(props_db::LegendDataManagement.PropsDB, filekey::FileKey, part::DataPartitionLike; apply_to::Symbol=:all)
Write validity for a given filekey.
"""
function writevalidity end
export writevalidity
function writevalidity(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, apply::String; apply_to::DataCategoryLike=:all)
    # write validity
    # get timestamp from filekey
    pars_validTimeStamp = string(filekey.time)
    # get validity filename and check if exists
    validity_filename = joinpath(data_path(props_db), "validity.jsonl")
    mkpath(dirname(validity_filename))
    touch(validity_filename)
    # check if validity already written
    validity_entry = "{\"valid_from\":\"$pars_validTimeStamp\", \"category\":\"$(string(apply_to))\", \"apply\":[\"$(apply)\"]}"
    validity_lines = readlines(validity_filename)
    is_validity = findfirst(x -> contains(x, "$pars_validTimeStamp"), validity_lines)
    if isnothing(is_validity)
        @info "Write validity for $pars_validTimeStamp"
        open(validity_filename, "a") do io
            println(io, validity_entry)
        end
    else
        @info "Delete old $pars_validTimeStamp validity entry"
        validity_lines[is_validity] = validity_entry
        open(validity_filename, "w") do io
            for line in sort(validity_lines)
                println(io, line)
            end
        end
    end
end
writevalidity(props_db, filekey, rsel::RunSelLike; kwargs...) = writevalidity(props_db, filekey, "$(first(rsel))/$(last(rsel)).json"; kwargs...)
writevalidity(props_db, filekey, part::DataPartitionLike; kwargs...) = writevalidity(props_db, filekey, "$(part).json"; kwargs...)
function writevalidity(props_db::LegendDataManagement.MaybePropsDB, validity::StructVector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}; kwargs...)
    # get unique runs and periods for the individual entries 
    runsel = unique([(row.period, row.run) for row in validity])
    # write validity for each run and period
    for (period, run) in runsel
        val = filter(row -> row.period == period && row.run == run, validity)
        writevalidity(props_db, first(val.filekey), join(val.validity, "\", \""); kwargs...)
    end
end

"""
    create_validity(result) -> StructArray
Create a StructArray from a result of the parallel processing
"""
function create_validity(result)
    validity_all = Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}()
    for (_, res_ch) in result
        if haskey(res_ch, :validity)
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
    partinfo = partitioninfo(data, ch, part)
    Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}([(period = pinf.period, run = pinf.run, filekey = start_filekey(data, (pinf.period, pinf.run, cat)), validity = "$det/$(part).json") for pinf in partinfo])
end
export get_partitionvalidity

"""
    detector2channel(data::LegendData, sel::Union{AnyValiditySelection, RunCategorySelLike}, channel::Union{ChannelIdLike, DetectorIdLike}; kwargs...)
Get the channelID for a given detectorID or vice versa.
input: 
* `data``, e.g. `LegendData(:l200)``
* `runsel``: runselection, e.g. `(DataPeriod(3), DataRun(0), :cal)`
* `channel``: can be DetectorID e.g. `DetectorId(:P00573A)`` OR ChannelID e.g. `ChannelId(1080005)``
output:
* if `channel` is of type `ChannelID`, then out returns the corresponding `DetectorID`
* if `channel` is of type `DetectorID`, then out returns the corresponding `ChannelID`
"""
function detector2channel(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike}, channel::Union{ChannelIdLike, DetectorIdLike}; kwargs...)
    chinfo = channelinfo(data, runsel; kwargs...)
    if isa(channel, DetectorId)
        idx = findfirst(map(x-> x == channel, chinfo.detector))  
        return chinfo.channel[idx]  
    elseif isa(channel, ChannelId) 
        idx = findfirst(map(x-> x == channel, chinfo.channel))
        return chinfo.detector[idx]
    end
end

"""
    get_det_type(data::LegendData, det::DetectorIdLike)
Looks up the detector type for a given DetectorID.
"""
function detector_type(data::LegendData, det::DetectorIdLike)
    det_type = Symbol(data.metadata.hardware.detectors.germanium.diodes[det].type)
    return det_type
end

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
