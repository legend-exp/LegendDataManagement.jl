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
function writevalidity(props_db::LegendDataManagement.PropsDB, filekey::FileKey; apply_to::Symbol=:all)
    # write validity
    # get timestamp from filekey
    pars_validTimeStamp = string(filekey.time)
    # get validity filename and check if exists
    validity_filename = joinpath(data_path(props_db), "validity.jsonl")
    mkpath(dirname(validity_filename))
    touch(validity_filename)
    # check if validity already written
    validity_entry = "{\"valid_from\":\"$pars_validTimeStamp\", \"category\":\"$(string(apply_to))\", \"apply\":[\"$(filekey.period)/$(filekey.run).json\"]}"
    has_validity = any([contains(ln, validity_entry) for ln in eachline(open(validity_filename, "r"))])
    if !has_validity
        @info "Write validity for $pars_validTimeStamp"
        open(validity_filename, "a") do io
            println(io, validity_entry)
        end
    else
        @info "Validity for $pars_validTimeStamp already written"
    end
end

function writevalidity(props_db::LegendDataManagement.PropsDB, filekey::FileKey, part::DataPartitionLike; apply_to::Symbol=:all)
    # write validity
    # get timestamp from filekey
    pars_validTimeStamp = string(filekey.time)
    # get validity filename and check if exists
    validity_filename = joinpath(data_path(props_db), "validity.jsonl")
    mkpath(dirname(validity_filename))
    touch(validity_filename)
    # check if validity already written
    validity_entry = "{\"valid_from\":\"$pars_validTimeStamp\", \"category\":\"$(string(apply_to))\", \"apply\":[\"$(part).json\"]}"
    has_validity = any([contains(ln, validity_entry) for ln in eachline(open(validity_filename, "r"))])
    if !has_validity
        @info "Write validity for $pars_validTimeStamp"
        open(validity_filename, "a") do io
            println(io, validity_entry)
        end
    else
        @info "Validity for $pars_validTimeStamp already written"
    end
end

"""
    det2ch(data::LegendData, det::DetectorIdLike; period::DataPeriodLike = DataPeriod(3), run::DataRunLike = DataRun(0))
Get the channelID for a given detectorID.
input: 
* data, e.g. LegendData(:l200)
* det: detectorID, e.g. DetectorId(:P00573A)
output:
* channelID
"""
function det2ch(data::LegendData, det::DetectorIdLike; period::DataPeriodLike = DataPeriod(3), run::DataRunLike = DataRun(0))
    filekey = start_filekey(data, (period, run , :cal)) 
    chinfo =  channelinfo(data, filekey; system = :geds)
    chidx = findfirst(map(x-> x == det, chinfo.detector))
    return chinfo.channel[chidx]
end

"""
    get_det_type(data::LegendData, det::DetectorIdLike)
Looks up the detector type for a given DetectorID.
"""
function get_det_type(data::LegendData, det::DetectorIdLike)
    det_type = Symbol(data.metadata.hardware.detectors.germanium.diodes[det].type)
    return det_type
end

function get_starttime(data::LegendData; period::DataPeriodLike = DataPeriod(3), run::DataRunLike = DataRun(0))
    filekey = start_filekey(data, (period, run, :cal))
    startdate = DateTime(filekey.time)
    return startdate
end
