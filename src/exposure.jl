# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

"""
    get_exposure(data::LegendData, det::DetectorIdLike, period::DataPeriodLike, run::DataRunLike; kwargs...)
    get_exposure(data::LegendData, det::DetectorIdLike, period::DataPeriodLike; kwargs...)
    get_exposure(data::LegendData, det::DetectorIdLike, partition::DataPartitionLike; kwargs...)

Calculates the exposure of a detector in a given run/period/partition.

# Arguments
- `data`: `LegendData` object with information on detector geometries and `runinfo` / `partitioninfo`
- `det` Detector for which the exposure is calculated
- `period`: `DataPeriod` for which the exposure is calculated
- `run`: `DataRun` for which the exposure is calculated
- `partition`: `DataPartition` for which the exposure is calculated

# Keyword Arguments
- `is_analysis_run`: If set to `true`, only the `runs` flagged as `is_analysis_phy_tun == true` are considered. Default is `true`.
- `cat` `DataCategory` for which the exposure is calculated. Default is `:phy`.`

# Returns
- `exposure`: the exposure of the detector `det` for the time given.


# Example
```julia
l200 = LegendData(:l200)
get_exposure(l200, :V00050A, DataPeriod(3), DataRun(0))
get_exposure(l200, :V00050A, DataPeriod(3))
get_exposure(l200, :V00050A, DataPartition(1))
````

"""
function get_exposure(data::LegendData, det::DetectorIdLike, period::DataPeriodLike, run::DataRunLike; kwargs...)
    rinfo = runinfo(data, period, run)
    get_exposure(data, det, rinfo; kwargs...)
end

Base.Broadcast.broadcasted(f::typeof(get_exposure), data::LegendData, detectors::Vector{<:DetectorIdLike}, period::DataPeriodLike, run::DataRunLike; kwargs...) = broadcast(det -> f(data, det, period, run; kwargs...), detectors)
get_exposure(data, det::Vector{<:DetectorIdLike}, period::DataPeriodLike, run::DataRunLike; kwargs...) = sum(get_exposure.(Ref(data), det, Ref(period), Ref(run); kwargs...))

function get_exposure(data::LegendData, det::DetectorIdLike, period::DataPeriod; kwargs...)
    rinfo = runinfo(data, period)
    get_exposure(data, det, rinfo; kwargs...)
end

function get_exposure(data::LegendData, det::DetectorIdLike, part::DataPartition; cat::DataCategoryLike=:phy, kwargs...)
    part_dict = partitioninfo(data, det)
    if haskey(part_dict, part)
        rinfo = partitioninfo(data, det, part; category=cat)
        return get_exposure(data, det, rinfo; cat=cat, kwargs...)
    end
    
    #default if partition does not exist
    return 0.0u"kg*yr"
end

function get_exposure(data::LegendData, det::DetectorIdLike, sel::Union{AbstractString, Symbol}; kwargs...)
    selectors = (DataPartition, DataPeriod)
    for SEL in selectors
        if _can_convert_to(SEL, sel)
            return get_exposure(data, det, SEL(sel); kwargs...)
        end
    end
    throw(ArgumentError("The selector $(sel) cannot be converted to type: $(selectors)"))
end

Base.Broadcast.broadcasted(f::typeof(get_exposure), data::LegendData, detectors::Vector{<:DetectorIdLike}, sel; kwargs...) = broadcast(det -> f(data, det, sel; kwargs...), detectors)
get_exposure(data, det::Vector{<:DetectorIdLike}, sel; kwargs...) = sum(get_exposure.(Ref(data), det, Ref(sel); kwargs...))

### TODO: determine livetimes from data files instead of metadata
function get_exposure(data::LegendData, det::DetectorIdLike, rinfo::Table; is_analysis_run::Bool=true, cat::DataCategoryLike=:phy, check_pf::PropertyFunction=@pf $detector == DetectorId(det))

    # check that the DataCategory is valid
    if !(_can_convert_to(DataCategory, cat) && hasproperty(rinfo, DataCategory(cat).label))
        throw(ArgumentError("Data category `$(cat)`` is invalid"))
    end
    cat_label::Symbol = Symbol(DataCategory(cat))

    # determine livetime
    rinfo_cat = getproperty(rinfo, cat_label) 
    exposure = 0.0u"kg * yr" 
    for r in rinfo_cat
        filekey = r.startkey
        chinfo = channelinfo(data, filekey, det, extended = true, verbose = false)
        if check_pf(chinfo)
            exposure += uconvert(u"kg * yr", chinfo.mass * r.livetime)
        end
    end   
    return exposure
end
  
export get_exposure