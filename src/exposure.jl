# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

"""
    get_exposure(data::LegendData, det::DetectorIdLike, period::DataPeriodLike, run::DataRunLike; kwargs...)
    get_exposure(data::LegendData, det::DetectorIdLike, period::DataPeriod; kwargs...)
    get_exposure(data::LegendData, det::DetectorIdLike, partition::DataPartition; kwargs...)
    get_exposure(data::LegendData, det::DetectorIdLike, sel::Union{AbstractString, Symbol}; kwargs...)
    get_exposure(data::LegendData, det::DetectorIdLike, rinfo::Table; is_analysis_run::Bool = true, cat::DataCategoryLike = :phy, check_pf::PropertyFunction = @pf \$detector == DetectorId(det))
    get_exposure(data::LegendData, dets::Vector{<:DetectorIdLike}, args...; kwargs...)

Calculates the exposure of a detector in a given run, period, partition or run
table.

The exposure is the sum over the runs of the product of the detector mass and
the livetime of the runs (from `runinfo`), as a quantity in `kg * yr`. The mass
is taken from the channel information at the start file key of each run. For a
vector of detectors the exposures are summed. A `Symbol` or `String` as
selector is interpreted as a [`DataPartition`](@ref) or a
[`DataPeriod`](@ref). For a partition that does not exist for the detector the
exposure is zero.

## Arguments
* `data::LegendData`: Data with information on detector masses, `runinfo` and `partitioninfo`.
* `det::DetectorIdLike`: Detector for which the exposure is calculated.
* `period::DataPeriodLike`: Period for which the exposure is calculated.
* `run::DataRunLike`: Run for which the exposure is calculated.
* `partition::DataPartition`: Partition for which the exposure is calculated.
* `rinfo::Table`: Run information as returned by [`runinfo`](@ref).

## Keywords
* `is_analysis_run::Bool = true`: Accepted by the method for a run table; it does not change the result.
* `cat::DataCategoryLike = :phy`: Data category for which the exposure is calculated. For a partition, the category of the partition.
* `check_pf::PropertyFunction = @pf \$detector == DetectorId(det)`: Condition on the channel information of a run that has to be `true` for the run to count.

## Examples
```julia
using LegendDataManagement

l200 = LegendData(:l200)
get_exposure(l200, :V00050A, DataPeriod(3), DataRun(0))
get_exposure(l200, :V00050A, DataPeriod(3))
get_exposure(l200, :V00050A, DataPartition(:calgroup001a))
```

See also [`livetime`](@ref), [`partitioninfo`](@ref).
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

function get_exposure(data::LegendData, det::DetectorIdLike, part::DataPartition; kwargs...)
    part_dict = partitioninfo(data, det, part.cat)
    if haskey(part_dict, part)
        rinfo = partitioninfo(data, det, part)
        return get_exposure(data, det, rinfo; cat=part.cat, kwargs...)
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