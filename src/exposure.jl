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
function get_exposure(data::LegendData, det::DetectorIdLike, period::DataPeriodLike, run::DataRunLike; is_analysis_run::Bool=true, cat::DataCategoryLike=:phy)
    rinfo = runinfo(data, period, run)
    _get_exposure(data, det, rinfo, is_analysis_run, cat)
end

function get_exposure(data::LegendData, det::DetectorIdLike, period::DataPeriod; is_analysis_run::Bool=true, cat::DataCategoryLike=:phy)
    rinfo = runinfo(data, period)
    _get_exposure(data, det, rinfo, is_analysis_run, cat)
end

function get_exposure(data::LegendData, det::DetectorIdLike, part::DataPartition; is_analysis_run::Bool=true, cat::DataCategoryLike=:phy)
    part_dict = partitioninfo(data, det)
    if haskey(part_dict, part)
        rinfo = partitioninfo(data, det, part)
        return _get_exposure(data, det, rinfo, is_analysis_run, cat)
    end
    
    #default if partition does not exist
    return 0.0u"kg*yr"
end

function get_exposure(data::LegendData, det::DetectorIdLike, sel::Union{AbstractString, Symbol}; kwargs...)
    selectors = (DataPartition, DataPeriod)
    for SEL in selectors
        if _can_convert_to(SEL, sel)
            return _get_exposure(data, det, SEL(sel); kwargs...)
        end
    end
    throw(ArgumentError("The selector $(sel) cannot be converted to type: $(selectors)"))
end


### TODO: determine livetimes from data files instead of metadata
function _get_exposure(data::LegendData, det::DetectorIdLike, rinfo::Table, is_analysis_run::Bool=true, cat::DataCategoryLike=:phy)

    # check that the DataCategory is valid
    if !(_can_convert_to(DataCategory, cat) && hasproperty(rinfo, DataCategory(cat).label))
        throw(ArgumentError("Data category `$(cat)`` is invalid"))
    end
    cat_label::Symbol = Symbol(DataCategory(cat))

    # determine livetime
    rinfo_cat = getproperty(rinfo, cat_label)
    livetimes = getproperty.(rinfo_cat, :livetime)
    
    if is_analysis_run
        livetimes = livetimes .* getproperty(rinfo_cat, :is_analysis_run)
    end
    # sum up all livetimes (excluding NaN values)
    livetime = !isempty(livetimes) ? sum((livetimes .* .!isnan.(livetimes))) : 0.0u"s"

    # determine the mass of 76Ge
    filekeys = getproperty.(rinfo_cat, :startkey)
    mass = if !iszero(livetime) && !isempty(filekeys)
        # read in the channelinfo
        filekey = first(filekeys)
        _chinfo = channelinfo(data, filekey, det, extended = true, verbose = false)
        chinfo = if !all(x -> hasproperty(_chinfo, x), (:enrichment, :mass, :active_volume, :total_volume))
            empty!(_cached_channelinfo)
            channelinfo(data, filekey, det, extended = true, verbose = false)
        else
            _chinfo
        end
        # chinfo.active_volume == chinfo.total_volume && @warn "No FCCD value given for detector $(det)"
        chinfo.mass * chinfo.enrichment * chinfo.active_volume / chinfo.total_volume
    else
        0.0u"kg"
    end
    
    return uconvert(u"kg*yr", livetime * mass)
end

export get_exposure