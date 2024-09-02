# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const _cached_dataprod_evt = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_evt(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_dataprod_evt, key) do
        dataprod_config(data).evt(sel)
    end
end

function _dataprod_evt(data::LegendData, sel::AnyValiditySelection, system::Symbol)
    _dataprod_evt(data, sel)[system]
end


### HPGe

"""
    get_ged_evt_chdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the Ge-detector channel data output PropertyFunction.
"""
function get_ged_evt_chdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :geds).chdata_output)
end
export get_ged_evt_chdata_propfunc

"""
    get_ged_evt_chsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the Ge-detector channel selection PropertyFunction.
"""
function get_ged_evt_chsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :geds).channels)
end
export get_ged_evt_chsel_propfunc


"""
    get_ged_evt_kwargs(data::LegendData, sel::AnyValiditySelection)

Get the Ge-detector evt kwargs.
"""
function get_ged_evt_kwargs(data::LegendData, sel::AnyValiditySelection)
    kwargs = _dataprod_evt(data, sel, :geds).kwargs
    NamedTuple([(k, if v isa String Symbol(v) else v end) for (k, v) in pairs(kwargs)])
end
export get_ged_evt_kwargs



### aux


const _cached_dataprod_evt_puls_pf = LRU{Tuple{UInt, AnyValiditySelection, DetectorId}, PropertyFunction}(maxsize = 10^2)

"""
    get_aux_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the aux calibration function for the given data, validity selection
and the aux channel referred to by `detector`.
"""
function get_aux_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel, detector)
    get!(_cached_dataprod_evt_puls_pf, key) do
        ljl_propfunc(_dataprod_evt(data, sel, :aux)[detector].cal)
    end
end
export get_aux_cal_propfunc

"""
    get_aux_evt_chdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the aux channel data output PropertyFunction.
"""
function get_aux_evt_chdata_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    ljl_propfunc(_dataprod_evt(data, sel, :aux)[detector].chdata_output)
end
export get_aux_evt_chdata_propfunc

"""
    get_aux_evt_levelname_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the aux event level name.
"""
function get_aux_evt_levelname_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    Symbol(_dataprod_evt(data, sel, :aux)[detector].evt_levelname)
end
get_aux_evt_levelname_propfunc(data::LegendData, sel::AnyValiditySelection, channel::ChannelId) = get_aux_evt_levelname_propfunc(data, sel, channelinfo(data, sel, channel).detector)
export get_aux_evt_levelname_propfunc


"""
    get_aux_evt_chsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the aux channel selection PropertyFunction.
"""
function get_aux_evt_chsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :aux).channels)
end
export get_aux_evt_chsel_propfunc



### SPMS

"""
    get_spms_evt_chdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the Ge-detector channel data output PropertyFunction.
"""
function get_spms_evt_chdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :spms).chdata_output)
end
export get_spms_evt_chdata_propfunc

"""
    get_spms_evt_chsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the SiPM channel selection PropertyFunction.
"""
function get_spms_evt_chsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :spms).channels)
end
export get_spms_evt_chsel_propfunc

