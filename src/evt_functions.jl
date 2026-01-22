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
    get_ged_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the Ge-detector detector data output PropertyFunction.
"""
function get_ged_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :geds).detdata_output)
end
export get_ged_evt_detdata_propfunc

"""
    get_ged_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the Ge-detector detector selection PropertyFunction.
"""
function get_ged_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :geds).detectors)
end
export get_ged_evt_detsel_propfunc

"""
    get_ged_evt_hitdetsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the hit Ge-detector detector selection PropertyFunction.
"""
function get_ged_evt_hitdetsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :geds).hitdetectors)
end
export get_ged_evt_hitdetsel_propfunc

"""
    get_ged_evt_is_valid_hit_properties(data::LegendData, sel::AnyValiditySelection)

Get the hit Ge-detector `is_valid_hit` selection properties.
"""
function get_ged_evt_is_valid_hit_properties(data::LegendData, sel::AnyValiditySelection)
    Symbol.(_dataprod_evt(data, sel, :geds).is_valid_hit_properties)
end
export get_ged_evt_is_valid_hit_properties


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
    get_aux_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the aux detector data output PropertyFunction.
"""
function get_aux_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    ljl_propfunc(_dataprod_evt(data, sel, :aux)[detector].detdata_output)
end
export get_aux_evt_detdata_propfunc

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
    get_aux_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the aux detector selection PropertyFunction.
"""
function get_aux_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :aux).detectors)
end
export get_aux_evt_detsel_propfunc



### SPMS

"""
    get_spms_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the SiPM-detector detector data output PropertyFunction.
"""
function get_spms_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :spms).detdata_output)
end
export get_spms_evt_detdata_propfunc

"""
    get_spms_evt_kwargs(data::LegendData, sel::AnyValiditySelection)

Get the SiPM-detector evt kwargs.
"""
function get_spms_evt_kwargs(data::LegendData, sel::AnyValiditySelection)
    kwargs = _dataprod_evt(data, sel, :spms).kwargs
    NamedTuple([(k, if v isa String Symbol(v) else v end) for (k, v) in pairs(kwargs)])
end
export get_spms_evt_kwargs

"""
    get_spms_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the SiPM detector selection PropertyFunction.
"""
function get_spms_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :spms).detectors)
end
export get_spms_evt_detsel_propfunc


"""
    get_spms_evt_lar_cut_props(data::LegendData, sel::AnyValiditySelection)

Get the SiPM LAr cut properties.
"""
function get_spms_evt_lar_cut_props(data::LegendData, sel::AnyValiditySelection)
    _dataprod_evt(data, sel, :spms).lar_cut
end
export get_spms_evt_lar_cut_props


### PMTS

"""
    get_pmts_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the PMT-detector detector data output PropertyFunction.
"""
function get_pmts_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :pmts).detdata_output)
end
export get_pmts_evt_detdata_propfunc

"""
    get_pmts_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the PMT detector selection PropertyFunction.
"""
function get_pmts_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :pmts).detectors)
end
export get_pmts_evt_detsel_propfunc


"""
    get_pmts_evt_kwargs(data::LegendData, sel::AnyValiditySelection)

Get the PMT evt kwargs.
"""
function get_pmts_evt_kwargs(data::LegendData, sel::AnyValiditySelection)
    kwargs = _dataprod_evt(data, sel, :pmts).kwargs
    NamedTuple([(k, if v isa String Symbol(v) else v end) for (k, v) in pairs(kwargs)])
end
export get_pmts_evt_kwargs

"""
    get_pmts_evt_muon_cut_props(data::LegendData, sel::AnyValiditySelection)

Get the PMT muon cut properties.
"""
function get_pmts_evt_muon_cut_props(data::LegendData, sel::AnyValiditySelection)
    _dataprod_evt(data, sel, :pmts).muon_cut
end
export get_pmts_evt_muon_cut_props


"""
    get_pmts_evt_evtdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the PMT-detector channel data output PropertyFunction.
"""
function get_pmts_evt_evtdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(get_pmts_evt_muon_cut_props(data, sel).evtdata_output)
end
export get_pmts_evt_evtdata_propfunc