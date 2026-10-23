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

Returns the `PropertyFunction` that defines the per-detector output data of
the Ge-detector events.

The function is built from the `detdata_output` entry of the `geds` section
of the event configuration (`evt`) of [`dataprod_config`](@ref) for the
validity selection `sel`.
"""
function get_ged_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :geds).detdata_output)
end
export get_ged_evt_detdata_propfunc

"""
    get_ged_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Returns the `PropertyFunction` that selects the Ge detectors of an event.

The function is built from the `detectors` entry of the `geds` section of the
event configuration (`evt`) of [`dataprod_config`](@ref) for the validity
selection `sel`.
"""
function get_ged_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :geds).detectors)
end
export get_ged_evt_detsel_propfunc

"""
    get_ged_evt_hitdetsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Returns the `PropertyFunction` that selects the Ge detectors with a hit in an
event.

The function is built from the `hitdetectors` entry of the `geds` section of
the event configuration (`evt`) of [`dataprod_config`](@ref) for the validity
selection `sel`.
"""
function get_ged_evt_hitdetsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :geds).hitdetectors)
end
export get_ged_evt_hitdetsel_propfunc

"""
    get_ged_evt_is_valid_hit_properties(data::LegendData, sel::AnyValiditySelection)

Returns the names of the hit properties that define a valid Ge-detector hit as
a vector of `Symbol`s.

The names are the `is_valid_hit_properties` entry of the `geds` section of the
event configuration (`evt`) of [`dataprod_config`](@ref) for the validity
selection `sel`.
"""
function get_ged_evt_is_valid_hit_properties(data::LegendData, sel::AnyValiditySelection)
    Symbol.(_dataprod_evt(data, sel, :geds).is_valid_hit_properties)
end
export get_ged_evt_is_valid_hit_properties


"""
    get_ged_evt_kwargs(data::LegendData, sel::AnyValiditySelection)

Returns the keyword arguments for the event building of the Ge detectors as a
`NamedTuple`.

The values are the `kwargs` entry of the `geds` section of the event
configuration (`evt`) of [`dataprod_config`](@ref) for the validity selection
`sel`; strings are converted to `Symbol`s.
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

Returns the calibration `PropertyFunction` for the auxiliary channel
`detector`.

The function is built from the `cal` entry of `detector` in the `aux` section
of the event configuration (`evt`) of [`dataprod_config`](@ref) for the
validity selection `sel`. The result is cached per `data`, `sel` and
`detector`; use a fresh `data` object if the configuration on disk may have
changed.
"""
function get_aux_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel, detector)
    get!(_cached_dataprod_evt_puls_pf, key) do
        ljl_propfunc(_dataprod_evt(data, sel, :aux)[detector].cal)
    end
end
export get_aux_cal_propfunc

"""
    get_aux_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Returns the `PropertyFunction` that defines the output data of the auxiliary
channel `detector` in an event.

The function is built from the `detdata_output` entry of `detector` in the
`aux` section of the event configuration (`evt`) of
[`dataprod_config`](@ref) for the validity selection `sel`.
"""
function get_aux_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    ljl_propfunc(_dataprod_evt(data, sel, :aux)[detector].detdata_output)
end
export get_aux_evt_detdata_propfunc

"""
    get_aux_evt_levelname_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    get_aux_evt_levelname_propfunc(data::LegendData, sel::AnyValiditySelection, channel::ChannelId)

Returns the name of the event level of the auxiliary channel `detector`, or of
the detector that `channel` belongs to, as a `Symbol`.

The name is the `evt_levelname` entry of the detector in the `aux` section of
the event configuration (`evt`) of [`dataprod_config`](@ref) for the validity
selection `sel`.
"""
function get_aux_evt_levelname_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    Symbol(_dataprod_evt(data, sel, :aux)[detector].evt_levelname)
end
get_aux_evt_levelname_propfunc(data::LegendData, sel::AnyValiditySelection, channel::ChannelId) = get_aux_evt_levelname_propfunc(data, sel, channelinfo(data, sel, channel).detector)
export get_aux_evt_levelname_propfunc


"""
    get_aux_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Returns the `PropertyFunction` that selects the auxiliary channels of an
event.

The function is built from the `detectors` entry of the `aux` section of the
event configuration (`evt`) of [`dataprod_config`](@ref) for the validity
selection `sel`.
"""
function get_aux_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :aux).detectors)
end
export get_aux_evt_detsel_propfunc



### SPMS

"""
    get_spms_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Returns the `PropertyFunction` that defines the per-detector output data of
the SiPM events.

The function is built from the `detdata_output` entry of the `spms` section
of the event configuration (`evt`) of [`dataprod_config`](@ref) for the
validity selection `sel`.
"""
function get_spms_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :spms).detdata_output)
end
export get_spms_evt_detdata_propfunc

"""
    get_spms_evt_kwargs(data::LegendData, sel::AnyValiditySelection)

Returns the keyword arguments for the event building of the SiPMs as a
`NamedTuple`.

The values are the `kwargs` entry of the `spms` section of the event
configuration (`evt`) of [`dataprod_config`](@ref) for the validity selection
`sel`; strings are converted to `Symbol`s.
"""
function get_spms_evt_kwargs(data::LegendData, sel::AnyValiditySelection)
    kwargs = _dataprod_evt(data, sel, :spms).kwargs
    NamedTuple([(k, if v isa String Symbol(v) else v end) for (k, v) in pairs(kwargs)])
end
export get_spms_evt_kwargs

"""
    get_spms_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Returns the `PropertyFunction` that selects the SiPM channels of an event.

The function is built from the `detectors` entry of the `spms` section of the
event configuration (`evt`) of [`dataprod_config`](@ref) for the validity
selection `sel`.
"""
function get_spms_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :spms).detectors)
end
export get_spms_evt_detsel_propfunc


"""
    get_spms_evt_lar_cut_props(data::LegendData, sel::AnyValiditySelection)

Returns the properties that define the liquid-argon (LAr) veto cut of the
SiPM events.

The properties are the `lar_cut` entry of the `spms` section of the event
configuration (`evt`) of [`dataprod_config`](@ref) for the validity selection
`sel`.
"""
function get_spms_evt_lar_cut_props(data::LegendData, sel::AnyValiditySelection)
    _dataprod_evt(data, sel, :spms).lar_cut
end
export get_spms_evt_lar_cut_props


### PMTS

"""
    get_pmts_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Returns the `PropertyFunction` that defines the per-detector output data of
the PMT events.

The function is built from the `detdata_output` entry of the `pmts` section
of the event configuration (`evt`) of [`dataprod_config`](@ref) for the
validity selection `sel`.
"""
function get_pmts_evt_detdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :pmts).detdata_output)
end
export get_pmts_evt_detdata_propfunc

"""
    get_pmts_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)

Returns the `PropertyFunction` that selects the PMT channels of an event.

The function is built from the `detectors` entry of the `pmts` section of the
event configuration (`evt`) of [`dataprod_config`](@ref) for the validity
selection `sel`.
"""
function get_pmts_evt_detsel_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(_dataprod_evt(data, sel, :pmts).detectors)
end
export get_pmts_evt_detsel_propfunc


"""
    get_pmts_evt_kwargs(data::LegendData, sel::AnyValiditySelection)

Returns the keyword arguments for the event building of the PMTs as a
`NamedTuple`.

The values are the `kwargs` entry of the `pmts` section of the event
configuration (`evt`) of [`dataprod_config`](@ref) for the validity selection
`sel`; strings are converted to `Symbol`s.
"""
function get_pmts_evt_kwargs(data::LegendData, sel::AnyValiditySelection)
    kwargs = _dataprod_evt(data, sel, :pmts).kwargs
    NamedTuple([(k, if v isa String Symbol(v) else v end) for (k, v) in pairs(kwargs)])
end
export get_pmts_evt_kwargs

"""
    get_pmts_evt_muon_cut_props(data::LegendData, sel::AnyValiditySelection)

Returns the properties that define the muon veto cut of the PMT events.

The properties are the `muon_cut` entry of the `pmts` section of the event
configuration (`evt`) of [`dataprod_config`](@ref) for the validity selection
`sel`.
"""
function get_pmts_evt_muon_cut_props(data::LegendData, sel::AnyValiditySelection)
    _dataprod_evt(data, sel, :pmts).muon_cut
end
export get_pmts_evt_muon_cut_props


"""
    get_pmts_evt_evtdata_propfunc(data::LegendData, sel::AnyValiditySelection)

Returns the `PropertyFunction` that defines the event-level output data of the
PMTs.

The function is built from the `evtdata_output` entry of the `muon_cut`
properties (see [`get_pmts_evt_muon_cut_props`](@ref)).
"""
function get_pmts_evt_evtdata_propfunc(data::LegendData, sel::AnyValiditySelection)
    ljl_propfunc(get_pmts_evt_muon_cut_props(data, sel).evtdata_output)
end
export get_pmts_evt_evtdata_propfunc