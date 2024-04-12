# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const _cached_dataprod_evt = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_evt(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_dataprod_evt, key) do
        dataprod_config(data).evt(sel)
    end
end

function _dataprod_evt(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    get(_dataprod_evt(data, sel), detector, _dataprod_evt(data, sel).default)
end

const _cached_dataprod_evt_chdata_pf = LRU{Tuple{UInt, AnyValiditySelection}, PropertyFunction}(maxsize = 10^2)


"""
    get_ged_evt_chdata_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the Ge-detector channel data output PropertyFunction.
"""
function get_ged_evt_chdata_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel)
    get!(_cached_dataprod_evt_chdata_pf, key) do
        chdata_def_props = _dataprod_evt(data, sel, detector).chdata_output
        return ljl_propfunc(chdata_def_props)
    end
end
export get_ged_evt_chdata_propfunc


const _cached_dataprod_evt_puls_pf = LRU{Tuple{UInt, AnyValiditySelection}, PropertyFunction}(maxsize = 10^2)

"""
    get_pulser_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the pulser calibration function for the given data, validity selection
and the pulser channel referred to by `detector`.
"""
function get_pulser_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel)
    get!(_cached_dataprod_evt_puls_pf, key) do
        cal_def_props = _dataprod_evt(data, sel, detector).cal
        return ljl_propfunc(cal_def_props)
    end
end
export get_pulser_cal_propfunc