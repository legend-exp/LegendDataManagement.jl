# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const _cached_get_ecal_props = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_ecal_props(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_get_ecal_props, key) do
        get_values(dataprod_parameters(data).rpars.ecal(sel))
    end
end

function _get_ecal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    _get_ecal_props(data, sel)[Symbol(detector)]
end

function _get_e_cal_propsfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol)
    ecal_props::String = get(get(get(_get_ecal_props(data, sel, detector), e_filter, PropDict()), :cal, PropDict()), :func, "0keV / 0")
    return ecal_props
end

const _cached_get_aoecal_props = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_aoecal_props(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_get_aoecal_props, key) do
        get_values(dataprod_parameters(data).ppars.aoe(sel))
    end
end

function _get_aoecal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    _get_aoecal_props(data, sel)[Symbol(detector)]
end

function _get_aoe_cal_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    psdcal_props::String = get(_get_aoecal_props(data, sel, detector), :func, "0 / 0")
    return psdcal_props
end

const _cached_dataprod_ged_cal = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_ged_cal(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_dataprod_ged_cal, key) do
        dataprod_config(data).energy(sel)
    end
end

function _dataprod_ged_cal(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    merge(_dataprod_ged_cal(data, sel).default, get(_dataprod_ged_cal(data, sel), detector, PropDict()))
end

"""
    get_ged_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the HPGe calibration function for the given data, validity selection and
detector.

Note: Caches configuration/calibration data internally, use a fresh `data`
object if on-disk configuration/calibration data may have changed.
"""
function get_ged_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    let energies = Symbol.(_dataprod_ged_cal(data, sel, detector).energies)
        energies_cal = Symbol.(_dataprod_ged_cal(data, sel, detector).energies .* "_cal")

        ljl_propfunc(Dict{Symbol, String}(
            append!(energies_cal, [:aoe_classifier]) .=>
                append!(_get_e_cal_propsfunc_str.(Ref(data), Ref(sel), Ref(detector), energies), [_get_aoe_cal_propfunc_str(data, sel, detector)])
        ))
    end
end
export get_ged_cal_propfunc


const _cached_dataprod_qc = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_qc(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_dataprod_qc, key) do
        dataprod_config(data).qc(sel)
    end
end

function _dataprod_qc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    merge(_dataprod_qc(data, sel).default, get(_dataprod_qc(data, sel), detector, PropDict()))
end

const _cached_dataprod_qc_cuts_pf = LRU{Tuple{UInt, AnyValiditySelection}, PropertyFunction}(maxsize = 10^2)


"""
    get_ged_qc_cuts_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the Ge-detector QC cut definitions for the given data and validity selection.
"""
function get_ged_qc_cuts_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel)
    get!(_cached_dataprod_qc_cuts_pf, key) do
        cut_def_props = _dataprod_qc(data, sel, detector).labels
        return ljl_propfunc(cut_def_props)
    end
end
export get_ged_qc_cuts_propfunc

const _cached_dataprod_is_trig_pf = LRU{Tuple{UInt, AnyValiditySelection}, PropertyFunction}(maxsize = 10^2)

"""
    get_ged_qc_istrig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the Ge-detector trigger condition for the given data and validity selection.
"""
function get_ged_qc_is_trig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel)
    get!(_cached_dataprod_is_trig_pf, key) do
        is_trig_def_props = _dataprod_qc(data, sel, detector).is_trig
        return ljl_propfunc(is_trig_def_props)
    end
end
export get_ged_qc_is_trig_propfunc

const _cached_dataprod_is_physical_pf = LRU{Tuple{UInt, AnyValiditySelection}, PropertyFunction}(maxsize = 10^2)

"""
    get_ged_qc_is_physical_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get a `PropertyFunction` that returns `true` for events that fullfill the `is_physical` definition.
"""
function get_ged_qc_is_physical_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel)
    get!(_cached_dataprod_is_physical_pf, key) do
        is_physical_def_props = _dataprod_qc(data, sel, detector).is_physical
        return ljl_propfunc(is_physical_def_props)
    end
end
export get_ged_qc_is_physical_propfunc

const _cached_dataprod_is_baseline_pf = LRU{Tuple{UInt, AnyValiditySelection}, PropertyFunction}(maxsize = 10^2)

"""
    get_ged_qc_is_baseline_propfunc(data::LegendData, sel::AnyValiditySelection)

Get a `PropertyFunction` that returns `true` for events that fullfill the `is_baseline` definition.
"""
function get_ged_qc_is_baseline_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel)
    get!(_cached_dataprod_is_baseline_pf, key) do
        is_baseline_def_props = _dataprod_qc(data, sel, detector).is_baseline
        return ljl_propfunc(is_baseline_def_props)
    end
end
export get_ged_qc_is_baseline_propfunc


const _cached_dataprod_pars_p_psd = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_pars_p_psd(data::LegendData, sel::AnyValiditySelection)
    data_id = objectid(data)
    key = (objectid(data), sel)
    get!(_cached_dataprod_pars_p_psd, key) do
        get_values(dataprod_parameters(data).ppars.aoe(sel))
    end
end

function _dataprod_pars_p_psd(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    _dataprod_pars_p_psd(data, sel)[Symbol(detector)]
end

"""
    LegendDataManagement.dataprod_pars_aoe_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the A/E cut window for the given data, validity selection and detector.
"""
function dataprod_pars_aoe_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    aoecut_lo::Float64 = get(_dataprod_pars_p_psd(data, sel, detector).cut, :lowcut, NaN)
    aoecut_hi::Float64 = get(_dataprod_pars_p_psd(data, sel, detector).cut, :highcut, NaN)  
    ClosedInterval(aoecut_lo, aoecut_hi)
end
export dataprod_pars_aoe_window



const _cached_get_larcal_props = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_larcal_props(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_get_larcal_props, key) do
        get_values(dataprod_parameters(data).ppars.sipm(sel))
    end
end

function _get_larcal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    _get_larcal_props(data, sel)[Symbol(detector)]
end


"""
    get_spm_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the LAr/SPMS calibration function for the given data, validity selection
and detector.
"""
function get_spm_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    larcal_props = _get_larcal_props(data, sel, detector)

    a::Float64 = get(larcal_props, :a, NaN)
    m::Float64 = get(larcal_props, :m, NaN)

    let a = a, m = m
        @pf (
            trig_pe = $trig_max .* m .+ a,
            trig_is_dc = [any(abs.($trig_pos_DC .- pos) .< 100u"ns") for pos in $trig_pos],
        )
    end
end
export get_spm_cal_propfunc
