# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

_get_cal_values(pd::PropsDB, sel::AnyValiditySelection) = get_values(pd(sel))
_get_cal_values(pd::NoSuchPropsDBEntry, sel::AnyValiditySelection) = PropDicts.PropDict()


### HPGe calibration functions
const _cached_get_ecal_props = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_ecal_props(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_get_ecal_props, key) do
        _get_cal_values(dataprod_parameters(data).rpars.ecal, sel)
    end
end

function _get_ecal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    get(_get_ecal_props(data, sel), Symbol(detector), PropDict())
end

function _get_e_cal_propsfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol)
    ecal_props::String = get(get(get(_get_ecal_props(data, sel, detector), e_filter, PropDict()), :cal, PropDict()), :func, "e_max * NaN*keV")
    return ecal_props
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
    let energies = Symbol.(_dataprod_ged_cal(data, sel, detector).energy_types), energies_cal = Symbol.(_dataprod_ged_cal(data, sel, detector).energy_types .* "_cal")

        ljl_propfunc(Dict{Symbol, String}(
            energies_cal .=> _get_e_cal_propsfunc_str.(Ref(data), Ref(sel), Ref(detector), energies)
        ))
    end
end
export get_ged_cal_propfunc



### HPGe PSD calibration functions
const _cached_get_aoecal_props = LRU{Tuple{UInt, AnyValiditySelection, Symbol, Symbol}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_aoecal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    key = (objectid(data), sel, pars_type, pars_cat)
    get!(_cached_get_aoecal_props, key) do
        _get_cal_values(dataprod_parameters(data)[pars_type][pars_cat], sel)
    end
end

function _get_aoecal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    get(_get_aoecal_props(data, sel; pars_type=pars_type, pars_cat=pars_cat), Symbol(detector), PropDict())
end

function _get_aoe_cal_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_type::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    aoecal_props::String = get(_get_aoecal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[aoe_type], :func, "a_raw * NaN")
    return aoecal_props
end

const _cached_dataprod_psd = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_psd(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_dataprod_psd, key) do
        dataprod_config(data).psd(sel)
    end
end

function _dataprod_aoe(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    dataprod_aoe_config = _dataprod_psd(data, sel).aoe
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    merge(dataprod_aoe_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_aoe_config, :p, PropDict()), dataprod_aoe_config), detector, PropDict()))
end

"""
    get_ged_psd_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)

Get the HPGe psd calibration function for the given data, validity selection and
detector.

Note: Caches configuration/calibration data internally, use a fresh `data`
object if on-disk configuration/calibration data may have changed.
"""
function get_ged_psd_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    let aoe_types = collect(keys(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_funcs)), aoe_classifier = Symbol.(string.(keys(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_funcs)) .* "_classifier")

        ljl_propfunc(Dict{Symbol, String}(
            aoe_classifier .=> _get_aoe_cal_propfunc_str.(Ref(data), Ref(sel), Ref(detector), aoe_types; pars_type=pars_type, pars_cat=pars_cat)
        ))
    end
end
export get_ged_psd_propfunc



### HPGe QC calibration functions

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




### HPGe cut functions

"""
    LegendDataManagement.dataprod_pars_aoe_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the A/E cut window for the given data, validity selection and detector.
"""
function dataprod_pars_aoe_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_type::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    aoecut_lo::Float64 = get(_get_aoecal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[aoe_type].cut, :lowcut, -Inf)
    aoecut_hi::Float64 = get(_get_aoecal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[aoe_type].cut, :highcut, Inf)
    ClosedInterval(aoecut_lo, aoecut_hi)
end
export dataprod_pars_aoe_window


function _get_ged_aoe_lowcut_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_type::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    "$(aoe_type)_classifier > $(leftendpoint(dataprod_pars_aoe_window(data, sel, detector, aoe_type; pars_type=pars_type, pars_cat=pars_cat)))"
end

function _get_ged_aoe_dscut_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_type::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    "$(aoe_type)_classifier > $(leftendpoint(dataprod_pars_aoe_window(data, sel, detector, aoe_type; pars_type=pars_type, pars_cat=pars_cat))) && $(aoe_type)_classifier < $(rightendpoint(dataprod_pars_aoe_window(data, sel, detector, aoe_type; pars_type=pars_type, pars_cat=pars_cat)))"
end

"""
    LegendDataManagement.get_ged_aoe_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)

Get the A/E cut propfuncs for the given data, validity selection and detector.
"""
function get_ged_aoe_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    let aoe_types = collect(keys(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_funcs)), aeo_low_cut = Symbol.(string.(keys(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_funcs)) .* "_low_cut"),
        aoe_ds_cut = Symbol.(string.(keys(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_funcs)) .* "_ds_cut")

        ljl_propfunc(
            merge(
                Dict{Symbol, String}(
                    aeo_low_cut .=> _get_ged_aoe_lowcut_propfunc_str.(Ref(data), Ref(sel), Ref(detector), aoe_types; pars_type=pars_type, pars_cat=pars_cat)
                ),
                Dict{Symbol, String}(
                    aoe_ds_cut .=> _get_ged_aoe_dscut_propfunc_str.(Ref(data), Ref(sel), Ref(detector), aoe_types; pars_type=pars_type, pars_cat=pars_cat)
                )
            )
        )
    end
end
export get_ged_aoe_cut_propfunc



### SiPM LAr cut functions

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
