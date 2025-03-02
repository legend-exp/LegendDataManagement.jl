# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

_get_cal_values(pd::PropsDB, sel::AnyValiditySelection) = get_values(pd(sel))
_get_cal_values(pd::NoSuchPropsDBEntry, sel::AnyValiditySelection) = PropDicts.PropDict()


### HPGe calibration functions
const _cached_get_ecal_props = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_ecal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:rpars, pars_cat::Symbol=:ecal)
    key = (objectid(data), sel)
    get!(_cached_get_ecal_props, key) do
        _get_cal_values(dataprod_parameters(data)[pars_type][pars_cat], sel)
    end
end

function _get_ecal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; kwargs...)
    get(_get_ecal_props(data, sel; kwargs...), Symbol(detector), PropDict())
end

function _get_e_cal_propsfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    ecal_props::String = get(get(get(_get_ecal_props(data, sel, detector; kwargs...), e_filter, PropDict()), :cal, PropDict()), :func, "e_max * NaN*keV")
    return ecal_props
end

const _cached_dataprod_ged_cal = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_ged_cal(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_dataprod_ged_cal, key) do
        dataprod_config(data).energy(sel)
    end
end

function _dataprod_ged_cal(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:rpars)
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    dataprod_ged_config = _dataprod_ged_cal(data, sel)
    merge(dataprod_ged_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_ged_config, :p, PropDict()), dataprod_ged_config), detector, PropDict()))
end

"""
    get_ged_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the HPGe calibration function for the given data, validity selection and
detector.

Note: Caches configuration/calibration data internally, use a fresh `data`
object if on-disk configuration/calibration data may have changed.
"""
function get_ged_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:rpars, pars_cat::Symbol=:ecal)
    let energies = Symbol.(_dataprod_ged_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_cal = Symbol.(_dataprod_ged_cal(data, sel, detector; pars_type=pars_type).energy_types .* "_cal")

        ljl_propfunc(Dict{Symbol, String}(
            energies_cal .=> _get_e_cal_propsfunc_str.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type, pars_cat=pars_cat)
        ))
    end
end
export get_ged_cal_propfunc



### HPGe PSD calibration functions
# A/E
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

# LQ
const _cached_get_lqcal_props = LRU{Tuple{UInt, AnyValiditySelection, Symbol, Symbol}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_lqcal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    key = (objectid(data), sel, pars_type, pars_cat)
    get!(_cached_get_lqcal_props, key) do
        _get_cal_values(dataprod_parameters(data)[pars_type][pars_cat], sel)
    end
end

function _get_lqcal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    get(_get_lqcal_props(data, sel; pars_type=pars_type, pars_cat=pars_cat), Symbol(detector), PropDict())
end

function _get_lq_cal_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_type::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    lqcal_props::String = get(_get_lqcal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[lq_type], :func, "lq * NaN")
    return lqcal_props
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

function _dataprod_lq(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    dataprod_lq_config = _dataprod_psd(data, sel).lq
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    merge(dataprod_lq_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_lq_config, :p, PropDict()), dataprod_lq_config), detector, PropDict()))
end

"""
    get_ged_psd_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)

Get the HPGe psd calibration function for the given data, validity selection and
detector.

Note: Caches configuration/calibration data internally, use a fresh `data`
object if on-disk configuration/calibration data may have changed.
"""
function get_ged_psd_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; aoe_pars_type::Symbol=:ppars, aoe_pars_cat::Symbol=:aoe, lq_pars_type::Symbol=:ppars, lq_pars_cat::Symbol=:lq)
    let aoe_types = Symbol.(_dataprod_aoe(data, sel, detector; pars_type=aoe_pars_type).aoe_types), aoe_classifier = Symbol.(_dataprod_aoe(data, sel, detector; pars_type=lq_pars_type).aoe_types .* "_classifier"), lq_types = Symbol.(_dataprod_lq(data, sel, detector; pars_type=lq_pars_type).lq_types), lq_classifier = Symbol.(_dataprod_lq(data, sel, detector; pars_type=lq_pars_type).lq_types .* "_classifier")

        ljl_propfunc(
            merge(
                Dict{Symbol, String}(
                    aoe_classifier .=> _get_aoe_cal_propfunc_str.(Ref(data), Ref(sel), Ref(detector), aoe_types; pars_type=aoe_pars_type, pars_cat=aoe_pars_cat)
                ),
                Dict{Symbol, String}(
                    lq_classifier .=> _get_lq_cal_propfunc_str.(Ref(data), Ref(sel), Ref(detector), lq_types; pars_type=lq_pars_type, pars_cat=lq_pars_cat)
                )
            )
        )
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
function dataprod_pars_aoe_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    aoecut_lo::Float64 = get(_get_aoecal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[aoe_classifier], :lowcut, -Inf)
    aoecut_hi::Float64 = get(_get_aoecal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[aoe_classifier], :highcut, Inf)
    ClosedInterval(aoecut_lo, aoecut_hi)
end
export dataprod_pars_aoe_window


function _get_ged_aoe_lowcut_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    "$(aoe_classifier) < $(leftendpoint(dataprod_pars_aoe_window(data, sel, detector, aoe_classifier; pars_type=pars_type, pars_cat=pars_cat)))"
end

function _get_ged_aoe_dscut_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    "$(aoe_classifier) < $(leftendpoint(dataprod_pars_aoe_window(data, sel, detector, aoe_classifier; pars_type=pars_type, pars_cat=pars_cat))) || $(aoe_classifier) > $(rightendpoint(dataprod_pars_aoe_window(data, sel, detector, aoe_classifier; pars_type=pars_type, pars_cat=pars_cat)))"
end

"""
    LegendDataManagement.get_ged_aoe_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)

Get the A/E cut propfuncs for the given data, validity selection and detector.
"""
function get_ged_aoe_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    let aoe_classifiers = Symbol.(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_classifiers), aeo_low_cut = Symbol.(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_classifiers .* "_low_cut"),
        aoe_ds_cut = Symbol.(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_classifiers .* "_ds_cut")

        ljl_propfunc(
            merge(
                Dict{Symbol, String}(
                    aeo_low_cut .=> _get_ged_aoe_lowcut_propfunc_str.(Ref(data), Ref(sel), Ref(detector), aoe_classifiers; pars_type=pars_type, pars_cat=pars_cat)
                ),
                Dict{Symbol, String}(
                    aoe_ds_cut .=> _get_ged_aoe_dscut_propfunc_str.(Ref(data), Ref(sel), Ref(detector), aoe_classifiers; pars_type=pars_type, pars_cat=pars_cat)
                )
            )
        )
    end
end
export get_ged_aoe_cut_propfunc


"""
    LegendDataManagement.dataprod_pars_lq_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)

Get the LQ cut window for the given data, validity selection and detector.
"""
function dataprod_pars_lq_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    lqcut_lo::Float64 = get(_get_lqcal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[lq_classifier], :lowcut, -Inf)
    lqcut_hi::Float64 = get(_get_lqcal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[lq_classifier], :cut, Inf)
    ClosedInterval(lqcut_lo, lqcut_hi)
end

function _get_ged_lq_highcut_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    "$(lq_classifier) > $(rightendpoint(dataprod_pars_lq_window(data, sel, detector, lq_classifier; pars_type=pars_type, pars_cat=pars_cat)))"
end

function _get_ged_lq_dscut_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    "$(lq_classifier) < $(leftendpoint(dataprod_pars_lq_window(data, sel, detector, lq_classifier; pars_type=pars_type, pars_cat=pars_cat))) || $(lq_classifier) > $(rightendpoint(dataprod_pars_lq_window(data, sel, detector, lq_classifier; pars_type=pars_type, pars_cat=pars_cat)))"
end

"""
    LegendDataManagement.get_ged_lq_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)

Get the LQ cut propfuncs for the given data, validity selection and detector.
"""
function get_ged_lq_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    let lq_classifiers = Symbol.(_dataprod_lq(data, sel, detector; pars_type=pars_type).lq_classifiers), lq_high_cut = Symbol.(_dataprod_lq(data, sel, detector; pars_type=pars_type).lq_classifiers .* "_high_cut"),
        lq_ds_cut = Symbol.(_dataprod_lq(data, sel, detector; pars_type=pars_type).lq_classifiers .* "_ds_cut")

        ljl_propfunc(
            merge(
                Dict{Symbol, String}(
                    lq_high_cut .=> _get_ged_lq_highcut_propfunc_str.(Ref(data), Ref(sel), Ref(detector), lq_classifiers; pars_type=pars_type, pars_cat=pars_cat)
                ),
                Dict{Symbol, String}(
                    lq_ds_cut .=> _get_ged_lq_dscut_propfunc_str.(Ref(data), Ref(sel), Ref(detector), lq_classifiers; pars_type=pars_type, pars_cat=pars_cat)
                )
            )
        )
    end
end
export get_ged_lq_cut_propfunc


"""
    get_ged_psd_classifier_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the PSD cut propfuncs for the given data and validity selection.
"""
function get_ged_psd_classifier_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    dataprod_psd_config = _dataprod_psd(data, sel).psd
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    ljl_propfunc(merge(dataprod_psd_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_psd_config, :p, PropDict()), dataprod_psd_config), detector, PropDict())).psd_classifier)
end
export get_ged_psd_classifier_propfunc


### SiPM LAr cut functions

const _cached_dataprod_spms_cal = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_spms_cal(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_dataprod_spms_cal, key) do
        dataprod_config(data).sipm(sel)
    end
end

function _dataprod_lar_cal(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    dataprod_lar_config = _dataprod_spms_cal(data, sel).lar
    merge(dataprod_lar_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_lar_config, :p, PropDict()), dataprod_lar_config), detector, PropDict()))
end

const _cached_get_larcal_props = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_larcal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:ppars, pars_cat::Symbol=:sipmcal)
    key = (objectid(data), sel)
    get!(_cached_get_larcal_props, key) do
        _get_cal_values(dataprod_parameters(data)[pars_type][pars_cat], sel)
    end
end

function _get_larcal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; kwargs...)
    _get_larcal_props(data, sel; kwargs...)[Symbol(detector)]
end

function _get_larcal_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    ecal_props::String = get(get(get(_get_larcal_props(data, sel, detector; kwargs...), e_filter, PropDict()), :cal, PropDict()), :func, "trig_max .* (NaN*e)")
    return ecal_props
end

function _get_larcal_dc_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    dataprod_lar = _dataprod_lar_cal(data, sel, detector; kwargs...)
    let Δt_pos_dc_tag = dataprod_lar.Δt_pos_dc_tag, dc_tag_interval = ClosedInterval(dataprod_lar.dc_tag_interval...)
        @pf [any(  abs.($pos_dc .- p) .< Δt_pos_dc_tag  .&&  $max_dc .∈ dc_tag_interval) for p in $pos]
    end
    
end

function _get_larcal_dc_sel_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    dataprod_lar_filter = _dataprod_lar_cal(data, sel, detector; kwargs...).energy_types[e_filter]
    PropSelFunction{Symbol.((e_filter, dataprod_lar_filter.pos, dataprod_lar_filter.dc, dataprod_lar_filter.dc_pos)), (:max, :pos, :max_dc, :pos_dc)}()
end


"""
    get_spm_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the LAr/SPMS calibration function for the given data, validity selection
and detector.
"""
function get_spm_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    let energies = keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_cal = Symbol.(string.(keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types)) .* "_cal")
        ljl_propfunc(
            Dict{Symbol, String}(
                energies_cal .=> _get_larcal_propfunc_str.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type)
            )
        )
    end
end
export get_spm_cal_propfunc

"""
    get_spm_dc_sel_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the LAr/SPMS DC calibration selector function for the given data, validity selection
and detector.
"""
function get_spm_dc_sel_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    let energies = keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_dc = Symbol.(string.(keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types)) .* "_is_dc")
        NamedTuple{Tuple(energies_dc)}(_get_larcal_dc_sel_propfunc.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type))
    end
end
export get_spm_dc_sel_propfunc


"""
    get_spm_dc_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the LAr/SPMS DC calibration function for the given data, validity selection
and detector.
"""
function get_spm_dc_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    let energies = keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_dc = Symbol.(string.(keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types)) .* "_is_dc")
        NamedTuple{Tuple(energies_dc)}(_get_larcal_dc_propfunc.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type))
    end
end
export get_spm_dc_cal_propfunc