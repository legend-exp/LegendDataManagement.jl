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
    ecal_props::String = get(get(get(_get_ecal_props(data, sel, detector; kwargs...), e_filter, PropDict()), :cal, PropDict()), :func, "blmean * NaN*keV")
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
    get_ged_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol = :rpars, pars_cat::Symbol = :ecal)

Returns the HPGe energy calibration `PropertyFunction` for the given data,
validity selection and detector.

The function has one property `<energy_type>_cal` for each energy type of the
energy configuration of `detector`. Each property evaluates the calibration
function stored in the parameters `pars_cat` of `pars_type`; without a stored
function the result is `NaN`.

## Keywords
* `pars_type::Symbol = :rpars`: Parameter set to use, `:rpars` or `:ppars`.
* `pars_cat::Symbol = :ecal`: Parameter category that holds the calibration.

The configuration and calibration data are cached internally; use a fresh
`data` object if the data on disk may have changed.
"""
function get_ged_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:rpars, pars_cat::Symbol=:ecal)
    let energies = Symbol.(_dataprod_ged_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_cal = Symbol.(string.(energies) .* "_cal")

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
    aoecal_props::String = get(_get_aoecal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[aoe_type], :func, "blmean * NaN")
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
    lqcal_props::String = get(_get_lqcal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[lq_type], :func, "blmean * NaN")
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
    get_ged_psd_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; aoe_pars_type::Symbol = :ppars, aoe_pars_cat::Symbol = :aoe, lq_pars_type::Symbol = :ppars, lq_pars_cat::Symbol = :lq)

Returns the HPGe PSD calibration `PropertyFunction` for the given data,
validity selection and detector.

The function has one property `<aoe_type>_classifier` for each A/E type and
one property `<lq_type>_classifier` for each LQ type of the PSD configuration
of `detector`. Each evaluates the calibration function stored in the
parameters.

## Keywords
* `aoe_pars_type::Symbol = :ppars`: Parameter set for A/E, `:ppars` or `:rpars`.
* `aoe_pars_cat::Symbol = :aoe`: Parameter category that holds the A/E calibration.
* `lq_pars_type::Symbol = :ppars`: Parameter set for LQ, `:ppars` or `:rpars`.
* `lq_pars_cat::Symbol = :lq`: Parameter category that holds the LQ calibration.

The configuration and calibration data are cached internally; use a fresh
`data` object if the data on disk may have changed.
"""
function get_ged_psd_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; aoe_pars_type::Symbol=:ppars, aoe_pars_cat::Symbol=:aoe, lq_pars_type::Symbol=:ppars, lq_pars_cat::Symbol=:lq)
    let aoe_types = Symbol.(_dataprod_aoe(data, sel, detector; pars_type=aoe_pars_type).aoe_types), aoe_classifier = Symbol.(string.(aoe_types) .* "_classifier"), lq_types = Symbol.(_dataprod_lq(data, sel, detector; pars_type=lq_pars_type).lq_types), lq_classifier = Symbol.(string.(lq_types) .* "_classifier")

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
    dataprod_qc = _dataprod_qc(data, sel)
    usability = channelinfo(data, sel, detector).usability
    merge(
        dataprod_qc.default,
        get(dataprod_qc, usability, PropDict()),
        get(dataprod_qc, detector, PropDict()),
    )
end

const _cached_dataprod_qc_cuts_pf = LRU{Tuple{UInt, AnyValiditySelection, DetectorId}, PropertyFunction}(maxsize = 10^2)


"""
    get_ged_qc_cuts_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Returns the `PropertyFunction` of the Ge-detector quality cut (QC) definitions
for the given data, validity selection and detector.

The function is built from the `labels` of the effective QC configuration,
which is merged in increasing order of precedence from `default`, the
usability group of the detector (for example `ac`), and the detector itself.
The result is cached per `data`, `sel` and `detector`.
"""
function get_ged_qc_cuts_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel, detector)
    get!(_cached_dataprod_qc_cuts_pf, key) do
        cut_def_props = _dataprod_qc(data, sel, detector).labels
        return ljl_propfunc(cut_def_props)
    end
end
export get_ged_qc_cuts_propfunc

const _cached_dataprod_is_trig_pf = LRU{Tuple{UInt, AnyValiditySelection, DetectorId}, PropertyFunction}(maxsize = 10^2)

"""
    get_ged_qc_is_trig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Returns the `PropertyFunction` of the Ge-detector trigger condition `is_trig`
for the given data, validity selection and detector.

The effective QC configuration is resolved as in
[`get_ged_qc_cuts_propfunc`](@ref).
"""
function get_ged_qc_is_trig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel, detector)
    get!(_cached_dataprod_is_trig_pf, key) do
        is_trig_def_props = _dataprod_qc(data, sel, detector).is_trig
        return ljl_propfunc(is_trig_def_props)
    end
end
export get_ged_qc_is_trig_propfunc

const _cached_dataprod_is_single_pulse_pf = LRU{Tuple{UInt, AnyValiditySelection, DetectorId}, PropertyFunction}(maxsize = 10^2)

"""
    get_ged_qc_is_single_pulse_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Returns a `PropertyFunction` that returns `true` for events that fulfill the
effective `is_single_pulse` definition.

Usability-group and detector-specific QC overrides are applied while resolving
the QC configuration of the detector, as in
[`get_ged_qc_cuts_propfunc`](@ref).
"""
function get_ged_qc_is_single_pulse_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel, detector)
    get!(_cached_dataprod_is_single_pulse_pf, key) do
        is_single_pulse_def_props = _dataprod_qc(data, sel, detector).is_single_pulse
        return ljl_propfunc(is_single_pulse_def_props)
    end
end
export get_ged_qc_is_single_pulse_propfunc

const _cached_dataprod_is_empty_trace_pf = LRU{Tuple{UInt, AnyValiditySelection, DetectorId}, PropertyFunction}(maxsize = 10^2)

"""
    get_ged_qc_is_empty_trace_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Returns a `PropertyFunction` that returns `true` for events that fulfill the
effective `is_empty_trace` definition.

The effective QC configuration is resolved as in
[`get_ged_qc_cuts_propfunc`](@ref).
"""
function get_ged_qc_is_empty_trace_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel, detector)
    get!(_cached_dataprod_is_empty_trace_pf, key) do
        is_empty_trace_def_props = _dataprod_qc(data, sel, detector).is_empty_trace
        return ljl_propfunc(is_empty_trace_def_props)
    end
end
export get_ged_qc_is_empty_trace_propfunc

const _cached_dataprod_is_crosstalk_pf = LRU{Tuple{UInt, AnyValiditySelection, DetectorId}, PropertyFunction}(maxsize = 10^2)

"""
    get_ged_qc_is_crosstalk_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Returns a `PropertyFunction` that returns `true` for events that fulfill the
effective `is_crosstalk` definition.

The effective QC configuration is resolved as in
[`get_ged_qc_cuts_propfunc`](@ref).
"""
function get_ged_qc_is_crosstalk_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = (objectid(data), sel, detector)
    get!(_cached_dataprod_is_crosstalk_pf, key) do
        is_crosstalk_def_props = _dataprod_qc(data, sel, detector).is_crosstalk
        return ljl_propfunc(is_crosstalk_def_props)
    end
end
export get_ged_qc_is_crosstalk_propfunc


### HPGe cut functions

"""
    dataprod_pars_aoe_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_classifier::Symbol; pars_type::Symbol = :ppars, pars_cat::Symbol = :aoe)

Returns the A/E cut window of `aoe_classifier` for the given data, validity
selection and detector as a `ClosedInterval`.

The limits are the `lowcut` and `highcut` entries of the A/E parameters; a
missing limit is `-Inf` or `Inf`, respectively.

## Keywords
* `pars_type::Symbol = :ppars`: Parameter set to use, `:ppars` or `:rpars`.
* `pars_cat::Symbol = :aoe`: Parameter category that holds the A/E parameters.

See also [`LegendDataManagement.dataprod_pars_lq_window`](@ref).
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
    get_ged_aoe_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol = :ppars, pars_cat::Symbol = :aoe)

Returns the A/E cut `PropertyFunction` for the given data, validity selection
and detector.

For each A/E classifier of the configuration of `detector` the function has
the properties `<classifier>_low_cut`, which is `true` below the lower limit
of the cut window, and `<classifier>_ds_cut`, which is `true` outside the
window. The window is given by [`dataprod_pars_aoe_window`](@ref).

## Keywords
* `pars_type::Symbol = :ppars`: Parameter set to use, `:ppars` or `:rpars`.
* `pars_cat::Symbol = :aoe`: Parameter category that holds the A/E parameters.

See also [`get_ged_lq_cut_propfunc`](@ref).
"""
function get_ged_aoe_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    let aoe_classifiers = Symbol.(_dataprod_aoe(data, sel, detector; pars_type=pars_type).aoe_classifiers), aoe_low_cut = Symbol.(string.(aoe_classifiers) .* "_low_cut"),
        aoe_ds_cut = Symbol.(string.(aoe_classifiers) .* "_ds_cut")

        ljl_propfunc(
            merge(
                Dict{Symbol, String}(
                    aoe_low_cut .=> _get_ged_aoe_lowcut_propfunc_str.(Ref(data), Ref(sel), Ref(detector), aoe_classifiers; pars_type=pars_type, pars_cat=pars_cat)
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
    LegendDataManagement.dataprod_pars_lq_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol = :ppars, pars_cat::Symbol = :lq)

Returns the LQ cut window of `lq_classifier` for the given data, validity
selection and detector as a `ClosedInterval`.

The limits are the `lowcut` and `highcut` entries of the LQ parameters; a
missing limit is `-Inf` or `Inf`, respectively.

## Keywords
* `pars_type::Symbol = :ppars`: Parameter set to use, `:ppars` or `:rpars`.
* `pars_cat::Symbol = :lq`: Parameter category that holds the LQ parameters.

See also [`dataprod_pars_aoe_window`](@ref).
"""
function dataprod_pars_lq_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    lqcut_lo::Float64 = get(_get_lqcal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[lq_classifier], :lowcut, -Inf)
    lqcut_hi::Float64 = get(_get_lqcal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[lq_classifier], :highcut, Inf)
    ClosedInterval(lqcut_lo, lqcut_hi)
end

function _get_ged_lq_highcut_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    "$(lq_classifier) > $(rightendpoint(dataprod_pars_lq_window(data, sel, detector, lq_classifier; pars_type=pars_type, pars_cat=pars_cat)))"
end

function _get_ged_lq_dscut_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    "$(lq_classifier) < $(leftendpoint(dataprod_pars_lq_window(data, sel, detector, lq_classifier; pars_type=pars_type, pars_cat=pars_cat))) || $(lq_classifier) > $(rightendpoint(dataprod_pars_lq_window(data, sel, detector, lq_classifier; pars_type=pars_type, pars_cat=pars_cat)))"
end

"""
    get_ged_lq_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol = :ppars, pars_cat::Symbol = :lq)

Returns the LQ cut `PropertyFunction` for the given data, validity selection
and detector.

For each LQ classifier of the configuration of `detector` the function has the
properties `<classifier>_high_cut`, which is `true` above the upper limit of
the cut window, and `<classifier>_ds_cut`, which is `true` outside the
window. The window is given by
[`LegendDataManagement.dataprod_pars_lq_window`](@ref).

## Keywords
* `pars_type::Symbol = :ppars`: Parameter set to use, `:ppars` or `:rpars`.
* `pars_cat::Symbol = :lq`: Parameter category that holds the LQ parameters.

See also [`get_ged_aoe_cut_propfunc`](@ref).
"""
function get_ged_lq_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    let lq_classifiers = Symbol.(_dataprod_lq(data, sel, detector; pars_type=pars_type).lq_classifiers), lq_high_cut = Symbol.(string.(lq_classifiers) .* "_high_cut"),
        lq_ds_cut = Symbol.(string.(lq_classifiers) .* "_ds_cut")

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
    get_ged_psd_classifier_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol = :ppars)

Returns the `PropertyFunction` of the PSD classifier for the given data,
validity selection and detector.

The function is built from the `psd_classifier` entry of the PSD
configuration of `detector`.

## Keywords
* `pars_type::Symbol = :ppars`: Configuration set to use, `:ppars` or `:rpars`.
"""
function get_ged_psd_classifier_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    dataprod_psd_config = _dataprod_psd(data, sel).psd
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    ljl_propfunc(merge(dataprod_psd_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_psd_config, :p, PropDict()), dataprod_psd_config), detector, PropDict())).psd_classifier)
end
export get_ged_psd_classifier_propfunc


### SiPM LAr cal functions

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
    ecal_props::String = get(get(get(_get_larcal_props(data, sel, detector; kwargs...), e_filter, PropDict()), :cal, PropDict()), :func, "$(e_filter) .* (NaN*e)")
    return ecal_props
end

function _get_larcal_dc_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    dataprod_lar = _dataprod_lar_cal(data, sel, detector; kwargs...)
    let Δt_pos_dc_tag = dataprod_lar.Δt_pos_dc_tag, dc_tag_interval = ClosedInterval(dataprod_lar.dc_tag_interval...)
        @pf [any(  abs.($pos_dc .- p) .< Δt_pos_dc_tag  .&&  ($max_dc .>= dc_tag_interval.left) .&& ($max_dc .<= dc_tag_interval.right)) for p in $pos]
    end
    
end

function _get_larcal_dc_sel_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    dataprod_lar_filter = _dataprod_lar_cal(data, sel, detector; kwargs...).energy_types[e_filter]
    PropSelFunction(
        Symbol(e_filter) => :max, Symbol(dataprod_lar_filter.pos) => :pos,
        Symbol(dataprod_lar_filter.dc) => :max_dc, Symbol(dataprod_lar_filter.dc_pos) => :pos_dc
    )
end


"""
    get_spm_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol = :ppars, pars_cat::Symbol = :sipmcal)

Returns the liquid-argon (LAr) SiPM calibration `PropertyFunction` for the
given data, validity selection and detector.

The function has one property `<energy_type>_cal` for each energy type of the
LAr configuration. Each evaluates the calibration function stored in the
parameters; without a stored function the result is `NaN`.

## Keywords
* `pars_type::Symbol = :ppars`: Parameter set to use, `:ppars` or `:rpars`.
* `pars_cat::Symbol = :sipmcal`: Parameter category that holds the calibration.
"""
function get_spm_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:sipmcal)
    let energies = keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_cal = Symbol.(string.(keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types)) .* "_cal")
        ljl_propfunc(
            Dict{Symbol, String}(
                energies_cal .=> _get_larcal_propfunc_str.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type, pars_cat=pars_cat)
            )
        )
    end
end
export get_spm_cal_propfunc

"""
    get_spm_dc_sel_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol = :ppars)

Returns the SiPM delayed-coincidence (DC) selector functions for the given
data, validity selection and detector as a `NamedTuple` of
`PropSelFunction`s.

The tuple has one entry `<energy_type>_is_dc` for each energy type of the LAr
configuration. Each selects the properties of the energy type and of its DC
counterpart under the names `max`, `pos`, `max_dc` and `pos_dc`, which are the
inputs of the corresponding function of [`get_spm_dc_cal_propfunc`](@ref).

## Keywords
* `pars_type::Symbol = :ppars`: Configuration set to use, `:ppars` or `:rpars`.
"""
function get_spm_dc_sel_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    let energies = keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_dc = Symbol.(string.(keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types)) .* "_is_dc")
        NamedTuple{Tuple(energies_dc)}(_get_larcal_dc_sel_propfunc.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type))
    end
end
export get_spm_dc_sel_propfunc


"""
    get_spm_dc_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol = :ppars)

Returns the SiPM delayed-coincidence (DC) tagging functions for the given
data, validity selection and detector as a `NamedTuple` of
`PropertyFunction`s.

The tuple has one entry `<energy_type>_is_dc` for each energy type of the LAr
configuration. Each returns, for every pulse position, whether a DC pulse lies
within the configured time difference and amplitude interval. The input
properties are those selected by [`get_spm_dc_sel_propfunc`](@ref).

## Keywords
* `pars_type::Symbol = :ppars`: Configuration set to use, `:ppars` or `:rpars`.
"""
function get_spm_dc_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    let energies = keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_dc = Symbol.(string.(keys(_dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types)) .* "_is_dc")
        NamedTuple{Tuple(energies_dc)}(_get_larcal_dc_propfunc.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type))
    end
end
export get_spm_dc_cal_propfunc


### PMT Muon cal functions

const _cached_dataprod_pmt_cal = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_pmt_cal(data::LegendData, sel::AnyValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_dataprod_pmt_cal, key) do
        dataprod_config(data).pmt(sel)
    end
end

function _dataprod_muon_cal(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:rpars)
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    dataprod_muon_config = _dataprod_pmt_cal(data, sel).muon
    merge(dataprod_muon_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_muon_config, :p, PropDict()), dataprod_muon_config), detector, PropDict()))
end

function _get_pmt_is_physical_trig_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    is_physical_trig_props::String = _dataprod_muon_cal(data, sel, detector; kwargs...).energy_types[e_filter].is_physical_trig
    return is_physical_trig_props
end

const _cached_get_pmtcal_props = LRU{Tuple{UInt, AnyValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_pmtcal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:rpars, pars_cat::Symbol=:pmtcal)
    key = (objectid(data), sel)
    mkpath(data_path(dataprod_parameters(data)[pars_type][pars_cat]))
    get!(_cached_get_pmtcal_props, key) do
        _get_cal_values(dataprod_parameters(data)[pars_type][pars_cat], sel)
    end
end

function _get_pmtcal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; kwargs...)
    _get_pmtcal_props(data, sel; kwargs...)[Symbol(detector)]
end

function _get_pmtcal_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    ecal_props::String = get(get(get(_get_pmtcal_props(data, sel, detector; kwargs...), e_filter, PropDict()), :cal, PropDict()), :func, "e_fc .* (NaN*e)")
    return ecal_props
end


"""
    get_pmt_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol = :rpars, pars_cat::Symbol = :pmtcal)

Returns the PMT calibration `PropertyFunction` for the given data, validity
selection and detector.

The function has one property `<energy_type>_cal` for each energy type of the
muon configuration. Each evaluates the calibration function stored in the
parameters; without a stored function the result is `NaN`.

## Keywords
* `pars_type::Symbol = :rpars`: Parameter set to use, `:rpars` or `:ppars`.
* `pars_cat::Symbol = :pmtcal`: Parameter category that holds the calibration.
"""
function get_pmt_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:rpars, pars_cat::Symbol=:pmtcal)
    let energies = keys(_dataprod_muon_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_cal = Symbol.(string.(keys(_dataprod_muon_cal(data, sel, detector; pars_type=pars_type).energy_types)) .* "_cal")
        ljl_propfunc(
            Dict{Symbol, String}(
                energies_cal .=> _get_pmtcal_propfunc_str.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type, pars_cat=pars_cat)
            )
        )
    end
end
export get_pmt_cal_propfunc


"""
    get_pmt_is_physical_trig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type = :rpars)

Returns the PMT physical trigger condition as a `PropertyFunction` for the
given data, validity selection and detector.

The function has one property `<energy_type>_is_physical_trig` for each energy
type of the muon configuration.

## Keywords
* `pars_type = :rpars`: Configuration set to use, `:rpars` or `:ppars`.
"""
function get_pmt_is_physical_trig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type=:rpars)
    let energies = keys(_dataprod_muon_cal(data, sel, detector; pars_type=pars_type).energy_types), energies_is_physical_trig = Symbol.(string.(keys(_dataprod_muon_cal(data, sel, detector; pars_type=pars_type).energy_types)) .* "_is_physical_trig")
        ljl_propfunc(
            Dict{Symbol, String}(
                energies_is_physical_trig .=> _get_pmt_is_physical_trig_propfunc_str.(Ref(data), Ref(sel), Ref(detector), energies; pars_type=pars_type)
            )
        )
    end
end
export get_pmt_is_physical_trig_propfunc
