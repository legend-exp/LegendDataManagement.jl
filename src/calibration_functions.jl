# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const _ValidityKey = Tuple{UInt, Tuple{Vararg{Symbol}}, Union{Nothing, Vector{String}}}
const _cached_validity_filelist = LRU{Tuple{UInt, Tuple{Vararg{Symbol}}, AnyValiditySelection}, Union{Nothing, Vector{String}}}(maxsize = 10^4)
const _cached_cal_values = LRU{_ValidityKey, PropDict}(maxsize = 15)
const _cached_dataprod_cal_config = LRU{_ValidityKey, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^2)

# Files of the validity entry that `sel` selects in the props db `get_pd()` called `name`, `nothing` if it does not exist:
function _validity_filelist(get_pd, data::LegendData, sel::AnyValiditySelection, name::Tuple{Vararg{Symbol}})
    get!(_cached_validity_filelist, (objectid(data), name, sel)) do
        pd = get_pd()
        pd isa NoSuchPropsDBEntry ? nothing : _validity_sel_filelist(pd, sel)
    end
end

# `f(get_pd()(sel))`, cached per validity entry, so all selections of the same entry share it:
function _validity_cached(f, cache::LRU, get_pd, data::LegendData, sel::AnyValiditySelection, name::Tuple{Vararg{Symbol}}; allow_missing::Bool = false)
    filelist = _validity_filelist(get_pd, data, sel, name)
    isnothing(filelist) && !allow_missing && throw(ArgumentError("No metadata $(join(name, '/')) in $(data.name)"))
    get!(cache, (objectid(data), name, filelist)) do
        isnothing(filelist) ? PropDict() : f(get_pd()(sel))
    end
end

# `merge` of PropDicts merges into the nested dicts of its arguments, which belong to the caches:
_merge_copies(dicts...) = merge(deepcopy.(dicts)...)

function _get_cal_values(data::LegendData, sel::AnyValiditySelection, pars_type::Symbol, pars_cat::Symbol)
    _validity_cached(get_values, _cached_cal_values, () -> dataprod_parameters(data)[pars_type][pars_cat], data, sel, (:pars, pars_type, pars_cat); allow_missing = true)
end

function _dataprod_config(data::LegendData, sel::AnyValiditySelection, name::Symbol; cache::LRU = _cached_dataprod_cal_config)
    _validity_cached(identity, cache, () -> getproperty(dataprod_config(data), name), data, sel, (:config, name))
end

# QC definitions of a detector depend on the QC config and on the detector's usability:
_qc_key(data::LegendData, sel::AnyValiditySelection, detector::DetectorId) =
    (objectid(data), _validity_filelist(() -> dataprod_config(data).qc, data, sel, (:config, :qc)), _channelinfo_inputs(data, sel, false), detector)


### HPGe calibration functions
function _get_ecal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:rpars, pars_cat::Symbol=:ecal)
    _get_cal_values(data, sel, pars_type, pars_cat)
end

function _get_ecal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; kwargs...)
    get(_get_ecal_props(data, sel; kwargs...), Symbol(detector), PropDict())
end

function _get_e_cal_propsfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    ecal_props::String = get(get(get(_get_ecal_props(data, sel, detector; kwargs...), e_filter, PropDict()), :cal, PropDict()), :func, "blmean * NaN*keV")
    return ecal_props
end

_dataprod_ged_cal(data::LegendData, sel::AnyValiditySelection) = _dataprod_config(data, sel, :energy)

function _dataprod_ged_cal(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:rpars)
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    dataprod_ged_config = _dataprod_ged_cal(data, sel)
    _merge_copies(dataprod_ged_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_ged_config, :p, PropDict()), dataprod_ged_config), detector, PropDict()))
end

"""
    get_ged_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the HPGe calibration function for the given data, validity selection and
detector.

Note: Caches configuration/calibration data internally, use a fresh `data`
object if on-disk configuration/calibration data may have changed.
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
function _get_aoecal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    _get_cal_values(data, sel, pars_type, pars_cat)
end

function _get_aoecal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    get(_get_aoecal_props(data, sel; pars_type=pars_type, pars_cat=pars_cat), Symbol(detector), PropDict())
end

function _get_aoe_cal_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, aoe_type::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)
    aoecal_props::String = get(_get_aoecal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[aoe_type], :func, "blmean * NaN")
    return aoecal_props
end

# LQ
function _get_lqcal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    _get_cal_values(data, sel, pars_type, pars_cat)
end

function _get_lqcal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    get(_get_lqcal_props(data, sel; pars_type=pars_type, pars_cat=pars_cat), Symbol(detector), PropDict())
end

function _get_lq_cal_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_type::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)
    lqcal_props::String = get(_get_lqcal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)[lq_type], :func, "blmean * NaN")
    return lqcal_props
end

_dataprod_psd(data::LegendData, sel::AnyValiditySelection) = _dataprod_config(data, sel, :psd)

function _dataprod_aoe(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    dataprod_aoe_config = _dataprod_psd(data, sel).aoe
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    _merge_copies(dataprod_aoe_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_aoe_config, :p, PropDict()), dataprod_aoe_config), detector, PropDict()))
end

function _dataprod_lq(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    dataprod_lq_config = _dataprod_psd(data, sel).lq
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    _merge_copies(dataprod_lq_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_lq_config, :p, PropDict()), dataprod_lq_config), detector, PropDict()))
end

"""
    get_ged_psd_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:aoe)

Get the HPGe psd calibration function for the given data, validity selection and
detector.

Note: Caches configuration/calibration data internally, use a fresh `data`
object if on-disk configuration/calibration data may have changed.
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

_dataprod_qc(data::LegendData, sel::AnyValiditySelection) = _dataprod_config(data, sel, :qc)

function _dataprod_qc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    dataprod_qc = _dataprod_qc(data, sel)
    usability = channelinfo(data, sel, detector).usability
    _merge_copies(
        dataprod_qc.default,
        get(dataprod_qc, usability, PropDict()),
        get(dataprod_qc, detector, PropDict()),
    )
end

const _cached_dataprod_qc_cuts_pf = LRU{Tuple, PropertyFunction}(maxsize = 10^3)


"""
    get_ged_qc_cuts_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the Ge-detector QC cut definitions for the given data and validity selection.

The effective QC configuration is merged in increasing order of precedence:
`default`, the detector's usability group (for example `ac`), and the detector.
"""
function get_ged_qc_cuts_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = _qc_key(data, sel, detector)
    get!(_cached_dataprod_qc_cuts_pf, key) do
        cut_def_props = _dataprod_qc(data, sel, detector).labels
        return ljl_propfunc(cut_def_props)
    end
end
export get_ged_qc_cuts_propfunc

const _cached_dataprod_is_trig_pf = LRU{Tuple, PropertyFunction}(maxsize = 10^3)

"""
    get_ged_qc_istrig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the Ge-detector trigger condition for the given data and validity selection.
"""
function get_ged_qc_is_trig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = _qc_key(data, sel, detector)
    get!(_cached_dataprod_is_trig_pf, key) do
        is_trig_def_props = _dataprod_qc(data, sel, detector).is_trig
        return ljl_propfunc(is_trig_def_props)
    end
end
export get_ged_qc_is_trig_propfunc

const _cached_dataprod_is_single_pulse_pf = LRU{Tuple, PropertyFunction}(maxsize = 10^3)

"""
    get_ged_qc_is_single_pulse_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get a `PropertyFunction` that returns `true` for events that fulfill the effective
`is_single_pulse` definition. Usability-group and detector-specific QC overrides
are applied while resolving the detector's QC configuration.
"""
function get_ged_qc_is_single_pulse_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = _qc_key(data, sel, detector)
    get!(_cached_dataprod_is_single_pulse_pf, key) do
        is_single_pulse_def_props = _dataprod_qc(data, sel, detector).is_single_pulse
        return ljl_propfunc(is_single_pulse_def_props)
    end
end
export get_ged_qc_is_single_pulse_propfunc

const _cached_dataprod_is_empty_trace_pf = LRU{Tuple, PropertyFunction}(maxsize = 10^3)

"""
    get_ged_qc_is_empty_trace_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get a `PropertyFunction` that returns `true` for events that fullfill the `is_empty_trace` definition.
"""
function get_ged_qc_is_empty_trace_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = _qc_key(data, sel, detector)
    get!(_cached_dataprod_is_empty_trace_pf, key) do
        is_empty_trace_def_props = _dataprod_qc(data, sel, detector).is_empty_trace
        return ljl_propfunc(is_empty_trace_def_props)
    end
end
export get_ged_qc_is_empty_trace_propfunc

const _cached_dataprod_is_crosstalk_pf = LRU{Tuple, PropertyFunction}(maxsize = 10^3)

"""
    get_ged_qc_is_crosstalk_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get a `PropertyFunction` that returns `true` for events that fullfill the `is_crosstalk` definition.
"""
function get_ged_qc_is_crosstalk_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    key = _qc_key(data, sel, detector)
    get!(_cached_dataprod_is_crosstalk_pf, key) do
        is_crosstalk_def_props = _dataprod_qc(data, sel, detector).is_crosstalk
        return ljl_propfunc(is_crosstalk_def_props)
    end
end
export get_ged_qc_is_crosstalk_propfunc


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
    LegendDataManagement.dataprod_pars_lq_window(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, lq_classifier::Symbol; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)

Get the LQ cut window for the given data, validity selection and detector.
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
    LegendDataManagement.get_ged_lq_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:lq)

Get the LQ cut propfuncs for the given data, validity selection and detector.
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
    get_ged_psd_classifier_propfunc(data::LegendData, sel::AnyValiditySelection)

Get the PSD cut propfuncs for the given data and validity selection.
"""
function get_ged_psd_classifier_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    dataprod_psd_config = _dataprod_psd(data, sel).psd
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    ljl_propfunc(_merge_copies(dataprod_psd_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_psd_config, :p, PropDict()), dataprod_psd_config), detector, PropDict())).psd_classifier)
end
export get_ged_psd_classifier_propfunc


### SiPM LAr cal functions

_dataprod_spms_cal(data::LegendData, sel::AnyValiditySelection) = _dataprod_config(data, sel, :sipm)

function _dataprod_lar_cal(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars)
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    dataprod_lar_config = _dataprod_spms_cal(data, sel).lar
    _merge_copies(dataprod_lar_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_lar_config, :p, PropDict()), dataprod_lar_config), detector, PropDict()))
end

function _get_larcal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:ppars, pars_cat::Symbol=:sipmcal)
    _get_cal_values(data, sel, pars_type, pars_cat)
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
    get_spm_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the LAr/SPMS calibration function for the given data, validity selection
and detector.
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


"""
    get_spm_noise_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the LAr/SPMS noise cut function for the given data, validity selection
and detector. For each energy type it keeps only the triggers above the
`trigger_threshold_cal` of the detector in the calibrated trigger amplitudes,
trigger positions and discharge flags.
"""
function get_spm_noise_cut_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:ppars, pars_cat::Symbol=:sipmcal)
    energy_types = _dataprod_lar_cal(data, sel, detector; pars_type=pars_type).energy_types
    larcal_props = _get_larcal_props(data, sel, detector; pars_type=pars_type, pars_cat=pars_cat)
    expr_map = Dict{Symbol, String}()
    for e_filter in keys(energy_types)
        haskey(get(larcal_props, e_filter, PropDict()), :trigger_threshold_cal) || throw(ArgumentError("No trigger_threshold_cal for $e_filter of $detector in $pars_type $pars_cat"))
        noise_cut = "$(e_filter)_cal .> $(ustrip(UnitfulAtomic.e_au, larcal_props[e_filter].trigger_threshold_cal))e"
        for col in (Symbol("$(e_filter)_cal"), Symbol(energy_types[e_filter].pos), Symbol("$(e_filter)_is_dc"))
            expr_map[col] = "$(col)[$(noise_cut)]"
        end
    end
    return ljl_propfunc(expr_map)
end
export get_spm_noise_cut_propfunc


### PMT Muon cal functions

_dataprod_pmt_cal(data::LegendData, sel::AnyValiditySelection) = _dataprod_config(data, sel, :pmt)

function _dataprod_muon_cal(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; pars_type::Symbol=:rpars)
    @assert pars_type in (:ppars, :rpars) "pars_type must be either :ppars or :rpars"
    dataprod_muon_config = _dataprod_pmt_cal(data, sel).muon
    _merge_copies(dataprod_muon_config[ifelse(pars_type == :ppars, :p_default, :default)], get(ifelse(pars_type == :ppars, get(dataprod_muon_config, :p, PropDict()), dataprod_muon_config), detector, PropDict()))
end

function _get_pmt_is_physical_trig_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    is_physical_trig_props::String = _dataprod_muon_cal(data, sel, detector; kwargs...).energy_types[e_filter].is_physical_trig
    return is_physical_trig_props
end

function _get_pmtcal_props(data::LegendData, sel::AnyValiditySelection; pars_type::Symbol=:rpars, pars_cat::Symbol=:pmtcal)
    mkpath(data_path(dataprod_parameters(data)[pars_type][pars_cat]))
    _get_cal_values(data, sel, pars_type, pars_cat)
end

function _get_pmtcal_props(data::LegendData, sel::AnyValiditySelection, detector::DetectorId; kwargs...)
    _get_pmtcal_props(data, sel; kwargs...)[Symbol(detector)]
end

function _get_pmtcal_propfunc_str(data::LegendData, sel::AnyValiditySelection, detector::DetectorId, e_filter::Symbol; kwargs...)
    ecal_props::String = get(get(get(_get_pmtcal_props(data, sel, detector; kwargs...), e_filter, PropDict()), :cal, PropDict()), :func, "e_fc .* (NaN*e)")
    return ecal_props
end


"""
    get_pmt_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the PMT calibration function for the given data, validity selection
and detector.
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
    get_pmt_is_physical_trig_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)

Get the PMT physical trigger condition for the given data and validity selection.
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
