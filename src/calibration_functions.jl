# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const _cached_get_ecal_props = LRU{Tuple{UInt, ValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_ecal_props(data::LegendData, sel::ValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_get_ecal_props, key) do
        dataprod_parameters(data).cal.energy(sel)
    end
end

function _get_ecal_props(data::LegendData, sel::ValiditySelection, detector::DetectorId)
    _get_ecal_props(data, sel)[Symbol(detector)]
end


const _cached_get_psdcal_props = LRU{Tuple{UInt, ValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_psdcal_props(data::LegendData, sel::ValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_get_psdcal_props, key) do
        dataprod_parameters(data).cal.psd(sel)
    end
end

function _get_psdcal_props(data::LegendData, sel::ValiditySelection, detector::DetectorId)
    _get_psdcal_props(data, sel)[Symbol(detector)].calibration
end

#=
function _get_e_cal_function(data::LegendData, sel::ValiditySelection, channel::ChannelId, e_filter::Symbol)
    detector = channelinfo(data, sel, channel).detector
    _get_e_cal_function(data, sel, detector, e_filter)
end
=#

function _get_e_cal_function(data::LegendData, sel::ValiditySelection, detector::DetectorId, e_filter::Symbol)
    ecal_props = _get_ecal_props(data, sel, detector)
    cal_pars = ecal_props[e_filter].energy

    cal_slope::Float64 = get(cal_pars, :m_calib, NaN)
    cal_offset::Float64 = get(cal_pars, :n_calib, NaN)

    let cal_slope = cal_slope * u"keV", cal_offset = cal_offset * u"keV"
        return _calib_energy(e_uncal::Real) = cal_slope * e_uncal + cal_offset
    end
end


#=
function get_e_cal_propfunc(data::LegendData, sel::ValiditySelection, channel::ChannelId, e_filter::Symbol)
    get_e_cal_propfunc(data, sel, channel, Val(e_filter))
end
export get_e_cal_propfunc

for e_filter in (:e_trap, :e_cusp, :e_zac)
    e_col_sym = Expr(:$, e_filter)
    @eval begin
        function get_e_cal_propfunc(data::LegendData, sel::ValiditySelection, channel::ChannelId, ::Val{$(Meta.quot(e_filter))})
            let _calib_energy = _get_e_cal_function(data, sel, channel, $(Meta.quot(e_filter)))
                @pf _calib_energy($e_col_sym)
            end
        end
    end
end
=#


#=
function _get_e_ctc_cal_function(data::LegendData, sel::ValiditySelection, channel::ChannelId, e_filter::Symbol)
    detector = channelinfo(data, sel, channel).detector
    _get_e_ctc_cal_function(data, sel, detector, e_filter)
end
=#

function _get_e_ctc_cal_function(data::LegendData, sel::ValiditySelection, detector::DetectorId, e_filter::Symbol)
    e_filter_ctc = Symbol("$(e_filter)_ctc")
    ecal_props = _get_ecal_props(data, sel, detector)
    ctc_pars = ecal_props[e_filter].ctc
    post_ctc_cal_pars = ecal_props[e_filter_ctc].energy

    fct::Float64 = get(ctc_pars, :fct, NaN)
    cal_slope::Float64 = get(post_ctc_cal_pars, :m_calib, NaN)
    cal_offset::Float64 = get(post_ctc_cal_pars, :n_calib, NaN)

    let cal_slope = cal_slope * u"keV", cal_offset = cal_offset * u"keV", fct = fct
        return function _calib_energy(e_uncal::Real, qdrift::Real)
            cal_slope * (e_uncal + fct * qdrift) + cal_offset
        end
    end
end

#=
function get_e_ctc_cal_propfunc(data::LegendData, sel::ValiditySelection, channel::ChannelId, e_filter::Symbol)
    get_e_ctc_cal_propfunc(data, sel, channel, Val(e_filter))
end
export get_e_ctc_cal_propfunc

for e_filter in (:e_trap, :e_cusp, :e_zac)
    e_col_sym = Expr(:$, e_filter)
    qdrift_sym = Expr(:$, :qdrift)
    @eval begin
        function get_e_ctc_cal_propfunc(data::LegendData, sel::ValiditySelection, channel::ChannelId, ::Val{$(Meta.quot(e_filter))})
            let _calib_energy = _get_e_ctc_cal_function(data, sel, channel, $(Meta.quot(e_filter)))
                @pf _calib_energy($e_col_sym, $qdrift_sym)
            end
        end
    end
end
=#


#=
function _get_aoe_cal_function(data::LegendData, sel::ValiditySelection, channel::ChannelId)
    detector = channelinfo(data, sel, channel).detector
    _get_aoe_cal_function(data, sel, detector)
end
=#

_f_aoe_sigma(x, p) = sqrt(abs(p[1]) + abs(p[2])/x^2)
_f_aoe_mu(x, p) = p[1] .+ p[2]*x


function _get_aoe_cal_function(data::LegendData, sel::ValiditySelection, detector::DetectorId, e_filter::Symbol)
    ecal_props = _get_ecal_props(data, sel, detector)
    cal_pars = ecal_props[e_filter].energy
    psdcal_props = _get_psdcal_props(data, sel, detector)

    cal_slope::Float64 = get(cal_pars, :m_calib, NaN)
    cal_offset::Float64 = get(cal_pars, :n_calib, NaN)
    μ_scs::NTuple{2,Float64} = (get(psdcal_props, :μ_scs, fill(NaN, 2))...,)
    σ_scs::NTuple{2,Float64} = (get(psdcal_props, :σ_scs, fill(NaN, 2))...,)

    let cal_slope = cal_slope, cal_offset = cal_offset,
        μ_scs = μ_scs, σ_scs = σ_scs

        return function _calib_aoe(e_uncal::Real, a_uncal::Real)
            e_cal = cal_slope * e_uncal + cal_offset
            aoe_raw = a_uncal / e_cal 
            aoe_corrected = aoe_raw / _f_aoe_mu(e_cal, μ_scs) - 1
            aoe_classifier = aoe_corrected / _f_aoe_sigma(e_cal, σ_scs)
            (aoe_raw = aoe_raw, aoe_corrected = aoe_corrected, aoe_classifier = aoe_classifier)
        end
    end
end



"""
    get_ged_cal_propfunc(data::LegendData, sel::ValiditySelection, detector::DetectorId)

Get the HPGe calibration function for the given data, validity selection and
detector.

Returns a `PropertyFunction` that takes a table-like data object with columns
`e_trap`, `e_cusp`, `e_zac` and `qdrift` and returns a `StructArrays` with
columns `e_trap_cal`, `e_cusp_cal`, `e_zac_cal`, `e_trap_ctc_cal`,
`e_cusp_ctc_cal` and `e_zac_ctc_cal`.

Note: Caches configuration/calibration data internally, use a fresh `data`
object if on-disk configuration/calibration data may have changed.
"""
function get_ged_cal_propfunc(data::LegendData, sel::ValiditySelection, detector::DetectorId)
    let cf_trap = _get_e_cal_function(data, sel, detector, :e_trap),
        cf_cusp = _get_e_cal_function(data, sel, detector, :e_cusp),
        cf_zac = _get_e_cal_function(data, sel, detector, :e_zac),
        cf_ctc_trap = _get_e_ctc_cal_function(data, sel, detector, :e_trap),
        cf_ctc_cusp = _get_e_ctc_cal_function(data, sel, detector, :e_cusp),
        cf_ctc_zac = _get_e_ctc_cal_function(data, sel, detector, :e_zac),
        cf_aoe = _get_aoe_cal_function(data, sel, detector, :e_trap)

        @pf begin
            # ToDo: Don't hardcode A/E reference energy source:
            aoe_ref_e_uncal = $e_trap
            aoe_raw, aoe_corrected, aoe_classifier = cf_aoe(aoe_ref_e_uncal, $a)

            (
                e_trap_cal = cf_trap($e_trap),
                e_cusp_cal = cf_cusp($e_cusp),
                e_zac_cal = cf_zac($e_zac),
                e_trap_ctc_cal = cf_ctc_trap($e_trap, $qdrift),
                e_cusp_ctc_cal = cf_ctc_cusp($e_cusp, $qdrift),
                e_zac_ctc_cal = cf_ctc_zac($e_zac, $qdrift),
                aoe_raw = aoe_raw, aoe_corrected = aoe_corrected, aoe_classifier = aoe_classifier
            )
        end
    end
end
export get_ged_cal_propfunc

function get_ged_cal_propfunc(data::LegendData, sel::AnyValiditySelection, detector::DetectorId)
    get_ged_cal_propfunc(data, ValiditySelection(sel), detector)
end



const _cached_dataprod_pars_p_psd = LRU{Tuple{UInt, ValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_pars_p_psd(data::LegendData, sel::ValiditySelection)
    data_id = objectid(data)
    key = (objectid(data), sel)
    get!(_cached_dataprod_pars_p_psd, key) do
        dataprod_parameters(data).cal.p_psd(sel)
    end
end

function _dataprod_pars_p_psd(data::LegendData, sel::ValiditySelection, detector::DetectorId)
    _dataprod_pars_p_psd(data, sel)[Symbol(detector)]
end


const _cached_dataprod_qc = LRU{Tuple{UInt, ValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _dataprod_qc(data::LegendData, sel::ValiditySelection)
    data_id = objectid(data)
    key = (objectid(data), sel)
    get!(_cached_dataprod_qc, key) do
        dataprod_config(data).qc(sel)
    end
end


const _cached_dataprod_qc_cuts_pf = LRU{Tuple{UInt, ValiditySelection}, PropertyFunction}(maxsize = 10^2)


# Will replace current get_ged_qc_cuts_propfunc in the future.
# Currently suffers from world-age problems due to `ljl_propfunc`.
function _get_ged_qc_cuts_propfunc_dynamic(data::LegendData, sel::ValiditySelection)
    data_id = objectid(data)
    key = (objectid(data), sel)
    get!(_cached_dataprod_qc_cuts_pf, key) do
        cut_def_props = _dataprod_qc(data, sel).default.cuts
        return ljl_propfunc(cut_def_props)
    end
end


"""
    LegendDataManagement.get_ged_qc_cuts_propfunc(data::LegendData, sel::ValiditySelection)

Hardcoded Ge-detector quality cuts.

Note: Temporary workaround for world-age problems with `ljl_propfunc`.
"""
function get_ged_qc_cuts_propfunc(data::LegendData, sel::ValiditySelection)
    # ToDo: Replace code with _get_ged_qc_cuts_propfunc_dynamic when world-age problems
    # with ljl_propfunc are fixed.

    @pf let ns = u"ns", kev = u"keV"
        (
            is_discharge = $n_sat_low > 0,
            is_downgoing_baseline = $blslope < -0.2 / (16ns),
            is_neg_energy = $e_10410_inv > 500 && ($t0_inv > 1000ns && !($e_10410_inv > 100 && ($t0_inv > 45000ns && $t0_inv < 55000ns))),
            is_negative_crosstalk = $e_10410_inv > 100 && ($t0_inv > 45000ns && $t0_inv < 55000ns),
            is_noise_burst = $e_min - $blmean < -300 && ($e_max - $blmean > 300 && abs($rt1090) < 800ns),
            is_nopileup = !($inTrace_intersect > $t0 + 2 * $drift_time && $inTrace_n > 1) || ($e_cusp_ctc_cal < 25kev || $e_cusp_ctc_cal != $e_cusp_ctc_cal),
            is_saturated = $n_sat_high > 0,
            is_upgoing_baseline = $blslope > 0.2 / (16ns),
            is_valid_baseline = $blsigma < 20,
            is_valid_dteff = $qdrift / $e_10410 > -30 || ($e_cusp_ctc_cal < 25kev || $e_cusp_ctc_cal != $e_cusp_ctc_cal),
            is_valid_ediff = abs($e_10410 - $e_313) > 100 || ($e_cusp_ctc_cal < 25kev || ($e_cusp_ctc_cal != $e_cusp_ctc_cal || $n_sat_high > 0)),
            is_valid_efrac = $e_10410 / $e_313 > 0.95 && $e_10410 / $e_313 < 1.1 || ($e_cusp_ctc_cal < 25kev || ($e_cusp_ctc_cal != $e_cusp_ctc_cal || $n_sat_high > 0)),
            is_valid_rt = $rt1090 > 96ns && $t50 - $t0 >= 16ns || ($e_cusp_ctc_cal < 25kev || $e_cusp_ctc_cal != $e_cusp_ctc_cal),
            is_valid_t0 = $t0 > 47000ns && $t0 < 55000ns || ($e_cusp_ctc_cal < 25kev || $e_cusp_ctc_cal != $e_cusp_ctc_cal),
            is_valid_tail = $tailsigma < 75 || (abs($blslope / $tailslope) < 0.4 && abs($blslope / $tailslope) > 2.5 || ($e_10410_inv > 100 && ($t0_inv > 45000ns && $t0_inv < 55000ns) || $n_sat_high > 0)),
        )
    end
end 


# ToDo: Make configurable
"""
    get_ged_qc_isgood_propfunc(data::LegendData, sel::ValiditySelection)

Get a `PropertyFunction` that returns `true` for events that pass the
Ge-detector quality cuts.
"""
function get_ged_qc_isgood_propfunc(data::LegendData, sel::ValiditySelection)
    @pf begin
        !$is_discharge && !$is_downgoing_baseline && !$is_neg_energy &&
        !$is_negative_crosstalk && !$is_noise_burst && $is_nopileup &&
        !$is_saturated && !$is_upgoing_baseline && $is_valid_baseline &&
        $is_valid_dteff && $is_valid_ediff && $is_valid_efrac &&
        $is_valid_rt && $is_valid_t0 && $is_valid_tail
    end
end


"""
    LegendDataManagement.dataprod_pars_aoe_window(data::LegendData, sel::ValiditySelection, detector::DetectorId)

Get the A/E cut window for the given data, validity selection and detector.
"""
function dataprod_pars_aoe_window(data::LegendData, sel::ValiditySelection, detector::DetectorId)
    aoecut_lo::Float64 = get(_dataprod_pars_p_psd(data, sel, detector).aoecut, :lo, NaN)
    aoecut_hi::Float64 = get(_dataprod_pars_p_psd(data, sel, detector).aoecut, :hi, NaN)   
    ClosedInterval(aoecut_lo, aoecut_hi) 
end



const _cached_get_larcal_props = LRU{Tuple{UInt, ValiditySelection}, Union{PropDict,PropDicts.MissingProperty}}(maxsize = 10^3)

function _get_larcal_props(data::LegendData, sel::ValiditySelection)
    key = (objectid(data), sel)
    get!(_cached_get_larcal_props, key) do
        dataprod_parameters(data).phy.p_sipm(sel)
    end
end

function _get_larcal_props(data::LegendData, sel::ValiditySelection, detector::DetectorId)
    _get_larcal_props(data, sel)[Symbol(detector)]
end


"""
    get_spm_cal_propfunc(data::LegendData, sel::ValiditySelection, detector::DetectorId)

Get the LAr/SPMS calibration function for the given data, validity selection
and detector.
"""
function get_spm_cal_propfunc(data::LegendData, sel::ValiditySelection, detector::DetectorId)
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


"""
    get_pulser_cal_propfunc(data::LegendData, sel::ValiditySelection, detector::DetectorId)

Get the pulser calibration function for the given data, validity selection
and the pulser channel referred to by `detector`.
"""
function get_pulser_cal_propfunc(data::LegendData, sel::ValiditySelection, detector::DetectorId)
    # ToDo: Make pulser threashold configurable:
    let pulser_threshold = 100
        @pf (
            puls_trig = $e_10410 > pulser_threshold,
        )
    end
end
export get_pulser_cal_propfunc
