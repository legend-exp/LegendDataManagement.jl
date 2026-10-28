# # A/E Calibration

# This tutorial calibrates and cuts the A/E pulse-shape parameter of one germanium
# detector from a calibration run with a Th-228 source. A/E is the ratio of the
# maximum current amplitude A to the energy E of a waveform. Multi-site events
# have a lower A/E than single-site events, so a cut on A/E separates them. The tutorial starts from the energy calibration stored by the
# [Energy Calibration](@ref) tutorial and uses
# [LegendSpecFits.jl](https://github.com/legend-exp/LegendSpecFits.jl) for the fits.

# The LegendTestData files used by the live examples of this page contain only
# a handful of events. The steps that read data and handle parameter files run
# on them. The fitting steps need the statistics of a real calibration run of a
# LEGEND data production, so they are shown as code that is not executed here.

#md # ```@setup aoe
#md # include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
#md # ```

# ## Setup

# The environment variable `LEGEND_DATA_CONFIG` points to the configuration files
# of the data production, as described in the [Energy Calibration](@ref) tutorial.

# ```julia
# ENV["LEGEND_DATA_CONFIG"] = "<path-to-production>/config.json"
#
# using LegendDataManagement
# using LegendHDF5IO
# using LegendSpecFits
# using PropDicts
# using PropertyFunctions
# using TypedTables
# using Unitful
# using Measurements
# using StatsBase
# using Statistics
# ```

# The live examples need all of these packages except LegendSpecFits.jl, StatsBase.jl and Statistics.

using LegendDataManagement
using LegendHDF5IO
using PropDicts
using PropertyFunctions
using TypedTables
using Unitful
using Measurements

l200 = LegendData(:l200);

# ## Selecting a Run, a Detector and a Partition

# The channel information of a detector holds its A/E status and the data partition it
# belongs to. We use the metadata-only toy period `p02` of the test data; `mfk` is a file key of the toy production in the test metadata (period 2, run 6, category `cal`).

det_meta = DetectorId(:V99000A)
chinfo_det = channelinfo(l200, mfk, det_meta)
(chinfo_det.detector, chinfo_det.low_aoe_status, chinfo_det.partition)

# A/E parameters are determined for a data partition, which groups runs of one
# detector. [`partitioninfo`](@ref) lists the runs of the partition.

partinfo = partitioninfo(l200, det_meta, chinfo_det.partition)
partinfo.run

# !!! note "Metadata and tier files of the test data"
#     The test data hold metadata for the toy period `p02` and tier files for
#     period `p18`. `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`) and `det` a detector ID with data in these files. The following steps read the tier files with `fk` and `det`.
#     The partition runs of the toy period only demonstrate the selection; the
#     tutorial uses the data of the first file `fk` only.

# For the calibration run of a production, the selection is the same as in the
# [Energy Calibration](@ref) tutorial. The partition comes from the channel information. In the code
# below, `period`, `run`, `filekey` and `det` refer to the selected calibration run
# and one of its detectors, and `out_dir` is the directory that holds the files
# written by the energy tutorial. The code reads the data of the selected run only. The rows of `partinfo` list the
# other runs of the partition.

# ```julia
# period, run = DataPeriod(<period>), DataRun(<run>)
# filekey = start_filekey(l200, (period, run, :cal))
# det = :<detector>
# chinfo_det = channelinfo(l200, filekey, det)
# partinfo = partitioninfo(l200, det, chinfo_det.partition)
# out_dir = joinpath(tempdir(), "ldm_tutorial")
# ```

# ## Reading Data and Applying the Energy Calibration

# The energy calibration is read from the parameter file written by the energy
# tutorial. To keep this page self-contained, we first write a stand-in file with the same
# layout (detector, energy type, `cal.func`) and an arbitrary calibration function. `scratch_dir` is a temporary directory for the files that the examples write.

ecal_func = "0.0037keV .* (e_cusp .- 0.0001 .* qdrift).^1"
ecal_file = joinpath(scratch_dir, "ecal.yaml")
writelprops(ecal_file, PropDict(Symbol(det) => PropDict(:e_cusp_ctc => PropDict(:cal => PropDict(:func => ecal_func)))))

# The A/E configuration names the energy type that the analysis uses, `e_cusp_ctc_cal`.
# It is the calibrated energy of the stored entry `e_cusp_ctc`. The stored function is
# compiled with [`ljl_propfunc`](@ref).

pars_ecal = readlprops(ecal_file)
e_type = :e_cusp_ctc_cal
e_type_uncal = :e_cusp_ctc
cal_func = ljl_propfunc(pars_ecal[det][e_type_uncal].cal.func);

# `read_ldata` also accepts a function of the event row as its first argument. Here
# the function adds the calibrated energy as a column in front of all DSP columns,
# which stay available for the A/E parameters.

hit_cal = read_ldata(row -> merge(NamedTuple{(e_type,)}((cal_func(row),)), row), l200, :jldsp, fk, det)
e_cal = getproperty(hit_cal, e_type)
first(e_cal, 3)

# The events of a calibration run are selected with the quality-cut flags of the `jlqcs`
# tier. This step is not executed because the test data have no `jlqcs` tier.

# ```julia
# pars_ecal = readlprops(joinpath(out_dir, "ecal.yaml"))
# e_type = Symbol(aoe_config_det.e_type)                      # e_cusp_ctc_cal
# e_type_uncal = Symbol(first(split(string(e_type), "_cal"))) # e_cusp_ctc
# cal_func = ljl_propfunc(pars_ecal[det][e_type_uncal].cal.func)
#
# qc_mask = read_ldata(@pf($is_single_pulse && !$is_pulser), l200, :jlqcs, filekey, det)
# hit_cal = read_ldata(row -> merge(NamedTuple{(e_type,)}((cal_func(row),)), row), l200, :jldsp, :cal, period, run, det)[qc_mask]
# e_cal = getproperty(hit_cal, e_type)
# e_unit = unit(eltype(e_cal))
# ```

# ## Configuration

# The A/E configuration is part of the PSD configuration of the data production. It has
# a section `p_default` for the parameters that are determined per partition and sections for
# individual detectors in `p`. The test metadata contain no PSD configuration, so this step is not executed.

# ```julia
# aoe_config = dataprod_config(l200).psd(filekey).aoe
# aoe_config_det = merge(aoe_config.p_default, get(aoe_config.p, det, PropDict()))
#
# compton_bands = aoe_config_det.compton_bands
# compton_window = aoe_config_det.compton_window
# p_value_cut = aoe_config_det.p_value
# aoe_funcs = aoe_config_det.aoe_funcs
# qdrift_expression = aoe_config_det.qdrift_expression
# sigma_high_sided = aoe_config_det.sigma_high_sided
#
# aoe_type = :aoe_sg
# ```

# ## Raw A/E

# `aoe_funcs` maps the name of an A/E type to a LEGEND Julia expression of the DSP columns,
# such as the ratio of an amplitude column to an energy column. The expression of `:aoe_sg`
# is evaluated with [`ljl_propfunc`](@ref). On the test data, we evaluate the ratio of the DSP columns
# `a_sg` and `e_cusp`, which is the expression that the configuration of a production uses for `:aoe_sg`:

aoe_raw_expression = "a_sg / e_cusp"
aoe_raw = ljl_propfunc(aoe_raw_expression).(hit_cal)
first(aoe_raw, 3)

# The analysis normalizes A/E to the position of the peak of its distribution, so that single-site events
# have a value close to 1. `cut_single_peak` returns the position `max` of that peak in the
# distribution of the A/E values.

# ```julia
# aoe = ljl_propfunc(aoe_funcs[aoe_type]).(hit_cal)
# cuts_aoe = cut_single_peak(aoe, 0.0, quantile(filter(isfinite, aoe), 0.99); n_bins = -1)
# aoe_expression = "$(aoe_funcs[aoe_type]) / $(cuts_aoe.max)"
# aoe ./= cuts_aoe.max
# ```

# ## Compton Band Fits and A/E Correction

# The mean and the width of A/E depend on the energy. `generate_aoe_compton_bands` histograms the A/E values
# in windows of the Compton continuum, `fit_aoe_compton` fits the distribution in
# every band, and `fit_aoe_corrections` models the mean and the width as functions of
# energy. Bands whose fit did not converge or whose p-value is below the configured
# value are not used for the correction.

# ```julia
# peakhists_bands = generate_aoe_compton_bands(aoe, e_cal, compton_bands, compton_window)
# result_fit, report_fit = fit_aoe_compton(peakhists_bands.peakhists, peakhists_bands.peakstats, compton_bands; uncertainty = true)
#
# qc_compton_bands = findall(band -> band in keys(result_fit) &&
#                                    result_fit[band].gof.converged &&
#                                    result_fit[band].gof.pvalue >= p_value_cut, compton_bands)
# compton_bands_qc = compton_bands[qc_compton_bands]
# peakhists = peakhists_bands.peakhists[qc_compton_bands]
# peakstats = peakhists_bands.peakstats[qc_compton_bands]
# μ = [result_fit[band].μ for band in compton_bands_qc]
# σ = [result_fit[band].σ for band in compton_bands_qc]
#
# result_fit_single, report_fit_single = fit_aoe_corrections(compton_bands_qc, μ, σ;
#                                                            aoe_expression, e_expression = e_type)
# ```

# The configuration entry `use_combined_fit` selects a second correction that fits all bands
# simultaneously, starting from the result above. `result_correction.func` is the A/E correction
# as an expression of the DSP columns and the calibrated energy.

# ```julia
# if aoe_config_det.use_combined_fit
#     result_fit_combined, report_fit_combined = fit_aoe_compton_combined(peakhists, peakstats, compton_bands_qc, result_fit_single;
#                                                                         e_expression = e_type, aoe_expression,
#                                                                         uncertainty = true)
#     result_correction = result_fit_combined
# else
#     result_correction = result_fit_single
# end
# aoe_corr = ljl_propfunc(result_correction.func).(hit_cal)
# ```

# ## Charge-Trapping Correction of A/E

# If `apply_ctc` is set in the configuration, `ctc_aoe` corrects the A/E from the previous
# step for its dependence on the expression `qdrift_expression`.

# ```julia
# result_aoe_ctc = NamedTuple()
# if aoe_config_det.apply_ctc
#     qdrift_e = ljl_propfunc(qdrift_expression).(hit_cal)
#     result_aoe_ctc, report_aoe_ctc = LegendSpecFits.ctc_aoe(aoe_corr, e_cal, qdrift_e, compton_bands_qc;
#                                                             aoe_expression = result_correction.func, qdrift_expression)
#     aoe_corr = ljl_propfunc(result_aoe_ctc.func).(hit_cal)
# end
# aoe_func = get(result_aoe_ctc, :func, result_correction.func)
# ```

# ## Low A/E Cut

# `get_low_aoe_cut` searches the cut value for which the number of events in the
# double-escape peak (DEP) after the cut is the fraction `dep_cut_search_target_sf` of the number before the cut.
# The result contains the low cut `lowcut` and the high cut `highcut`, which is set to `sigma_high_sided`.

# ```julia
# result_cut, report_cut = get_low_aoe_cut(aoe_corr, e_cal; dep = aoe_config_det.dep, window = aoe_config_det.dep_window,
#                                          cut_search_interval = Tuple(aoe_config_det.dep_cut_search_interval),
#                                          bin_width_window = aoe_config_det.dep_bin_width_window,
#                                          rtol = aoe_config_det.dep_cut_search_rtol,
#                                          maxiters = aoe_config_det.dep_cut_search_maxiters,
#                                          dep_sf = aoe_config_det.dep_cut_search_target_sf,
#                                          fixed_position = aoe_config_det.dep_cut_search_fixed_position,
#                                          sigma_high_sided,
#                                          fit_func = Symbol(aoe_config_det.dep_cut_search_fit_func), uncertainty = true)
# ```

# ## Survival Fractions

# The survival fraction of a peak is the fraction of its events that pass the cut.
# `get_peaks_survival_fractions` fits the peaks listed in the configuration before and after the cut,
# and `get_continuum_survival_fraction` counts the events in an energy window of the continuum
# around the energy `qbb`.
# The cut can be one-sided (`low_cut`) or two-sided (`low_cut` and `high_cut`).

# ```julia
# peak_names = Symbol.(aoe_config_det.aoe_peaks_names)
# peak_fit_funcs = Symbol.(aoe_config_det.aoe_peaks_fit_funcs)
#
# # Low cut only
# result_peaks_low, report_peaks_low = get_peaks_survival_fractions(aoe_corr, e_cal, aoe_config_det.aoe_peaks, peak_names,
#                                                                   aoe_config_det.aoe_peaks_windows_left,
#                                                                   aoe_config_det.aoe_peaks_windows_right;
#                                                                   low_cut = result_cut.lowcut,
#                                                                   bin_width_window = aoe_config_det.aoe_peaks_bin_width_window,
#                                                                   fit_funcs = peak_fit_funcs, uncertainty = true)
# qbb_result_low, _ = get_continuum_survival_fraction(aoe_corr, e_cal, aoe_config_det.qbb, aoe_config_det.qbb_window;
#                                                     low_cut = result_cut.lowcut)
#
# # Low and high cut
# result_peaks_ds, report_peaks_ds = get_peaks_survival_fractions(aoe_corr, e_cal, aoe_config_det.aoe_peaks, peak_names,
#                                                                 aoe_config_det.aoe_peaks_windows_left,
#                                                                 aoe_config_det.aoe_peaks_windows_right;
#                                                                 low_cut = result_cut.lowcut, high_cut = result_cut.highcut,
#                                                                 bin_width_window = aoe_config_det.aoe_peaks_bin_width_window,
#                                                                 fit_funcs = peak_fit_funcs, uncertainty = true)
# qbb_result_ds, _ = get_continuum_survival_fraction(aoe_corr, e_cal, aoe_config_det.qbb, aoe_config_det.qbb_window;
#                                                    low_cut = result_cut.lowcut, high_cut = result_cut.highcut)
# ```

# ## Storing Results

# The A/E function and the cut values are stored in the same layout as the production
# parameter files: the function under the A/E type, the cut and the survival fractions
# under the classifier of that type.

# ```julia
# result_aoe = (func = aoe_func, ctc = result_aoe_ctc, correction = result_correction,
#               apply_ctc = aoe_config_det.apply_ctc, use_combined_fit = aoe_config_det.use_combined_fit)
# result_cut_all = merge(result_cut, (peaks = (low = result_peaks_low, ds = result_peaks_ds),
#                                     qbb = (low = qbb_result_low, ds = qbb_result_ds)))
# pars_aoe = PropDict(det => PropDict(aoe_type => PropDict(pairs(result_aoe)),
#                                     Symbol("$(aoe_type)_classifier") => PropDict(pairs(result_cut_all))))
# writelprops(joinpath(out_dir, "aoe.yaml"), pars_aoe)
# ```

# The same calls work on the test data. We store a hand-written A/E function and an
# arbitrary cut value in this layout.

aoe_type = :aoe_sg
aoe_func_string = "($aoe_raw_expression) / 0.0153"
pars_aoe = PropDict(
    Symbol(det) => PropDict(
        aoe_type => PropDict(:func => aoe_func_string, :apply_ctc => false),
        Symbol("$(aoe_type)_classifier") => PropDict(:lowcut => 0.9),
    ),
)
aoe_file = joinpath(scratch_dir, "aoe.yaml")
writelprops(aoe_file, pars_aoe)
print(read(aoe_file, String))

# ## Applying the Stored A/E Function and Cut

# The stored function is applied to the events with [`ljl_propfunc`](@ref), and the cut value
# selects the events whose A/E is not below the low cut.

pars_aoe_read = readlprops(aoe_file)
aoe_stored = ljl_propfunc(pars_aoe_read[det][aoe_type].func).(hit_cal)
lowcut = pars_aoe_read[det][Symbol("$(aoe_type)_classifier")].lowcut
pass_cut = aoe_stored .>= lowcut
(count(pass_cut), length(pass_cut))
