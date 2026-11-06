# # Energy Calibration

# This tutorial calibrates the energy of one germanium detector from a
# calibration run with a Th-228 source. It selects a run and a detector with
# LegendDataManagement.jl, reads the digital signal processing output, fits the
# gamma lines with [LegendSpecFits.jl](https://github.com/legend-exp/LegendSpecFits.jl),
# and stores the result as a parameter file that can be applied to the data later.

# The LegendTestData files used by the live examples of this page contain only
# a handful of events. The steps that read data and handle parameter files run
# on them. The fitting steps need the statistics of a real calibration run of a
# LEGEND data production, so they are shown as code that is not executed here.

#md # ```@setup energy
#md # include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
#md # ```

# ## Setup

# The environment variable `LEGEND_DATA_CONFIG` points to the configuration files
# of the data production. Several files can be given, separated by `:`; later
# files are merged over earlier ones. The setup of the live examples on this page sets it
# to the LegendTestData production.

# ```julia
# ENV["LEGEND_DATA_CONFIG"] = "<path-to-production>/config.json"
#
# using LegendDataManagement
# using LegendHDF5IO            # reading and writing lh5 files
# using LegendSpecFits
# using PropDicts
# using PropertyFunctions
# using TypedTables
# using Unitful
# using Measurements
# ```

# The live examples need all of these packages except LegendSpecFits.jl.

using LegendDataManagement
using LegendHDF5IO
using PropDicts
using PropertyFunctions
using TypedTables
using Unitful
using Measurements

# [`LegendData`](@ref) gives access to the metadata, the tier files and the parameter
# files of the production.

l200 = LegendData(:l200);

# ## Selecting a Run and a Detector

# [`runinfo`](@ref) lists the runs of a data period, here the runs of the metadata-only
# toy period `p02` of the test data.

rinfo = runinfo(l200, DataPeriod(2))
rinfo.run

# A calibration run is identified by its period, run number and data category.
# [`start_filekey`](@ref) returns the [`FileKey`](@ref) of the first file of that run, which
# selects the metadata that is valid for the run.

period, run = DataPeriod(2), DataRun(6)
filekey = start_filekey(l200, (period, run, :cal))

# [`channelinfo`](@ref) returns one row per channel, with the detector name, the
# channel number, the detector type, and its status in this run. We keep the
# germanium detectors that are processable and filter them with
# `filterby` from TypedTables.jl and `@pf` from PropertyFunctions.jl.

chinfo = channelinfo(l200, filekey; system = :geds, only_processable = true)
chinfo_icpc = chinfo |> filterby(@pf $det_type == :icpc && $usability == :on)
chinfo_icpc.detector

# Passing a [`DetectorId`](@ref) as the last argument selects a single row.

chinfo_det = channelinfo(l200, filekey, DetectorId(:V99000A))
(chinfo_det.detector, chinfo_det.channel, chinfo_det.det_type)

# !!! note "Metadata and tier files of the test data"
#     The test data hold metadata for the toy period `p02` and tier files for period `p18`.
#     `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`) and `det` a detector ID with data in these files. The next steps read the tier
#     files with `fk` and `det`. In a real production, the same `filekey` and `det` serve
#     for the metadata and for the data.

# For the calibration run of a production, the selection is the same with the
# run chosen by the analyst. In the code below, `period`, `run`, `filekey` and `det`
# refer to the selected calibration run and one of its detectors.

# ```julia
# period, run = DataPeriod(<period>), DataRun(<run>)
# filekey = start_filekey(l200, (period, run, :cal))
# chinfo = channelinfo(l200, filekey; system = :geds, only_processable = true)
# det = :<detector>
# ```

# ## Reading the Tier Data

# [`read_ldata`](@ref) reads tier files. The data of one detector in one file of the
# `jldsp` tier is a table with one row per event.

dsp = read_ldata(l200, :jldsp, fk, det)
length(dsp), length(propertynames(dsp))

# A tuple of property names reads only those columns. We use the CUSP-filter
# energy `e_cusp` in ADC units and the column `qdrift`, which the
# charge-trapping correction needs.

data_all = read_ldata((:e_cusp, :qdrift), l200, :jldsp, fk, det)
data_all[1:3]

# With a property function as the first argument, `read_ldata` evaluates the
# function for every event and returns the results. A boolean function gives an event
# mask that selects events of the table.

mask = read_ldata(@pf($e_cusp > 10000.0), l200, :jldsp, fk, det)
data = data_all[mask]
length(data)

# In a production with quality-cut flags, the `jlqcs` tier holds a boolean flag
# per event and quality cut. The analysis keeps single-pulse events that are not
# pulser events. The DSP data of the whole calibration run is read for the
# detector by period, run and category. The test data have no `jlqcs` tier, so this step is not executed:

# ```julia
# qcs = read_ldata(l200, :jlqcs, filekey, det)
# propertynames(qcs)   # the available quality-cut flags
#
# qc_mask = read_ldata(@pf($is_single_pulse && !$is_pulser), l200, :jlqcs, filekey, det)
# data = read_ldata((:e_cusp, :qdrift), l200, :jldsp, :cal, period, run, det)[qc_mask]
# ```

# ## Configuration

# The analysis parameters, such as the gamma lines, the fit windows and the
# polynomial orders, are part of the data production metadata and are read with
# [`dataprod_config`](@ref). The energy configuration holds a `default` entry and
# entries for individual detectors. A detector uses its own entry merged over the
# default. The charge-trapping correction has its own `ctc` section with the same layout.
# The test metadata contain no energy configuration, so this step is not executed.

# ```julia
# energy_config = dataprod_config(l200).energy(filekey)
# energy_config_det = merge(energy_config.default, get(energy_config, det, PropDict()))
# ctc_config_det = merge(energy_config.ctc.default, get(energy_config.ctc, det, PropDict()))
#
# e_type = :e_cusp
# th228_names = Symbol.(energy_config_det.th228_names)
# th228_lines_dict = Dict(th228_names .=> energy_config_det.th228_lines)
# quantile_perc = energy_config_det.quantile_perc isa String ?
#                 parse(Float64, energy_config_det.quantile_perc) : energy_config_det.quantile_perc
# ```

# ## Charge-Trapping Correction

# Incomplete charge collection (charge trapping) lowers the measured energy of some events.
# `ctc_energy` fits a polynomial correction that uses the column `qdrift` to a
# peak of the spectrum. It needs a first calibration to locate the peak, so we run
# `simple_calibration` on the uncorrected energy. `simple_calibration` locates the
# gamma lines in the spectrum and returns the preliminary calibration factor `c`.

# ```julia
# e_uncal = getproperty(data, e_type)
# result_simple0, _ = simple_calibration(e_uncal, energy_config_det.th228_lines, energy_config_det.left_window_sizes,
#                                        energy_config_det.right_window_sizes; calib_type = :th228, quantile_perc,
#                                        binning_peak_window = energy_config_det.binning_peak_window)
# m_cal_simple0 = result_simple0.c
#
# result_ctc, report_ctc = ctc_energy(e_uncal .* m_cal_simple0, data.qdrift, ctc_config_det.peak,
#                                     (ctc_config_det.left_window_size, ctc_config_det.right_window_size), m_cal_simple0;
#                                     e_expression = "$e_type", pol_order = ctc_config_det.ctc_order)
# ```

# `result_ctc.func` is the correction as a LEGEND Julia expression of the DSP
# columns. [`ljl_propfunc`](@ref) compiles such an expression to a function of one event, which
# we broadcast over the table to get the corrected energy.

# ```julia
# e_type_ctc = Symbol("$(e_type)_ctc")
# e_uncal_ctc = ljl_propfunc(result_ctc.func).(data)
# ```

# The same mechanism works on the test data with a hand-written expression (the
# coefficient is arbitrary):

ctc_func = "e_cusp .- 0.0001 .* qdrift"
e_ctc = ljl_propfunc(ctc_func).(data)
first(e_ctc, 3)

# ## Simple Calibration

# We repeat the simple calibration on the corrected energy. The result contains
# the histograms and the start values of the peak fits.

# ```julia
# result_simple, report_simple = simple_calibration(e_uncal_ctc, energy_config_det.th228_lines,
#                                                   energy_config_det.left_window_sizes,
#                                                   energy_config_det.right_window_sizes; calib_type = :th228,
#                                                   quantile_perc, binning_peak_window = energy_config_det.binning_peak_window)
# m_cal_simple = result_simple.c
# ```

# Every fit function returns a result and a report. The report can be plotted
# with `lplot` from LegendMakie.jl (not executed here; the plots of this tutorial need
# `using CairoMakie, LegendMakie`):

# ```julia
# using CairoMakie, LegendMakie
# lplot(report_simple, cal = true, title = "Simple Calibration")
# lplot(report_ctc, figsize = (600, 600), title = "CTC")
# ```

# ## Peak Fits

# `fit_peaks` fits every gamma line with the peak shape named in the configuration.

# ```julia
# result_fit, report_fit = fit_peaks(result_simple.peakhists, result_simple.peakstats, th228_names;
#                                    e_unit = result_simple.unit, calib_type = :th228,
#                                    fit_func = Symbol.(energy_config_det.th228_fit_func), m_cal_simple)
# ```

# The configuration defines quality criteria for the fits (`qc.fit`, a LEGEND Julia expression of the
# fit result) and limits on the fitted width. Only peaks that pass are used for the calibration curve.

# ```julia
# fit_qc = energy_config_det.qc.fit
# fwhm_cut = [result_fit[th228_names[i]].fwhm .* m_cal_simple > energy_config_det.qc.min_fwhm &&
#             result_fit[th228_names[i]].fwhm .* m_cal_simple <
#             energy_config_det.qc.max_fwhm_per_window *
#             (energy_config_det.left_window_sizes[i] + energy_config_det.right_window_sizes[i])
#             for i in eachindex(th228_names)]
# qc_cut = ljl_propfunc(fit_qc).([result_fit[k] for k in th228_names]) .&& fwhm_cut
# th228_names_qc = th228_names[qc_cut]
#
# lplot(report_fit, figsize = (600, 400 * length(report_fit)), title = "Peak Fits")
# ```

# ## Calibration Curve

# `fit_calibration` fits a polynomial between the fitted peak positions and the
# literature energies of the lines. Peaks listed in `cal_fit_excluded_peaks` are left out.
# The `e_expression` argument is the expression of the energy that the calibration
# function is applied to, so `result_calib.func` contains the charge-trapping correction.

# ```julia
# th228_names_qc_cal_fit = [p for p in th228_names_qc if !(p in Symbol.(energy_config_det.cal_fit_excluded_peaks))]
# result_calib, report_calib = fit_calibration(energy_config_det.cal_pol_order,
#                                              [result_fit[p].centroid for p in th228_names_qc_cal_fit],
#                                              [th228_lines_dict[p] for p in th228_names_qc_cal_fit];
#                                              e_expression = result_ctc.func)
#
# lplot(report_calib, xerrscaling = 100, title = "Calibration Curve")
# ```

# ## Energy Resolution

# The fitted peak widths are converted from ADC units to keV with the
# calibration curve, and `fit_fwhm` fits the resolution as a function of energy.
# The result includes the resolution at the Q-value of the decay, `result_fwhm.qbb`.

# ```julia
# f_cal_widths(x) = report_calib.f_fit(x) .* report_calib.e_unit .- first(report_calib.par)
# th228_names_qc_fwhm_fit = [p for p in th228_names_qc if !(p in Symbol.(energy_config_det.fwhm_fit_excluded_peaks))]
# result_fwhm, report_fwhm = fit_fwhm(energy_config_det.fwhm_pol_order,
#                                     [th228_lines_dict[p] for p in th228_names_qc_fwhm_fit],
#                                     f_cal_widths.([result_fit[p].fwhm for p in th228_names_qc_fwhm_fit]);
#                                     e_type_cal = Symbol("$(e_type_ctc)_cal"), e_expression = result_ctc.func,
#                                     uncertainty = true)
#
# lplot(report_fwhm, title = "FWHM")
# ```

# ## Calibrated Spectrum

# The calibration function `result_calib.func` is again a LEGEND Julia expression.
# Applied to the table, it returns the energy in keV:

# ```julia
# e_cal = ljl_propfunc(result_calib.func).(data)
# ```

# ## Storing Results

# Parameter files hold `PropDict`s from [PropDicts.jl](https://github.com/oschulz/PropDicts.jl). The production
# layout is detector, then energy type, then the results. [`writelprops`](@ref) writes
# quantities with units and values with uncertainties as entries with the fields `val`, `unit`
# and `err`, and [`readlprops`](@ref) converts them back.

# ```julia
# out_dir = joinpath(tempdir(), "ldm_tutorial")
# mkpath(out_dir)
#
# result_energy = (m_cal_simple = m_cal_simple, fwhm = result_fwhm, cal = result_calib, fit = result_fit)
# pars_ctc = PropDict(det => PropDict(e_type => PropDict(pairs(result_ctc))))
# pars_ecal = PropDict(det => PropDict(e_type_ctc => PropDict(pairs(result_energy))))
#
# writelprops(joinpath(out_dir, "ctc.yaml"), pars_ctc)
# writelprops(joinpath(out_dir, "ecal.yaml"), pars_ecal)
# ```

# The same calls work on the test data. We store a hand-written calibration with a
# simple-calibration factor that has an uncertainty and a unit, in the layout of the
# fit result (the numbers are arbitrary). `scratch_dir` is a temporary directory for the files that the examples write.

ecal_func = "-0.5keV .* ($ctc_func).^0 .+ 0.0037keV .* ($ctc_func).^1"
pars_ecal = PropDict(
    Symbol(det) => PropDict(
        :e_cusp_ctc => PropDict(
            :m_cal_simple => (0.0037 ± 0.0001) * u"keV",
            :cal => PropDict(:func => ecal_func),
        ),
    ),
)
ecal_file = joinpath(scratch_dir, "ecal.yaml")
writelprops(ecal_file, pars_ecal)
print(read(ecal_file, String))

# [`readlprops`](@ref) restores the quantity with its uncertainty:

pars_ecal_read = readlprops(ecal_file)
pars_ecal_read[det].e_cusp_ctc.m_cal_simple

# The production parameter directories are organized by validity and are accessed through
# `l200.par`, see [Parameters and Property Files](@ref). A tutorial run like this one keeps the files
# in a directory of its own.

# ## Applying a Stored Calibration

# The stored calibration function is a string. [`ljl_propfunc`](@ref) turns it into a function
# that is applied to the events, in the order the analysis applied the correction and the
# calibration, and gives the energy in keV:

cal_func = ljl_propfunc(pars_ecal_read[det].e_cusp_ctc.cal.func)
e_cal = cal_func.(data_all)
first(e_cal, 3)

# The [A/E Calibration](@ref) tutorial continues from the file `ecal.yaml` written in this tutorial.
