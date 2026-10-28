# Use in Juleana.jl

[Juleana.jl](https://github.com/legend-exp/Juleana.jl) (package name `JuLeAna`) is the Julia dataflow of the LEGEND-200 analysis. It processes the data of a production tier by tier with run-level and partition-level processors, and it relies on LegendDataManagement for the configuration, the metadata, the file layout, the parameter databases and the reports. This page shows the calls that a processor makes and links to the pages that describe them. The code blocks are sketches and are not run here; the same functions run on the test data in the other pages of this manual.

Juleana imports the following names from `LegendDataManagement` and `LegendDataManagement.LDMUtils`:

```julia
using LegendDataManagement: LegendData, DataPeriod, DataRun, DataCategory, DataTier, DetectorId, FileKey,
    ProcessStatus, process_succeeded, process_warning, process_failed,
    runinfo, bad_filekeys, is_analysis_run, is_lrun, start_filekey, channelinfo, partitioninfo,
    dataprod_config, ljl_propfunc, lstring, read_ldata, readlprops, writelprops, data_path, get_values,
    get_exposure, get_ged_evt_kwargs, get_aux_cal_propfunc, get_aux_evt_detsel_propfunc,
    lreport, lreport!, writelreport
using LegendDataManagement.LDMUtils: create_pars, create_validity, create_logtbl, create_metadatatbl,
    get_partitionvalidity, get_plottitle, get_rreportfilename, get_preportfilename, get_totalTimer,
    savelfig, writevalidity
```

## Setup and Run Selection

The data configuration is read from the file named by the environment variable `LEGEND_DATA_CONFIG`, which therefore has to be set before the [`LegendData`](@ref) object is created. The `dataset` keyword selects a list of runs from the metadata, `:default` or a key of `l200.metadata.datasets.runlists`. See [Configuration and LegendData](@ref).

```julia
ENV["LEGEND_DATA_CONFIG"] = "<path-to-config>"
l200 = LegendData(:l200; dataset = :default)

haskey(l200.metadata.datasets.runlists, dataset)
```

Periods and runs come from the run information of the dataset, which is read from the metadata and does not check the files on disk (see [Channel, Run and Partition Information](@ref)). A processor is run for a run only if the run is an analysis run of the category that the processor handles.

```julia
periods = sort!(unique(runinfo(l200).period))

rinfo = runinfo(l200)
runs = sort!(rinfo.run[rinfo.period .== period])

is_analysis_run(l200, (period, run, :cal))
is_lrun(l200, (period, run, :cal))
```

Periods, runs and categories given as symbols, strings or integers are converted with `DataPeriod`, `DataRun` and `DataCategory` (see [Identifiers and FileKeys](@ref)).

## Run-Level Processors

A run-level processor is called with the processing configuration, the `LegendData` object, a [`DataPeriod`](@ref) and a [`DataRun`](@ref). It selects the file keys and channels of the run, reads its configuration and the results of upstream processors, processes each detector, and writes parameters, validity and a report.

```julia
# file keys of the run that are listed in the metadata and not ignored
filekeys = filter(!in(bad_filekeys(l200; load_key = :unprocessable)),
                  only(runinfo(l200, (period, run, :cal)).keys))
filekey = start_filekey(l200, (period, run, :cal))

chinfo = channelinfo(l200, filekey; system = :geds, only_processable = true)

energy_config = dataprod_config(l200).energy(filekey)       # sections: raw, dsp, qc, psd, energy, sipm, pmt, evt
```

[`bad_filekeys`](@ref), [`start_filekey`](@ref), [`channelinfo`](@ref) and [`dataprod_config`](@ref) are described in [Metadata and Validity](@ref) and [Channel, Run and Partition Information](@ref). The per-detector function uses the channel and detector of a row of `chinfo` and reads the data of the detector:

```julia
function process_detector(chinfo_det)
    ch  = chinfo_det.channel
    det = chinfo_det.detector

    qc_mask = read_ldata(@pf($is_single_pulse && !$is_pulser), l200, :jlqcs, filekey, det)
    data    = read_ldata((:e_cusp, :timestamp), l200, :jldsp, :cal, period, run, det)[qc_mask]
    # fits and other processing ...
end
```

The stored property functions of the parameters are applied while reading with [`ljl_propfunc`](@ref); see [Property Functions and Expressions](@ref) and [Reading Data](@ref).

## Partition-Level Processors

A partition-level processor is called with the configuration, the `LegendData` object and a [`DataPeriod`](@ref). It handles every detector and partition that contains the period, so the channel table is expanded to one row per detector and partition.

```julia
rinfo   = runinfo(l200, period) |> filterby(@pf $cal.is_analysis_run)
filekey = first(rinfo).cal.startkey

chinfo_unfolded = channelinfo(l200, period, :cal; system = :geds, only_processable = true,
                              explode_partitions = true)
```

For each row, the runs of the partition of the detector are found with [`partitioninfo`](@ref), and the data of all these runs are read and combined:

```julia
det, part = chinfo_det.detector, chinfo_det.partition

partinfo_det = partitioninfo(l200, det, part)               # rows with period, run and cal.startkey
filekey_det  = start_filekey(l200, (first(partinfo_det.period), first(partinfo_det.run), :cal))
validity_det = get_partitionvalidity(l200, det, part)

energy_config = dataprod_config(l200).energy(filekey_det)

data = fast_flatten([read_ldata((:e_cusp, :timestamp), l200, :jldsp, :cal, p.period, p.run, det)
                     for p in partinfo_det])                 # fast_flatten is from LegendDataTypes
```

The tables and their columns are described in [Channel, Run and Partition Information](@ref). The exposure of a set of detectors in a period is calculated with [`get_exposure`](@ref), see [Exposure and Livetime](@ref).

## Reading and Writing LH5 Data

Input tiers are read with [`read_ldata`](@ref), which is provided by the LegendHDF5IO extension (see [Extensions](@ref) and [Reading Data](@ref)). The path of an output file is given by the tier layout, `l200.tier[:jldsp, filekey]`, and `l200.tier[:jlevt, filekey]` for the event tier (see [Data Tiers and Files](@ref)). The output tables themselves are written directly with LegendHDF5IO:

```julia
using LegendHDF5IO
using ParallelProcessingTools: write_files, CreateOrModify

dspfilename = l200.tier[:jldsp, filekey]
write_files(dspfilename; mode = CreateOrModify()) do outfilename
    lh5open(outfilename, "cw") do outdata
        outdata[:jldsp, det] = dsp_table
    end
end
```

`write_files` of `ParallelProcessingTools` hands the function a temporary file name and moves the file to the final name afterwards. [`atomic_fcreate`](@ref) of LegendDataManagement does the same for any function that writes files (see [Reports and Utilities](@ref)).

## Parameters and Validity

Processors keep their results as `PropDict` objects in the parameter databases `l200.par.rpars.<category>` (run level, one file per run) and `l200.par.ppars.<category>` (partition level, one file per detector and partition). Upstream results are read with the file name (`[period, run]`, `[det, part]`) or through the validity selection with a file key, and `get_values` removes units and uncertainties. See [Parameters and Property Files](@ref).

```julia
pars_ctc = get_values(l200.par.rpars.ctc[period, run])                 # file of a run
pars_tau = get_values(l200.par[pars_type, :pz](filekey))               # validity lookup, pars_type is :rpars or :ppars

mkpath(joinpath(data_path(l200.par.rpars.ecal), string(period)))
pars_db = PropDict(l200.par.rpars.ecal[period, run])                   # existing results of the run
```

The results of the per-detector function are collected with [`create_pars`](@ref LegendDataManagement.LDMUtils.create_pars), written with `writelprops`, and made valid with [`writevalidity`](@ref LegendDataManagement.LDMUtils.writevalidity); see [Reports and Utilities](@ref) for the expected fields of the results.

```julia
# run level
pars_db = create_pars(pars_db, result)
writelprops(l200.par.rpars.ecal[period], run, pars_db)
writevalidity(l200.par.rpars.ecal, filekey, (period, run))

# partition level, once per detector and partition
pars_db_det = create_pars(pars_db_det, Dict(chinfo_det => result_det))
writelprops(l200.par.ppars.ecal[det], part, pars_db_det)
writevalidity(l200.par.ppars.ecal[det], filekey_det, part)

# partition level, after all detectors of the period
writevalidity(l200.par.ppars.ecal, create_validity(result))
```

The keyword `category` of [`writevalidity`](@ref LegendDataManagement.LDMUtils.writevalidity) restricts an entry to a data category, for example `writevalidity(db, filekey, "<period>/<file>.yaml"; category = :phy)`. Juleana calls `writelprops`, `writevalidity` and `writelreport` through the wrappers `write_pars`, `write_validity` and `write_report`, which skip the write when a step is run with temporary outputs.

## Reports and Plots

Each processor builds one Markdown report with the same structure and writes it to the file given by [`get_rreportfilename`](@ref LegendDataManagement.LDMUtils.get_rreportfilename) (run level) or [`get_preportfilename`](@ref LegendDataManagement.LDMUtils.get_preportfilename) (partition level):

```julia
report = lreport()
lreport!(report, "# Main Log")
lreport!(report, StructArray(var"Processor Status" = [master_status(result)]))   # master_status: a ProcessStatus
lreport!(report, "# Metadata")
lreport!(report, create_metadatatbl(filekey))
lreport!(report, "# Results")
lreport!(report, create_logtbl(result))

writelreport(get_rreportfilename(l200, filekey, :energy_calibration), report)
```

The rows of the log table hold a [`ProcessStatus`](@ref) that is created from `ProcessStatus(0)` for a failed and `ProcessStatus(1)` for a successful item, and processors compare statuses with `process_succeeded`, `process_warning` and `process_failed`. Plot titles and plot files use [`get_plottitle`](@ref LegendDataManagement.LDMUtils.get_plottitle) and [`savelfig`](@ref LegendDataManagement.LDMUtils.savelfig), with the function that writes the figure as first argument:

```julia
title = get_plottitle(filekey, det, "Simple Calibration"; additional_type = "e_cusp")
savelfig(LegendMakie.lsavefig, fig, l200, filekey, det, :simple_calibration_e_cusp)
```

At the partition level, [`get_plottitle`](@ref LegendDataManagement.LDMUtils.get_plottitle) takes the partition after the file key, `get_plottitle(filekey_det, part, det, "Energy Spectrum")`, and [`savelfig`](@ref LegendDataManagement.LDMUtils.savelfig) takes it before the file key, `savelfig(save_func, fig, l200, part, filekey_det, det, :energy_spectrum)`.
