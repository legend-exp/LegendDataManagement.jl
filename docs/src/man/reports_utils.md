# Reports and Utilities

LegendDataManagement provides a small report builder ([`lreport`](@ref)) and the status type [`ProcessStatus`](@ref) for processing reports, helpers for atomic file creation and for processing many files, and the submodule `LegendDataManagement.LDMUtils` with the helpers that the Juleana dataflow uses to name report and plot files and to turn processing results into parameters and validity entries.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

## Reports

A [`LegendReport`](@ref LegendDataManagement.LegendReport) is a list of contents that can be shown as plain text, Markdown or HTML. It is created with [`lreport`](@ref) and extended with [`lreport!`](@ref), which returns the report.

```@example ldm
using StructArrays, Unitful

tbl = StructArray(
    detector = ["B99000A", "V99000A"],
    status = [process_succeeded, process_failed],
    fwhm = [2.5, 3.0],
)

rpt = lreport("# Processing Report", "The table lists one row per detector:", tbl)
lreport!(rpt, "Total livetime:", 12.34567u"s")
nothing # hide
```

The following content types are supported:

* Strings are parsed as Markdown. Consecutive Markdown contents are merged into one Markdown document, and a `Markdown.MD` object is accepted as well.
* Floating-point numbers and real `Unitful` quantities are rounded to three digits and added as text.
* Tables, i.e. any object for which `Tables.istable` is `true` (such as a `StructArray` or a `TypedTables.Table`), are shown as Markdown tables. The cells of a table can be numbers, strings, symbols, expressions, intervals, arrays and [`ProcessStatus`](@ref) values.
* Any other object, for example a plot, is added as it is. It is shown in the requested format if the object supports it; otherwise HTML (for Markdown and HTML output) or plain text is used.

### Output Formats

`show` with the MIME types `text/plain`, `text/markdown` and `text/html` renders a report. A [`ProcessStatus`](@ref) is printed as `Success`, `Failure` or `Warning`, with colors in Markdown and HTML.

```@example ldm
show(stdout, MIME"text/markdown"(), rpt)
```

```@example ldm
show(stdout, MIME"text/plain"(), rpt)
```

[`writelreport`](@ref) writes a report to a file. The format is taken from the file extension (for example `.md`, `.html` or `.txt`) or from a MIME type given as second argument. The directory of the file has to exist. `scratch_dir` is a temporary directory for the files that the examples write.

```@example ldm
report_dir = mkpath(joinpath(scratch_dir, "reports"))
writelreport(joinpath(report_dir, "report.md"), rpt)
writelreport(joinpath(report_dir, "report.html"), MIME"text/html"(), rpt)
readdir(report_dir)
```

```@example ldm
print(read(joinpath(report_dir, "report.md"), String))
```

### Custom Content Types

`show` converts the contents of a report with `LegendDataManagement.lreport_for_show!(rpt, mime, content)`. A package that wants its own type in reports specializes the method for this type, usually for `MIME"text/markdown"`; see [`LegendDataManagement.lreport_for_show!`](@ref).

## Process Status

[`ProcessStatus`](@ref) is an `Enum` with the values `process_succeeded` (`1`), `process_failed` (`0`) and `process_warning` (`-1`). Processing code stores a status in each row of its log table, and the report renders it in color. A status can be created from its integer value and compared with the values.

```@example ldm
ProcessStatus(1) == process_succeeded, ProcessStatus(0) == process_failed, ProcessStatus(-1)
```

## Report and Plot Files

The following functions of `LegendDataManagement.LDMUtils` build the paths of the report and plot files of a processing step from a [`LegendData`](@ref) object and a file key. Report files are stored in the tier folder `jlrep` and plot files in `jlplt` (see [Data Tiers and Files](@ref)). The functions create the folders if they do not exist. The submodule is loaded with

```julia
using LegendDataManagement
using LegendDataManagement.LDMUtils
```

The examples use a second `LegendData` object whose output tiers point into a temporary directory, so that no file is written to the test data. The configuration file selects the directories (see [Configuration and LegendData](@ref)); `LegendData` reads it when it is created.

```@example ldm
using YAML

out_dir = joinpath(scratch_dir, "out")
out_config = joinpath(scratch_dir, "out_config.yaml")
YAML.write_file(out_config, Dict("setups" => Dict("l200" => Dict("paths" => Dict(
    "tier" => joinpath(out_dir, "tier"),
    "metadata" => joinpath(testdata_dir, "data", "legend", "metadata"),
    "par" => joinpath(out_dir, "par"),
    "log" => joinpath(out_dir, "log"),
    "plt" => joinpath(out_dir, "plt"),
)))))
out = withenv(() -> LegendData(:l200), "LEGEND_DATA_CONFIG" => out_config)

using LegendDataManagement.LDMUtils
relpath(get_rreportfilename(out, mfk, :energy), out_dir)
```

[`get_rreportfilename`](@ref LegendDataManagement.LDMUtils.get_rreportfilename) is used by run-level processors and [`get_preportfilename`](@ref LegendDataManagement.LDMUtils.get_preportfilename) by partition-level processors. Both take a file key and a process name as a `Symbol`, or the individual selectors; the folders are returned by [`get_rreportfolder`](@ref LegendDataManagement.LDMUtils.get_rreportfolder) and [`get_preportfolder`](@ref LegendDataManagement.LDMUtils.get_preportfolder).

```@example ldm
relpath(get_preportfilename(out, mfk, :energy), out_dir)
```

[`create_metadatatbl`](@ref LegendDataManagement.LDMUtils.create_metadatatbl) returns a one-row table with the setup, period, run and category of a file key, or, when a [`DataPartition`](@ref) is given as second argument, the setup, partition and category, for the metadata section of a report.

```@example ldm
create_metadatatbl(mfk)
```

[`get_plottitle`](@ref LegendDataManagement.LDMUtils.get_plottitle) returns a plot title for a detector, with the process name and, optionally, an additional type in front of the run or partition selection.

```@example ldm
get_plottitle(mfk, :V99000A, "Spectrum"; additional_type = "e_cusp")
```

```@example ldm
get_plottitle(mfk, DataPartition(1), :V99000A, "Spectrum")
```

[`savelfig`](@ref LegendDataManagement.LDMUtils.savelfig) saves a plot to the plot file of a selection. The first argument is the function that writes the file; it is called as `save_func(p, filename; kwargs...)`, so `Plots.savefig` can be used directly. The folder is `jlplt/rplt/<category>/<period>/<run>/<process>` for a file key and `jlplt/pplt/<detector>/<partition>/<category>/<process>` when a [`DataPartition`](@ref) is given before the file key.

```@example ldm
using Plots

p = plot(rand(10); title = get_plottitle(mfk, :V99000A, "Example"))
savelfig(savefig, p, out, mfk, :V99000A, :example)
savelfig(savefig, p, out, DataPartition(1), mfk, :V99000A, :example)
[relpath(f, out_dir) for f in (
    LegendDataManagement.LDMUtils.get_pltfilename(out, mfk, :V99000A, :example),
    LegendDataManagement.LDMUtils.get_pltfilename(out, DataPartition(1), mfk.setup, mfk.category, :V99000A, :example),
)]
```

The file names are built by [`LegendDataManagement.LDMUtils.get_pltfilename`](@ref) and the folders by [`LegendDataManagement.LDMUtils.get_pltfolder`](@ref); neither is exported.

## Parameters and Validity from Processing Results

The helpers [`create_pars`](@ref LegendDataManagement.LDMUtils.create_pars), [`create_validity`](@ref LegendDataManagement.LDMUtils.create_validity), [`create_logtbl`](@ref LegendDataManagement.LDMUtils.create_logtbl) and [`get_totalTimer`](@ref LegendDataManagement.LDMUtils.get_totalTimer) collect the results of a processing step that runs one function per item (a detector, or a detector and partition). The `result` argument is a collection of pairs `item => res`, where the `NamedTuple` `res` has the following fields:

* `result`: parameters as a `NamedTuple`, `Dict` or `PropDict`, which [`create_pars`](@ref LegendDataManagement.LDMUtils.create_pars) merges into the parameters of `item.detector`.
* `processed`: `Bool`, or a `Dict` (which counts as processed); only processed items contribute their `result`.
* `log`: a row of the log table, or a `Dict` of rows, for [`create_logtbl`](@ref LegendDataManagement.LDMUtils.create_logtbl).
* `validity`: a vector of `(period, run, filekey, validity)` named tuples, for [`create_validity`](@ref LegendDataManagement.LDMUtils.create_validity); see [`get_partitionvalidity`](@ref LegendDataManagement.LDMUtils.get_partitionvalidity).
* `skipped`: `Bool`, for [`create_validity`](@ref LegendDataManagement.LDMUtils.create_validity).
* `timer`: a `TimerOutputs.TimerOutput`, for [`get_totalTimer`](@ref LegendDataManagement.LDMUtils.get_totalTimer).

Fields that a function does not use can be left out. The following example creates such a result for the two processable detectors of a run, collects the parameters, and writes them.

```@example ldm
using PropDicts, TypedTables

chinfo = channelinfo(l200, mfk; system = :geds, only_processable = true)
filekey = start_filekey(l200, DataPeriod(2), DataRun(6), :cal)

result = [
    (; detector = row.detector) => (
        processed = true,
        result = (; ecal = (; cal = 0.25 * i)),
    )
    for (i, row) in enumerate(chinfo)
]

pars = create_pars(PropDict(), result)
```

[`create_pars`](@ref LegendDataManagement.LDMUtils.create_pars) modifies and returns the `PropDict` that is passed as first argument, so that existing parameters of other detectors are kept. The parameters are written with `writelprops` (see [Parameters and Property Files](@ref)), and [`writevalidity`](@ref LegendDataManagement.LDMUtils.writevalidity) adds the entry to `validity.yaml` that makes them valid from the start of the run:

```@example ldm
mkpath(joinpath(data_path(l200.par.rpars.ecal), "p02"))
writelprops(l200.par.rpars.ecal[DataPeriod(2)], DataRun(6), pars)
writevalidity(l200.par.rpars.ecal, filekey, (DataPeriod(2), DataRun(6)))
print(read(joinpath(data_path(l200.par.rpars.ecal), "validity.yaml"), String))
```

```@example ldm
l200.par.rpars.ecal(filekey)
```

[`writevalidity`](@ref LegendDataManagement.LDMUtils.writevalidity) accepts a file key together with a file name (or a vector of file names), a `(DataPeriod, DataRun)` or a [`DataPartition`](@ref). The keywords `category` (default `:all`) and `mode` (`"reset"`, the default, `"append"`, `"remove"` or `"replace"`) are written into the entry. An existing entry with the same time and mode is replaced, and the entries stay sorted by time.

For partition-level parameters, [`get_partitionvalidity`](@ref LegendDataManagement.LDMUtils.get_partitionvalidity) returns the validity entries of a detector and partition, with one entry per run of the partition. [`create_validity`](@ref LegendDataManagement.LDMUtils.create_validity) collects these entries from a result into a `NamedTuple` `(result, skipped)`, which `writevalidity(props_db, validity)` writes as a sequence of entries for all detectors and partitions. The keyword `impl` selects `:full` (default) or `:diff`.

```@example ldm
validity = (result = StructArray(get_partitionvalidity(l200, :V99000A, DataPartition(1))), skipped = false)
mkpath(data_path(l200.par.ppars.ecal))
writevalidity(l200.par.ppars.ecal, validity)
print(read(joinpath(data_path(l200.par.ppars.ecal), "validity.yaml"), String))
```

[`create_logtbl`](@ref LegendDataManagement.LDMUtils.create_logtbl) turns the `log` rows of all items into one table for a report: each row holds the status and the log columns of one item; a column that is missing in a row is filled with `"-"`. [`get_totalTimer`](@ref LegendDataManagement.LDMUtils.get_totalTimer) merges the timers of all items into one `TimerOutput`. Typical use in a processor is

```julia
report = lreport()
lreport!(report, "# Main Log")
lreport!(report, StructArray(var"Processor Status" = [process_succeeded]))
lreport!(report, "# Metadata")
lreport!(report, create_metadatatbl(filekey))
lreport!(report, "# Results")
lreport!(report, create_logtbl(result))
writelreport(get_rreportfilename(l200, filekey, :energy_calibration), report)
```

See [Use in Juleana.jl](@ref) for the complete flow of a processor.

## Detector and Channel Helpers

`LegendDataManagement.LDMUtils` also exports short helpers on top of [`channelinfo`](@ref) and the detector metadata. [`detector2channel`](@ref LegendDataManagement.LDMUtils.detector2channel) and [`channel2detector`](@ref LegendDataManagement.LDMUtils.channel2detector) convert between [`DetectorId`](@ref) and [`ChannelId`](@ref) for a selection (a file key or `(period, run, category)`), [`detector_type`](@ref LegendDataManagement.LDMUtils.detector_type) returns the type of a germanium detector from the diode metadata, and [`data_starttime`](@ref LegendDataManagement.LDMUtils.data_starttime) returns the start time of a run.

```@example ldm
detector2channel(l200, mfk, :V99000A), channel2detector(l200, mfk, ChannelId(12))
```

```@example ldm
detector_type(l200, :V99000A), data_starttime(l200, mfk)
```

[`get_partitionfilekeys`](@ref LegendDataManagement.LDMUtils.get_partitionfilekeys) returns the file keys of a partition that are found on disk and [`get_mltrainfilename`](@ref LegendDataManagement.LDMUtils.get_mltrainfilename) the name of the machine-learning training file. The functions `load_run_ch`, `load_partition_ch`, `load_raw_evt`, `get_peaksfilename`, `get_hitchfilename` and `get_partition_channelinfo` are deprecated; their deprecation messages name the replacements, such as [`read_ldata`](@ref).

## Atomic File Creation

[`atomic_fcreate`](@ref) creates files such that a file is either complete or absent. The function `body` receives temporary file names in the directories of the final files, and the temporary files are renamed to the final names after `body` returns. If `body` throws, the temporary files are deleted (`delete_on_error = true`) and the final files are not touched. Missing directories are created (`create_dirs = true`). With `overwrite = false`, the call does nothing if all files exist.

```@example ldm
target = joinpath(scratch_dir, "atomic", "result.txt")

atomic_fcreate(target) do tmp
    write(tmp, "complete")
end

err = try
    atomic_fcreate(target) do tmp
        write(tmp, "partial")
        error("writing failed")
    end
catch e
    e
end

err, read(target, String), readdir(dirname(target))
```

## Processing Many Files

[`map_datafiles`](@ref) applies a function to the files of a list of file keys. It opens `data.tier[tier, filekey]` with `f_open`, calls `f_process` on the opened file and closes it. Errors are logged and collected instead of thrown, so that one unreadable file does not stop the loop. The result is a `NamedTuple` with `result` (a `Dict` from file key to the value of `f_process`), `success` (`true` if no file failed) and `failed` (a `Dict` from file key to the error).

In this example, `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`).

```@example ldm
missing_fk = FileKey("l200-p18-r000-cal-20260101T000000Z")
res = map_datafiles(f -> collect(keys(f)), lh5open, l200, :jldsp, [fk, missing_fk])
res.success, res.result, collect(keys(res.failed))
```
