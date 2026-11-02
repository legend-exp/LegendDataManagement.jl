# Property Functions and Expressions

Calibration functions, quality cuts and event selections of a LEGEND production are stored as short text expressions in the metadata and in the parameter files. [`ljl_propfunc`](@ref) turns such an expression into a `PropertyFunctions.PropertyFunction` that is applied to a table row, to a whole table, or passed to [`read_ldata`](@ref). The `get_*_propfunc` functions assemble the property functions that a production configuration defines.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

## LEGEND Julia Expressions

A LEGEND Julia expression is a restricted Julia expression. Its variable names are the properties (columns) of the input of the function. [`ljl_propfunc`](@ref) compiles it. `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`) and `det` a detector ID with data in these files.

```@example ldm
using TypedTables, PropertyFunctions, Unitful

dsp = read_ldata(l200, :jldsp, fk, det)

is_high = ljl_propfunc("e_cusp > 10000.0 && !isnan(blmean)")
is_high((e_cusp = 15000.0, blmean = 1.0))
```

The function accepts a row, as above, and any table with the used columns when broadcast:

```@example ldm
is_high.(dsp)
```

It is also a valid selector for [`read_ldata`](@ref), in which case only the columns it uses are read (see [Reading Data](@ref)).

```@example ldm
count(read_ldata(is_high, l200, :jldsp, fk, det))
```

The unit names `s`, `ms`, `μs` (also `us`), `ns`, `MeV`, `keV`, `eV` and `e` denote the corresponding Unitful units, and `u"..."` string macros are allowed. Calibration functions that are stored in parameter files are expressions of this kind, with the fit results written into the string:

```@example ldm
calibrate = ljl_propfunc("e_cusp * 0.25keV + 1.5keV")
first(calibrate.(dsp), 2)
```

### Allowed Syntax

An expression can contain numbers, variable names, property access (`a.b`), indexing (`a[1]`), symbols (`:name`), comparisons, `&&`, `||`, the conditional `a ? b : c`, broadcasts of allowed functions (`f.(x)` and dotted operators like `.+`), and calls of the allowed functions. The names `NaN`, `Inf`, `missing` and `nothing` are values. The allowed functions are:

* arithmetic and logic: `+`, `-`, `*`, `/`, `^`, `div`, `rem`, `mod`, `!`, `|`, `&`, `xor`, `==`, `!=`, `<`, `>`, `<=`, `>=`, `isapprox`, `≈`, `in`, `∈`, `..`, the range operator `:`
* mathematical functions: `abs`, `abs2`, `sqrt`, `exp`, `exp2`, `exp10`, `log`, `log2`, `log10`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `min`, `max`, `norm`, `normalize`, `one`, `zero`, `identity`
* tests: `isnan`, `isinf`, `isfinite`, `isempty`, `haskey`
* reductions and collections: `all`, `any`, `sum`, `prod`, `minimum`, `maximum`, `mean`, `first`, `last`, `length`, `size`, `get`, `getindex`, `getproperty`, `broadcast`
* conversions: `Symbol`, `String`, `string`, `Int`, `Float64`, `Bool`, `parse`, `DetectorId`, `ChannelId`
* measurements: `value`, `uncertainty`, `stdscore`, `weightedmean`, `±`

The set is stored in `LegendDataManagement.ljl_expr_allowed_funcs`. Any other function, macro or syntax, including tuples, blocks and assignments, throws an `ArgumentError` when the expression is parsed, so that an expression read from a configuration file cannot run arbitrary code.

```@example ldm
try
    ljl_propfunc("run(e_cusp)")
catch err
    println(sprint(showerror, err))
end
```

### Several Outputs

A dictionary from output names to expressions, or a `PropDict` of strings as read from a configuration file, compiles to one property function that returns a `NamedTuple` with these names, sorted by name.

```@example ldm
select = ljl_propfunc(Dict(:energy => "e_cusp * 0.5", :is_high => "e_cusp > 10000.0"))
first(select.(dsp), 2)
```

### Parsing Without Compiling

[`parse_ljlexpr`](@ref) parses a string and returns the Julia syntax tree after the checks above, with the unit names replaced by units. [`process_ljlexpr`](@ref) checks a syntax tree that is already parsed (a [`LJlExprLike`](@ref)) and replaces its variable names by `f_varsubst(name)`. Parsed expressions and compiled functions are cached.

```@example ldm
parse_ljlexpr("e_cusp * 0.5keV < 10.0keV && !isnan(baseline)")
```

```@example ldm
process_ljlexpr(:(a + b), name -> name == :a ? :x : name)
```

## LEGEND Python Expressions

The metadata of the Python software uses expressions with `~` for negation and `|`, `&` for the logical operators. [`parse_lpyexpr`](@ref) converts them to LEGEND Julia expressions and [`lpy_propfunc`](@ref) compiles them. Supported are numbers, variable names, the operators `~`, `!=`, `+`, `-`, `*`, `/`, `<`, `>`, `>=`, `<=`, the function `abs`, and `|`, `&`, which become `||` and `&&`. Other syntax throws an `ArgumentError`.

```@example ldm
parse_lpyexpr("~(is_saturated) & (e_cusp > 100.0)")
```

```@example ldm
lpy_propfunc("(e_cusp > 10000.0) & ~(blmean < 0)").(dsp)
```

## Property Functions of a Production

A production defines its functions in the configuration `dataprod_config(data)` (see [`dataprod_config`](@ref)) and in the parameter categories of `data.par` (see [Parameters and Property Files](@ref)). The functions below read these for a [`LegendData`](@ref) object, a validity selection (a [`FileKey`](@ref) or a [`LegendDataManagement.ValiditySelection`](@ref)) and, where it applies, a [`DetectorId`](@ref), and return compiled property functions. All have the signature `f(data, sel[, detector]; kwargs...)`. They require the `dataprod` or `jldataprod` configuration of a production, which the test data of this documentation do not contain; the examples below are therefore not run.

```julia
using LegendDataManagement

l200 = LegendData(:l200)
filekey = FileKey("l200-p03-r000-cal-20230311T235840Z")
det = DetectorId(:V00000A)

cal_pf = get_ged_cal_propfunc(l200, filekey, det)
qc_pf = get_ged_qc_cuts_propfunc(l200, filekey, det)
```

Properties of the result are named after the entries of the configuration. Their values are applied to a table of the matching tier by broadcasting, e.g. `cal_pf.(dsp)` returns a table with one calibrated energy column `<energy type>_cal` per energy type.

**Germanium detectors.**

* [`get_ged_cal_propfunc`](@ref): energy calibration, with the functions from the parameter category `pars_cat` (`:ecal`) of `pars_type` (`:rpars` or `:ppars`). Energy types without a stored function evaluate to `NaN`.
* [`get_ged_qc_cuts_propfunc`](@ref), [`get_ged_qc_is_trig_propfunc`](@ref), [`get_ged_qc_is_single_pulse_propfunc`](@ref), [`get_ged_qc_is_empty_trace_propfunc`](@ref), [`get_ged_qc_is_crosstalk_propfunc`](@ref): quality cut definitions, taken from the `qc` configuration. The configuration is merged from its `default` section, the section of the usability of the detector and the section of the detector.
* [`get_ged_psd_propfunc`](@ref), [`get_ged_psd_classifier_propfunc`](@ref): pulse-shape classifiers from the `psd` configuration and the parameter categories `aoe` and `lq`.
* [`get_ged_aoe_cut_propfunc`](@ref), [`get_ged_lq_cut_propfunc`](@ref): cut conditions for the classifiers, built from the cut limits that [`dataprod_pars_aoe_window`](@ref) and [`LegendDataManagement.dataprod_pars_lq_window`](@ref) return as a `ClosedInterval`.

**Photomultipliers and silicon photomultipliers.**

* [`get_spm_cal_propfunc`](@ref), [`get_spm_dc_sel_propfunc`](@ref), [`get_spm_dc_cal_propfunc`](@ref): calibration of the liquid-argon SiPM channels and their delayed-coincidence tagging, from the `sipm` configuration and the parameter category `sipmcal`.
* [`get_pmt_cal_propfunc`](@ref), [`get_pmt_is_physical_trig_propfunc`](@ref): calibration and trigger condition of the PMT channels, from the muon configuration and the parameter category `pmtcal`.

**Event building.** The sections `geds`, `spms`, `pmts` and `aux` of the event configuration (`evt`) define how events are assembled. Each entry has a getter that compiles the stored expression, for example [`get_ged_evt_detsel_propfunc`](@ref) (which detectors take part in an event), [`get_ged_evt_detdata_propfunc`](@ref) (the per-detector output data), [`get_ged_evt_kwargs`](@ref) (keyword arguments of the event builder as a `NamedTuple`) and the corresponding `get_spms_evt_*`, `get_pmts_evt_*` and `get_aux_evt_*` functions. The calibration of an auxiliary channel, such as the pulser, is [`get_aux_cal_propfunc`](@ref):

```julia
using PropertyFunctions

det_aux = DetectorId(:<auxiliary-detector>)
aux_cal = get_aux_cal_propfunc(l200, filekey, det_aux)
chinfo_aux = filterby(get_aux_evt_detsel_propfunc(l200, filekey))(channelinfo(l200, filekey))
```

The functions cache their configuration per `data` object. A new `LegendData` object is needed to see files that changed on disk.
