# Test-data setup for the documentation examples (not a page).
#
# Include this file from a hidden `@setup` block. It
#   * points `ENV["LEGEND_DATA_CONFIG"]` at a temporary config that combines the
#     metadata of the LegendTestData production `data/legend/metadata` with the
#     tier files of `data/lh5/prod-ref-l200`, and
#   * places the writable directories (`par`, `log`, `plt`) in a `mktempdir()`.
# The LegendTestData artifact is only read, never modified.
#
# Defines: `l200` (the `LegendData` object), `testdata_dir` (artifact path),
# `scratch_dir` (writable temporary directory), `fk` (a FileKey with tier files
# on disk: p18 r000 cal), `det` (detector with tier data for `fk`) and `mfk`
# (a FileKey of the metadata-only toy production: p02 r006 cal).

using LegendDataManagement, LegendTestData, LegendHDF5IO, YAML, Logging

# Info messages of the package (for example from `writevalidity`) and the one-time
# creation of the ParallelProcessingTools cache directory would otherwise appear in
# the rendered output blocks. Warnings and errors stay visible.
LegendDataManagement.ParallelProcessingTools.default_cache_dir()
Logging.disable_logging(Logging.Info)

testdata_dir = legend_test_data_path()
scratch_dir = mktempdir()
let tier = joinpath(testdata_dir, "data", "lh5", "prod-ref-l200", "generated", "tier"),
    meta = joinpath(testdata_dir, "data", "legend", "metadata"),
    par = joinpath(scratch_dir, "par"), log = joinpath(scratch_dir, "log"), plt = joinpath(scratch_dir, "plt"),
    cfg = joinpath(scratch_dir, "config.yaml")

    foreach(mkpath, (par, log, plt))
    YAML.write_file(cfg, Dict("setups" => Dict("l200" => Dict("paths" => Dict(
        "tier" => tier, "metadata" => meta, "par" => par, "log" => log, "plt" => plt,
    )))))
    ENV["LEGEND_DATA_CONFIG"] = cfg
end

l200 = LegendData(:l200)
fk = first(search_disk(FileKey, l200.tier[:jldsp, :cal, DataPeriod(18), DataRun(0)]))
det = DetectorId(:V97061A)
mfk = FileKey("l200-p02-r006-cal-20221226T200846Z")
