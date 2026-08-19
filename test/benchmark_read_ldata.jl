# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).
#
# read_ldata benchmark over a real jl-v0.6.0+ production: every major call form
# per tier, timed with allocation tracking, printed as a markdown table.
#
# Not part of the test suite (timings are environment-dependent). Run it as:
#     LEGEND_TESTDATA_PROD_CONFIG=/path/to/config.json \
#         julia --project=<env-with-LDM+LegendHDF5IO> test/benchmark_read_ldata.jl
# Period/run/detector auto-detection matches test_ext_legendhdf5io_prod.jl.

using LegendDataManagement
using LegendHDF5IO
using PropertyFunctions
using TypedTables
using Unitful: @u_str

isempty(get(ENV, "LEGEND_TESTDATA_PROD_CONFIG", "")) &&
    error("set LEGEND_TESTDATA_PROD_CONFIG=<path/to/config.json> of a jl-v0.6.0+ production")

ENV["LEGEND_DATA_CONFIG"] = ENV["LEGEND_TESTDATA_PROD_CONFIG"]
l200 = LegendData(:l200)

_catdir(tier, cat) = dirname(dirname(string(l200.tier[DataTier(tier), DataCategory(cat), DataPeriod("p00"), DataRun("r000")])))
_periods(tier, cat) = (d = _catdir(tier, cat); isdir(d) ? sort(filter(x -> occursin(r"^p\d+$", x), readdir(d))) : String[])
_runs(tier, cat, per) = (d = joinpath(_catdir(tier, cat), string(per)); isdir(d) ? sort(filter(x -> occursin(r"^r\d+$", x), readdir(d))) : String[])
_hastier(tier, cat) = !isempty(_periods(tier, cat))

envper = get(ENV, "LEGEND_TESTDATA_PROD_PERIOD", "")
PER = DataPeriod(isempty(envper) ? first(intersect(_periods(:jldsp, :phy), _periods(:jldsp, :cal), _periods(:jlevt, :phy))) : envper)
RUN = DataRun(get(ENV, "LEGEND_TESTDATA_PROD_RUN") do
    first(intersect(_runs(:jldsp, :phy, PER), _runs(:jlevt, :phy, PER)))
end)
CALRUN = DataRun(get(ENV, "LEGEND_TESTDATA_PROD_CALRUN") do
    first(_runs(:jldsp, :cal, PER))
end)

fks_phy = search_disk(FileKey, l200.tier[DataTier(:jlevt), :phy, PER, RUN])
FK_PHY = first(fks_phy)
FK_CAL = first(search_disk(FileKey, l200.tier[DataTier(:jldsp), :cal, PER, CALRUN]))

chinfo = channelinfo(l200, FK_CAL)
ged_dets = [c.detector for c in filter(c -> c.system == :geds && c.processable, chinfo)]
have_jlhit = _hastier(:jlhit, :cal)
hitsel = have_jlhit ? filter(d -> isfile(l200.tier[DataTier(:jlhit), FK_CAL, d]), ged_dets) : ged_dets
DET_GED = first(hitsel)
DET_SPM = first(first(filter(!isempty, read_ldata((:spms_detector,), l200, (:jlevt, FK_PHY)).spms_detector)))
have_raw = isfile(l200.tier[DataTier(:raw), FK_PHY])

@info "benchmark setup" PER RUN CALRUN FK_PHY FK_CAL DET_GED DET_SPM

results = NamedTuple[]

function bench!(name::String, f::Function)
    f()                                # warm up: compilation + file cache
    stats = @timed f()
    rows = try length(stats.value) catch; missing end
    push!(results, (; name, time_s = stats.time, alloc_MB = stats.bytes / 1e6, rows))
    @info "bench" name time_s = round(stats.time; digits = 3) alloc_MB = round(stats.bytes / 1e6; digits = 1)
    stats.value
end

# --- jldsp: per-trigger tier, per-detector groups in per-filekey files -------
bench!("jldsp full table (1 det, 1 fk)",       () -> read_ldata(l200, (:jldsp, FK_CAL, DET_GED)))
bench!("jldsp 1 column (1 det, 1 fk)",         () -> read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, DET_GED)))
bench!("jldsp @pf rename",                     () -> read_ldata(@pf((; energy = $e_cusp)), l200, (:jldsp, FK_CAL, DET_GED)))
bench!("jldsp filterby fast path",             () -> read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, DET_GED); filterby = @pf $e_cusp > 1000))
bench!("jldsp 1 column, n_evts=1000",          () -> read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, DET_GED); n_evts = 1000))
bench!("jldsp 1 column, ALL dets (no-det)",    () -> read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL)))
bench!("jldsp batched 4 dets",                 () -> read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, hitsel[1:min(4, length(hitsel))])))

nfk = min(2, length(fks_phy))
bench!("jldsp 1 column, $nfk filekeys",        () -> read_ldata((:e_cusp,), l200, (:jldsp, fks_phy[1:nfk], DET_GED)))
bench!("jldsp 1 column, $nfk fks parallel",    () -> read_ldata((:e_cusp,), l200, (:jldsp, fks_phy[1:nfk], DET_GED); parallel = true))

# --- jlevt: event tier, flat-prefixed per-detector slicing -------------------
bench!("jlevt nested full (no det)",           () -> read_ldata(l200, (:jlevt, FK_PHY)))
bench!("jlevt 2 flat cols (no det)",           () -> read_ldata((:geds_e_cusp_ctc_cal, :geds_detector), l200, (:jlevt, FK_PHY)))
bench!("jlevt per-det sliced, 2 cols",         () -> read_ldata((:geds_e_cusp_ctc_cal, :geds_trig_e_cusp_ctc_cal), l200, (:jlevt, FK_PHY, DET_GED)))
bench!("jlevt per-det + filterby fast path",   () -> read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED);
                                                                filterby = @pf $geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV"))
bench!("jlevt batched [GED, SPM]",             () -> read_ldata((:geds_e_cusp_ctc_cal, :spms_trig_max_cal), l200, (:jlevt, FK_PHY, [DET_GED, DET_SPM])))

# --- raw: legacy <det>/raw layout, waveform payloads -------------------------
if have_raw
    bench!("raw waveform column (GED)",        () -> read_ldata((:waveform_presummed,), l200, (:raw, FK_PHY, DET_GED)))
    bench!("raw waveform, filtertier=jlevt",   () -> read_ldata((:waveform_presummed,), l200, (:raw, FK_PHY, DET_GED);
                                                                filterby = @pf($geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV"),
                                                                filtertier = :jlevt))
end

# --- per-detector-file tiers -------------------------------------------------
have_jlhit && bench!("jlhit dataQC 2 cols",    () -> read_ldata((:e_cusp, :timestamp), l200, (:jlhit, FK_CAL, DET_GED); subgroup = :dataQC))
_hastier(:jlpls, :phy) && bench!("jlpls tags", () -> read_ldata((:e_10410, :timestamp), l200, (:jlpls, FK_PHY, "PULS01ANA"); subgroup = :tags))
_hastier(:jlpeaks, :cal) && bench!("jlpeaks Tl208FEP 2 cols", () -> read_ldata((:daqenergy, :timestamp), l200, (:jlpeaks, FK_CAL, DET_GED); subgroup = :Tl208FEP))

# --- report ------------------------------------------------------------------
println("\n## read_ldata benchmark — ", basename(dirname(ENV["LEGEND_TESTDATA_PROD_CONFIG"])),
        " (", PER, " ", RUN, "/", CALRUN, ", GED=", DET_GED, ", SPM=", DET_SPM, ")\n")
println("| call | time [s] | alloc [MB] | rows |")
println("|---|---:|---:|---:|")
for r in results
    println("| ", r.name, " | ", round(r.time_s; digits = 3), " | ", round(r.alloc_MB; digits = 1), " | ", r.rows, " |")
end
