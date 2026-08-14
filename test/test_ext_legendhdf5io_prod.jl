# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).
#
# read_ldata integration matrix against a real jl-v0.6.0+ production.
# Included by test_ext_legendhdf5io.jl when LEGEND_TESTDATA_PROD_CONFIG is set;
# see the banner there for how to activate. Everything below auto-detects
# period, runs and detectors from the production, and skips (with @info)
# testsets whose tier is not present in it.

using LegendDataManagement
using Test

using LegendHDF5IO
using PropertyFunctions
using TypedTables
using Unitful: @u_str

@testset "read_ldata production integration (jl-v0.6.0+ schema)" begin
    # LegendData construction goes through the LEGEND_DATA_CONFIG env var (the only
    # public constructor route)
    ENV["LEGEND_DATA_CONFIG"] = ENV["LEGEND_TESTDATA_PROD_CONFIG"]
    l200 = LegendData(:l200)

    # ---- auto-detection of period / runs / detectors -----------------------
    # category dir via path construction with dummy period/run (pure string op)
    _catdir(tier, cat) = dirname(dirname(string(l200.tier[DataTier(tier), DataCategory(cat), DataPeriod("p00"), DataRun("r000")])))
    _periods(tier, cat) = (d = _catdir(tier, cat); isdir(d) ? sort(filter(x -> occursin(r"^p\d+$", x), readdir(d))) : String[])
    _runs(tier, cat, per) = (d = joinpath(_catdir(tier, cat), string(per)); isdir(d) ? sort(filter(x -> occursin(r"^r\d+$", x), readdir(d))) : String[])
    _hastier(tier, cat) = !isempty(_periods(tier, cat))

    envper = get(ENV, "LEGEND_TESTDATA_PROD_PERIOD", "")
    per_candidates = isempty(envper) ? intersect(_periods(:jldsp, :phy), _periods(:jldsp, :cal), _periods(:jlevt, :phy)) : [envper]
    isempty(per_candidates) && error("production has no period with jldsp phy+cal and jlevt phy data")
    PER = DataPeriod(first(per_candidates))

    envrun = get(ENV, "LEGEND_TESTDATA_PROD_RUN", "")
    run_candidates = isempty(envrun) ? intersect(_runs(:jldsp, :phy, PER), _runs(:jlevt, :phy, PER)) : [envrun]
    isempty(run_candidates) && error("period $PER has no run with both jldsp and jlevt phy data")
    RUN = DataRun(first(run_candidates))

    envcalrun = get(ENV, "LEGEND_TESTDATA_PROD_CALRUN", "")
    calrun_candidates = isempty(envcalrun) ? _runs(:jldsp, :cal, PER) : [envcalrun]
    isempty(calrun_candidates) && error("period $PER has no cal jldsp runs")
    CALRUN = DataRun(first(calrun_candidates))
    @info "read_ldata production tests using" PER RUN CALRUN

    fks_phy = search_disk(FileKey, l200.tier[DataTier(:jlevt), :phy, PER, RUN])
    @test !isempty(fks_phy)
    FK_PHY = first(fks_phy)
    @test isfile(l200.tier[DataTier(:jldsp), FK_PHY])

    fks_cal = search_disk(FileKey, l200.tier[DataTier(:jldsp), :cal, PER, CALRUN])
    @test !isempty(fks_cal)
    FK_CAL = first(fks_cal)

    have_raw     = isfile(l200.tier[DataTier(:raw), FK_PHY])
    have_jlhit   = _hastier(:jlhit, :cal)
    have_jlpls   = _hastier(:jlpls, :phy)
    have_jlpeaks = _hastier(:jlpeaks, :cal)
    have_jlpmt   = _hastier(:jlpmt, :phy)

    chinfo = channelinfo(l200, FK_CAL)
    geds = filter(c -> c.system == :geds && c.processable, chinfo)
    @test !isempty(geds)
    ged_dets = [c.detector for c in geds]
    hitsel = have_jlhit ? filter(d -> isfile(l200.tier[DataTier(:jlhit), FK_CAL, d]), ged_dets) : ged_dets
    @test !isempty(hitsel)
    DET_GED = first(hitsel)
    DET_GED2 = length(hitsel) > 1 ? hitsel[2] : first(ged_dets)

    # SPM / PMT detectors straight from the event data (guaranteed present there)
    spm_detcol = read_ldata((:spms_detector,), l200, (:jlevt, FK_PHY)).spms_detector
    DET_SPM = first(first(filter(!isempty, spm_detcol)))
    @test DET_SPM isa DetectorId

    @testset "column selection — equivalent selector forms" begin
        full = read_ldata(l200, :jldsp, FK_CAL, DET_GED)
        @test full isa TypedTables.Table
        # NB: isequal, not == — e_cusp contains NaN for failed DSP entries
        @test isequal(read_ldata(:e_cusp, l200, (:jldsp, FK_CAL, DET_GED)).e_cusp, full.e_cusp)
        @test isequal(read_ldata((:e_cusp, :timestamp), l200, (:jldsp, FK_CAL, DET_GED)).e_cusp, full.e_cusp)
        @test isequal(read_ldata(@pf((; $e_cusp, $timestamp)), l200, (:jldsp, FK_CAL, DET_GED)).e_cusp, full.e_cusp)
        @test isequal(read_ldata(@pf((; energy = $e_cusp)), l200, (:jldsp, FK_CAL, DET_GED)).energy, full.e_cusp)
        # run-level forms read ALL filekeys of the run (not just FK_CAL): compare the
        # string-selector form (via _convert_rsel2dsel) and the vararg front-end against
        # the canonical run read; FK_CAL is the first filekey, so `full` is its prefix
        run_full = read_ldata((:e_cusp,), l200, (:jldsp, :cal, PER, CALRUN, DET_GED))
        @test length(run_full) >= length(full) && isequal(run_full.e_cusp[1:length(full)], full.e_cusp)
        @test isequal(read_ldata((:e_cusp,), l200, ("jldsp", "cal", string(PER), string(CALRUN), string(DET_GED))).e_cusp, run_full.e_cusp)
        @test isequal(read_ldata((:e_cusp,), l200, :jldsp, :cal, PER, CALRUN, DET_GED).e_cusp, run_full.e_cusp)
    end

    @testset "per-detector reads across tiers" begin
        @test read_ldata((:e_cusp, :t50), l200, (:jldsp, FK_PHY, DET_GED)) isa TypedTables.Table
        # SiPMs only exist in phy jldsp (not in every cal file)
        @test read_ldata(l200, (:jldsp, FK_PHY, DET_SPM)) isa TypedTables.Table
        if have_raw
            raw_ged = read_ldata((:waveform_presummed, :daqenergy), l200, (:raw, FK_PHY, DET_GED))
            @test raw_ged isa TypedTables.Table && !isempty(raw_ged)
            # column-agnostic (raw waveform sets differ between periods)
            @test read_ldata(l200, (:raw, FK_PHY, DET_SPM)) isa TypedTables.Table
        else
            @info "raw tier not present — skipping raw reads"
        end
    end

    @testset "per-detector-file tiers + subgroup descent" begin
        if have_jlhit
            hit_qc = read_ldata((:e_cusp, :timestamp), l200, (:jlhit, FK_CAL, DET_GED); subgroup = :dataQC)
            @test hit_qc isa TypedTables.Table && !isempty(hit_qc)
            @test read_ldata((:is_physical,), l200, (:jlhit, FK_CAL, DET_GED); subgroup = :qc) isa TypedTables.Table
            @test_throws ArgumentError read_ldata((:e_cusp,), l200, (:jlhit, FK_CAL, DET_GED); subgroup = :nope)
            # per-detector-file tier without detector must fail loudly
            @test_throws ArgumentError read_ldata(l200, (:jlhit, FK_CAL))
        else
            @info "jlhit tier not present — skipping"
        end
        if have_jlpls
            pls = read_ldata((:e_10410, :timestamp), l200, (:jlpls, FK_PHY, "PULS01ANA"); subgroup = :tags)
            @test pls isa TypedTables.Table && !isempty(pls)
        else
            @info "jlpls tier not present — skipping"
        end
        if have_jlpeaks
            pk = read_ldata((:daqenergy, :timestamp), l200, (:jlpeaks, FK_CAL, DET_GED); subgroup = :Tl208FEP)
            @test pk isa TypedTables.Table && !isempty(pk)
        else
            @info "jlpeaks tier not present — skipping"
        end
    end

    @testset "no-detector forms" begin
        # per-detector-group tier -> NamedTuple over all detectors
        perdet = read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL))
        @test perdet isa NamedTuple
        @test Symbol(DET_GED) in propertynames(perdet)
        @test getproperty(perdet, Symbol(DET_GED)) isa TypedTables.Table
        # event tier without detector -> native nested view (subsampled for speed)
        nested = read_ldata(l200, (:jlevt, FK_PHY); n_evts = 100)
        @test hasproperty(nested, :geds) && hasproperty(nested, :spms)
        @test length(nested.geds.detector) == 100
    end

    @testset "event tier + detector -> flat-prefixed, sliced table" begin
        evt = read_ldata((:geds_e_cusp_ctc_cal, :geds_trig_e_cusp_ctc_cal), l200, (:jlevt, FK_PHY, DET_GED))
        @test evt isa TypedTables.Table
        @test :geds_e_cusp_ctc_cal in propertynames(evt)
        @test !(eltype(evt.geds_e_cusp_ctc_cal) <: AbstractVector)       # sliced to scalars
        @test !(eltype(evt.geds_trig_e_cusp_ctc_cal) <: AbstractVector)  # per-trigger slice
        spm_evt = read_ldata((:spms_trig_max_cal,), l200, (:jlevt, FK_PHY, DET_SPM))
        @test spm_evt isa TypedTables.Table
        # SPM trig columns are per-(det, trigger): the det slice leaves the per-event
        # trigger LIST of this det (possibly several triggers), not a scalar
        @test eltype(spm_evt.spms_trig_max_cal) <: AbstractVector{<:Number}
        # unknown flat column errors with the available names
        @test_throws ErrorException read_ldata((:geds_no_such_col,), l200, (:jlevt, FK_PHY, DET_GED))
        # subgroup is not defined for event tiers
        @test_throws ArgumentError read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED); subgroup = :geds)
    end

    if have_jlpmt
        @testset "single-system event tier (jlpmt)" begin
            fks_pmt_per = _runs(:jlpmt, :phy, PER)
            fk_pmt_run = DataRun(first(fks_pmt_per))
            fks_pmt = search_disk(FileKey, l200.tier[DataTier(:jlpmt), :phy, PER, fk_pmt_run])
            @test !isempty(fks_pmt)
            FK_PMT = first(fks_pmt)
            pmt_all = read_ldata((:detector, :is_valid_muon), l200, (:jlpmt, FK_PMT))
            @test pmt_all isa TypedTables.Table
            nonempty = filter(!isempty, pmt_all.detector)
            if isempty(nonempty)
                @info "no PMT readout in $FK_PMT — skipping per-detector jlpmt read"
            else
                DET_PMT = first(first(nonempty))
                @test DET_PMT isa DetectorId
                pmt = read_ldata((:pulse_height_cal,), l200, (:jlpmt, FK_PMT, DET_PMT))
                @test pmt isa TypedTables.Table
                @test !(eltype(pmt.pulse_height_cal) <: AbstractVector)
            end
        end
    else
        @info "jlpmt tier not present — skipping"
    end

    @testset "batched multi-detector reads" begin
        # non-event tier: loop per detector, NamedTuple result
        b = read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, [DET_GED, DET_GED2]))
        @test b isa NamedTuple && Set(propertynames(b)) == Set(Symbol.([DET_GED, DET_GED2]))
        @test isequal(getproperty(b, Symbol(DET_GED)).e_cusp, read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, DET_GED)).e_cusp)
        # event tier: one shared physical read, per-detector masks
        f_evt = (:geds_e_cusp_ctc_cal, :spms_trig_max_cal)
        be = read_ldata(f_evt, l200, (:jlevt, FK_PHY, [DET_GED, DET_SPM]))
        @test be isa NamedTuple && Set(propertynames(be)) == Set(Symbol.([DET_GED, DET_SPM]))
        @test isequal(getproperty(be, Symbol(DET_GED)).geds_e_cusp_ctc_cal,
              read_ldata(f_evt, l200, (:jlevt, FK_PHY, DET_GED)).geds_e_cusp_ctc_cal)
        @test isequal(getproperty(be, Symbol(DET_SPM)).spms_trig_max_cal,
              read_ldata(f_evt, l200, (:jlevt, FK_PHY, DET_SPM)).spms_trig_max_cal)
        @test_throws ArgumentError read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, [DET_GED, DET_GED]))
    end

    @testset "filterby" begin
        full = read_ldata((:e_cusp, :timestamp), l200, (:jldsp, FK_CAL, DET_GED))
        sel = read_ldata((:e_cusp, :timestamp), l200, (:jldsp, FK_CAL, DET_GED); filterby = @pf $e_cusp > 1000)
        @test all(sel.e_cusp .> 1000)
        @test sel.timestamp == full.timestamp[findall(coalesce.(full.e_cusp .> 1000, false))]
        # event tier, flat names, incl. cross-subgroup sources
        base = read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED))
        sel1 = read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED);
                          filterby = @pf $geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 20u"keV")
        @test sel1 isa TypedTables.Table && length(sel1) <= length(base)
        sel2 = read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED);
                          filterby = @pf $geds_is_valid_qc && $ged_spm_is_valid_lar &&
                                         !$aux_muonveto_aux_trig && !$aux_pulser_aux_trig)
        @test sel2 isa TypedTables.Table && length(sel2) <= length(base)
    end

    if have_raw
        @testset "filtertier — cross-tier filter" begin
            # (1) raw waveforms of phy events passing a jlevt QC + trigger-energy cut
            raw_sel = read_ldata(@pf((; $waveform_presummed, $timestamp)), l200, (:raw, FK_PHY, DET_GED);
                                 filterby   = @pf($geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV"),
                                 filtertier = :jlevt)
            @test raw_sel isa TypedTables.Table
            n_expected = length(read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED);
                                           filterby = @pf($geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV")))
            @test length(raw_sel) == n_expected
            # (2) symmetric per-trigger: jldsp filtered via a row-aligned raw mask
            dsp_sel = read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY, DET_GED);
                                 filterby = (@pf $daqenergy > 100), filtertier = :raw)
            @test dsp_sel isa TypedTables.Table
            # (3) cal raw filtered via cal jldsp (no jlevt for cal)
            @test read_ldata((:waveform_presummed,), l200, (:raw, FK_CAL, DET_GED);
                             filterby = (@pf $e_cusp > 1000), filtertier = :jldsp) isa TypedTables.Table
            # errors: filtertier without detector; event target with event filtertier
            @test_throws ArgumentError read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY); filtertier = :raw)
            if have_jlpmt
                @test_throws ArgumentError read_ldata((:is_valid_muon,), l200, (:jlpmt, FK_PHY, DET_GED);
                                                      filterby = (@pf $geds_is_valid_qc), filtertier = :jlevt)
            end
        end
    else
        @info "raw tier not present — skipping filtertier tests"
    end

    @testset "n_evts subsampling" begin
        full = read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, DET_GED))
        sub = read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, DET_GED); n_evts = 500)
        @test length(sub) == min(500, length(full))
        @test all(in(Set(full.e_cusp)), sub.e_cusp)
        # n_evts beyond the row count must be the identical full read
        @test isequal(read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, DET_GED); n_evts = 10^9).e_cusp, full.e_cusp)
        # subsample applied after filtering
        fsub = read_ldata((:e_cusp,), l200, (:jldsp, FK_CAL, DET_GED); filterby = @pf($e_cusp > 1000), n_evts = 100)
        @test length(fsub) <= 100 && all(fsub.e_cusp .> 1000)
        # full read without selection, subsampled
        @test length(read_ldata(l200, (:jldsp, FK_CAL, DET_GED); n_evts = 200)) == min(200, length(full))
    end

    @testset "missing detectors and error paths" begin
        @test read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY, "V99999A"); ignore_missing = true) === nothing
        @test_throws ArgumentError read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY, "V99999A"))
    end

    @testset "multi-filekey / run / period / runtable forms" begin
        nfk = min(2, length(fks_phy))
        multi = read_ldata((:e_cusp,), l200, (:jldsp, fks_phy[1:nfk], DET_GED))
        @test multi isa TypedTables.Table
        singles = [read_ldata((:e_cusp,), l200, (:jldsp, fk, DET_GED)) for fk in fks_phy[1:nfk]]
        @test length(multi) == sum(length.(singles))
        @test isequal(multi.e_cusp, vcat((s.e_cusp for s in singles)...))
        # parallel read (worker pool may be just the master process)
        par = read_ldata((:e_cusp,), l200, (:jldsp, fks_phy[1:nfk], DET_GED); parallel = true)
        @test isequal(par.e_cusp, multi.e_cusp)
        # per-filekey n_evts
        sub = read_ldata((:e_cusp,), l200, (:jldsp, fks_phy[1:nfk], DET_GED); n_evts = 50)
        @test length(sub) == 50 * nfk
        # (tier, cat, period, run, det)
        run_read = read_ldata((:e_cusp,), l200, (:jldsp, :cal, PER, CALRUN, DET_GED))
        @test run_read isa TypedTables.Table
        @test length(run_read) == sum(length.(read_ldata((:e_cusp,), l200, (:jldsp, fk, DET_GED)) for fk in fks_cal))
        # explicit runtable
        rtbl = Table(period = [PER], run = [CALRUN])
        @test isequal(read_ldata((:e_cusp,), l200, (:jldsp, :cal, rtbl, DET_GED)).e_cusp, run_read.e_cusp)
        # runtable with extra columns (period/run extracted)
        rtbl2 = Table(period = [PER], run = [CALRUN], comment = ["x"])
        @test isequal(read_ldata((:e_cusp,), l200, (:jldsp, :cal, rtbl2, DET_GED)).e_cusp, run_read.e_cusp)
        # (tier, cat, period, det) via runinfo — only when every runinfo run is on disk
        rinfo = runinfo(l200, PER)
        if all(r -> isdir(joinpath(_catdir(:jldsp, :cal), string(PER), string(r.run))), rinfo)
            per_read = read_ldata((:e_cusp,), l200, (:jldsp, :cal, PER, DET_GED))
            @test per_read isa TypedTables.Table && length(per_read) >= length(run_read)
        else
            @info "period $PER has runinfo runs without cal jldsp data — skipping the period form"
        end
    end

    if have_jlhit
        @testset "partition form (per-detector-file tier)" begin
            parts = partitioninfo(l200, DET_GED, :cal)
            ondisk(pinfo) = all(r -> isfile(l200.tier[DataTier(:jlhit), start_filekey(l200, (r.period, r.run, :cal)), DET_GED]), pinfo)
            cand = [p for (p, pinfo) in parts if !isempty(pinfo) && ondisk(pinfo)]
            if isempty(cand)
                @info "no partition of $DET_GED fully on disk — skipping the partition form"
            else
                part = first(sort(cand))
                pinfo = parts[part]
                pread = read_ldata((:timestamp,), l200, (:jlhit, :cal, part, DET_GED); subgroup = :dataQC)
                @test pread isa TypedTables.Table
                nruns = sum(length(read_ldata((:timestamp,), l200,
                    (:jlhit, start_filekey(l200, (r.period, r.run, :cal)), DET_GED); subgroup = :dataQC)) for r in pinfo)
                @test length(pread) == nruns
            end
        end
    end

    @testset "fast path is lighter + faster than load-all-then-filter" begin
        f    = @pf((; $geds_e_cusp_ctc_cal))
        filt = @pf $geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV"
        read_ldata(f, l200, (:jlevt, FK_PHY, DET_GED); filterby = filt)    # warm up (compile)
        read_ldata(l200, (:jlevt, FK_PHY, DET_GED))
        fast = @timed read_ldata(f, l200, (:jlevt, FK_PHY, DET_GED); filterby = filt)
        slow = @timed begin
            full = read_ldata(l200, (:jlevt, FK_PHY, DET_GED))             # all flat columns materialized
            full[coalesce.(filt.(full), false)]                            # then filter in memory
        end
        @info "read_ldata fast-path benchmark" fast_s=fast.time slow_s=slow.time fast_MB=fast.bytes/1e6 slow_MB=slow.bytes/1e6
        @test fast.bytes < slow.bytes     # loads only the needed columns
        @test fast.time  < slow.time      # and is faster
    end

    if have_raw
        @testset "full reads do not double-copy (allocation regression)" begin
            rd() = read_ldata((:waveform_presummed,), l200, (:raw, FK_PHY, DET_GED))
            wf = rd()                       # warm up (compile + page cache)
            payload = Base.summarysize(wf)
            alloc = @allocated rd()
            @info "full-read allocation" payload_MB=payload/1e6 alloc_MB=alloc/1e6 ratio=alloc/payload
            # the historical arr[:][1:n] double copy makes this ratio >= 2
            @test alloc < 1.7 * payload
        end
    end
end
