# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using LegendHDF5IO
using LegendTestData
using PropertyFunctions
using TypedTables

using HDF5

@testset "test_ext_legendhdf5io" begin

    lh5testdata_dir = joinpath(legend_test_data_path(), "data", "lh5", "prod-ref-l200")
    ENV["LEGEND_DATA_CONFIG"] = joinpath(lh5testdata_dir, "config.json")

    l200_lh5 = LegendData(:l200)
    # disabled until testdata updated to Det ID key format
    #=
    @testset "read_ldata" begin
        # Test the read_ldata function
        period = DataPeriod(3)
        run = DataRun(0)
        cat = DataCategory(:cal)
        tier = DataTier(:dsp)
        fk = search_disk(FileKey, l200_lh5.tier[tier, cat, period, run])[1]

        det_str, data_fk = lh5open(l200_lh5.tier[tier, fk]) do f
            first(keys(f)), f[Symbol(first(keys(f))), Symbol(tier)][:]
        end
        @test DetectorId(det_str) isa DetectorId
        @test data_fk isa TypedTables.Table
        det = DetectorId(det_str)

        # test read_ldata
        @test read_ldata(l200_lh5, tier, cat, period, run, det) isa TypedTables.Table
        @test read_ldata(l200_lh5, tier, cat, period, run, det).timestamp == data_fk.timestamp
        @test read_ldata(:timestamp, l200_lh5, tier, cat, period, run, det).timestamp == data_fk.timestamp
        @test read_ldata((:timestamp, :baseline), l200_lh5, tier, cat, period, run, det).timestamp == data_fk.timestamp
        @test read_ldata((@pf (; bltime = $timestamp * $baseline, )), l200_lh5, tier, cat, period, run, det).bltime == data_fk.timestamp .* data_fk.baseline
        @test read_ldata(l200_lh5, tier, fk, det).timestamp == data_fk.timestamp
        @test read_ldata(l200_lh5, tier, cat, period, run) isa TypedTables.Table

        # test parallel read
        @test read_ldata(l200_lh5, tier, cat, period, run, det; parallel=true) isa TypedTables.Table
        @test read_ldata(l200_lh5, tier, cat, period, run, det; parallel=true).timestamp == data_fk.timestamp
        @test read_ldata(:timestamp, l200_lh5, tier, cat, period, run, det; parallel=true).timestamp == data_fk.timestamp
        @test read_ldata((:timestamp, :baseline), l200_lh5, tier, cat, period, run, det; parallel=true).timestamp == data_fk.timestamp
        @test read_ldata((@pf (; bltime = $timestamp * $baseline, )), l200_lh5, tier, cat, period, run, det; parallel=true).bltime == data_fk.timestamp .* data_fk.baseline
        @test read_ldata(l200_lh5, tier, fk, det; parallel=true).timestamp == data_fk.timestamp
        @test read_ldata(l200_lh5, tier, cat, period, run; parallel=true) isa TypedTables.Table

        # test filterby
        @test read_ldata(l200_lh5, tier, cat, period, run, det; filterby=@pf($daqenergy > 1000)) isa TypedTables.Table
        @test all(read_ldata(l200_lh5, tier, cat, period, run, det; filterby=@pf($daqenergy > 1000)).daqenergy .> 1000)
        @test read_ldata(:timestamp, l200_lh5, tier, cat, period, run, det; filterby=@pf($daqenergy > 1000)).timestamp == data_fk.timestamp[findall(data_fk.daqenergy .> 1000)]        
        @test read_ldata((:timestamp, :baseline), l200_lh5, tier, cat, period, run, det; filterby=@pf($daqenergy > 1000)).timestamp == data_fk.timestamp[findall(data_fk.daqenergy .> 1000)]
        @test read_ldata((@pf (; bltime = $timestamp * $baseline, )), l200_lh5, tier, cat, period, run, det; filterby=@pf($daqenergy > 1000)).bltime == data_fk.timestamp[findall(data_fk.daqenergy .> 1000)] .* data_fk.baseline[findall(data_fk.daqenergy .> 1000)]
        
        # test multi run read
        #ToDo: update legend-testdata to make possible
        # rinfo = Table([(period = DataPeriod(3), run = DataRun(0)), (period = DataPeriod(3), run = DataRun(1))])
    end
    =#
    
    @testset "DetectorId LH5 I/O" begin
        mktempdir() do tmpdir
            # Create a temporary file for testing
            test_filename = "detidtest.lh5"
            
            try
                # Test writing and reading a single DetectorId
                det1 = DetectorId("V99999J")
                det2 = DetectorId("B59231A")
                det3 = DetectorId("PULS99ANA")
                
                # Write DetectorIds
                lh5open(test_filename, "w") do f
                    f["single_det"] = det1
                    f["det_array"] = [det1, det2, det3]
                end
                
                # Read back and verify
                lh5open(test_filename, "r") do f
                    # Single DetectorId
                    read_det1 = f["single_det"]
                    @test read_det1 == det1
                    @test read_det1 isa DetectorId
                    
                    # Array of DetectorIds
                    read_dets = f["det_array"][:]
                    @test read_dets == [det1, det2, det3]
                    @test eltype(read_dets) <: DetectorId
                end
                
                # Verify the data is stored as UInt32
                HDF5.h5open(test_filename, "r") do h5f
                    single_data = read(h5f["single_det"])
                    @test single_data isa UInt32
                    @test single_data == UInt32(det1)
                    
                    array_data = read(h5f["det_array"])
                    @test eltype(array_data) == UInt32
                    @test array_data == UInt32.([det1, det2, det3])
                end
                
            finally
                isfile(test_filename) && rm(test_filename)
            end
            
            # Test reading DetectorId from string representation (backward compatibility)
            test_filename_str = "strdetidtest.lh5"
            try
                # Write as string (simulating old format)
                HDF5.h5open(test_filename_str, "w") do h5f
                    h5f["det_string"] = "V99999J"
                    HDF5.attributes(h5f["det_string"])["datatype"] = "detectorid"
                    
                    h5f["det_array_string"] = ["V99999J", "B59231A", "PULS99ANA"]
                    HDF5.attributes(h5f["det_array_string"])["datatype"] = "array<1>{detectorid}"
                end
                
                # Read back via LH5Array
                lh5open(test_filename_str, "r") do f
                    read_det = f["det_string"]
                    @test read_det == DetectorId("V99999J")
                    @test read_det isa DetectorId
                    
                    read_dets = f["det_array_string"][:]
                    @test read_dets == [DetectorId("V99999J"), DetectorId("B59231A"), DetectorId("PULS99ANA")]
                end
                
            finally
                isfile(test_filename_str) && rm(test_filename_str)
            end
        end # tmpdir
    end
end

# ============================================================================
#  read_ldata — new DetectorId API integration tests:
#  per-detector event-tier slicing, filtertier (cross-tier filter), subgroup
#  descent, the PropSel+filterby fast path, and a fast-path-vs-load-all benchmark.
#
#  DISABLED by default: these need real new-schema (jl-v0.6.0+) tier data, which
#  is NOT shipped via LegendTestData. To ACTIVATE:
#    1. point LEGEND_DATA_CONFIG at a config over jl-v0.6.0+ data, e.g.
#         ENV["LEGEND_DATA_CONFIG"] =
#             "/ptmp/oschulz/legend/data/l200/tmp/jl-v0.6.0dev7/config.json"
#    2. set PER / RUN / CALRUN below to a period and runs present in your data
#    3. delete the surrounding  #=  …  =#
# ============================================================================
#=
using Unitful: @u_str

@testset "read_ldata new API (jl-v0.6.0+ data)" begin
    l200 = LegendData(:l200)
    PER, RUN, CALRUN = DataPeriod(19), DataRun(2), DataRun(5)

    fks_phy = search_disk(FileKey, l200.tier[:raw, :phy, PER, RUN])
    FK_PHY  = first(fks_phy)
    FK_CAL  = first(search_disk(FileKey, l200.tier[:jldsp, :cal, PER, CALRUN]))

    chinfo  = channelinfo(l200, FK_PHY)
    DET_GED = first(filter(c -> c.system == :geds && c.processable, chinfo)).detector
    DET_SPM = first(filter(c -> c.system == :spms, chinfo)).detector

    @testset "column selection — three equivalent forms (+ rename)" begin
        full = read_ldata(l200, :jldsp, FK_PHY, DET_GED)
        @test full isa Table
        @test read_ldata(:e_cusp, l200, (:jldsp, FK_PHY, DET_GED)).e_cusp == full.e_cusp
        @test read_ldata((:e_cusp, :timestamp), l200, (:jldsp, FK_PHY, DET_GED)).e_cusp == full.e_cusp
        @test read_ldata(@pf((; $e_cusp, $timestamp)), l200, (:jldsp, FK_PHY, DET_GED)).e_cusp == full.e_cusp
        @test read_ldata(@pf((; energy = $e_cusp)), l200, (:jldsp, FK_PHY, DET_GED)).energy == full.e_cusp
    end

    @testset "per-detector across tiers (system auto-detected)" begin
        @test read_ldata((:waveform_presummed,), l200, (:raw,   FK_PHY, DET_GED)) isa Table
        @test read_ldata((:waveform_bit_drop,),  l200, (:raw,   FK_PHY, DET_SPM)) isa Table
        @test read_ldata((:e_cusp, :t50),        l200, (:jldsp, FK_PHY, DET_GED)) isa Table
    end

    @testset "subgroup descent (jlhit / jlpls)" begin
        @test read_ldata((:e_cusp,),    l200, (:jlhit, FK_CAL, DET_GED); subgroup=:dataQC) isa Table
        @test read_ldata((:daqenergy,), l200, (:jlpls, FK_CAL, "PULS01"); subgroup=:tags)  isa Table
        @test_throws ArgumentError read_ldata((:e_cusp,), l200, (:jlhit, FK_CAL, DET_GED); subgroup=:nope)
    end

    @testset "event tier + detector -> flat-prefixed, sliced table" begin
        evt = read_ldata((:geds_e_cusp_ctc_cal, :geds_trig_e_cusp_ctc_cal),
                         l200, (:jlevt, FK_PHY, DET_GED))
        @test evt isa Table
        @test :geds_e_cusp_ctc_cal      in propertynames(evt)
        @test :geds_trig_e_cusp_ctc_cal in propertynames(evt)
        @test eltype(evt.geds_e_cusp_ctc_cal) <: Union{Missing, Number}   # sliced to scalars, not VoV
        # no detector -> native nested LH5 view (back-compat)
        nested = read_ldata(l200, :jlevt, FK_PHY)
        @test hasproperty(nested, :geds)
    end

    @testset "filterby on flat names (single + cross-subgroup)" begin
        base = read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED))
        sel  = read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED);
                          filterby = @pf $geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 20u"keV")
        @test sel isa Table
        @test length(sel) <= length(base)
        # cross-subgroup QC: geds + ged_spm + !aux_muonveto + !aux_pulser
        sel2 = read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, FK_PHY, DET_GED);
                          filterby = @pf $geds_is_valid_qc && $ged_spm_is_valid_lar &&
                                         !$aux_muonveto_aux_trig && !$aux_pulser_aux_trig)
        @test sel2 isa Table
        @test length(sel2) <= length(base)
    end

    @testset "filtertier — cross-tier filter" begin
        # (1) raw waveforms of phy events passing a jlevt QC + trigger-energy cut
        raw = read_ldata(@pf((; $waveform_presummed, $timestamp)), l200, (:raw, FK_PHY, DET_GED);
                         filterby   = @pf $geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV",
                         filtertier = :jlevt)
        @test raw isa Table
        # (2) symmetric per-trigger: jldsp filtered via raw
        @test read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY, DET_GED);
                         filterby = (@pf $daqenergy > 100), filtertier = :raw) isa Table
        # (3) cal raw filtered via cal jldsp (no jlevt for cal)
        @test read_ldata((:waveform_presummed,), l200, (:raw, FK_CAL, DET_GED);
                         filterby = (@pf $e_cusp > 1000), filtertier = :jldsp) isa Table
    end

    @testset "edge cases" begin
        @test length(read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY, DET_GED); n_evts=500)) <= 500
        # validly-formatted but absent detector: ignore_missing -> nothing; otherwise throws
        @test read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY, "V99999A"); ignore_missing=true) === nothing
        @test_throws ArgumentError read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY, "V99999A"))
        # filtertier needs a DetectorId
        @test_throws ArgumentError read_ldata((:e_cusp,), l200, (:jldsp, FK_PHY); filtertier=:raw)
        # event<->event filtertier rejected
        @test_throws ArgumentError read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlpmt, FK_PHY, DET_GED);
                                              filterby=(@pf $geds_is_valid_qc), filtertier=:jlevt)
    end

    @testset "multi-filekey / period / run forms" begin
        @test read_ldata((:e_cusp,), l200, (:jldsp, fks_phy[1:min(2, length(fks_phy))], DET_GED)) isa Table
        @test read_ldata((:e_cusp,), l200, (:jldsp, :phy, PER, RUN, DET_GED)) isa Table
    end

    @testset "fast path is lighter + faster than load-all-then-filter" begin
        f    = @pf((; $geds_e_cusp_ctc_cal))
        filt = @pf $geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV"
        read_ldata(f, l200, (:jlevt, FK_PHY, DET_GED); filterby=filt)   # warm up (compile)
        read_ldata(l200, (:jlevt, FK_PHY, DET_GED))
        fast = @timed read_ldata(f, l200, (:jlevt, FK_PHY, DET_GED); filterby=filt)
        slow = @timed begin
            full = read_ldata(l200, (:jlevt, FK_PHY, DET_GED))          # all flat columns materialized
            full[coalesce.(filt.(full), false)]                         # then filter in memory
        end
        @info "read_ldata fast-path benchmark" fast_s=fast.time slow_s=slow.time fast_MB=fast.bytes/1e6 slow_MB=slow.bytes/1e6
        @test fast.bytes < slow.bytes     # loads only the needed columns
        @test fast.time  < slow.time      # and is faster
    end
end
=#
