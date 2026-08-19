# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using LegendHDF5IO
using PropertyFunctions
using TypedTables

using HDF5

@testset "test_ext_legendhdf5io" begin

    @testset "extension internals: _sample_idx / _subsample" begin
        Ext = Base.get_extension(LegendDataManagement, :LegendDataManagementLegendHDF5IOExt)
        @test Ext !== nothing

        # _sample_idx: full-read sentinel values return the identity range
        @test Ext._sample_idx(10, -1) == 1:10
        @test Ext._sample_idx(10, 0) == 1:10
        @test Ext._sample_idx(10, 10) == 1:10
        @test Ext._sample_idx(10, 11) == 1:10
        idx = Ext._sample_idx(1000, 50)
        @test length(idx) == 50 && issorted(idx) && allunique(idx) && all(i -> 1 <= i <= 1000, idx)

        # _subsample: full reads must return the input UNCOPIED (=== is the point:
        # regression test for the historical arr[:][1:n] double copy)
        v = collect(1.0:1000.0)
        @test Ext._subsample(v, -1) === v
        @test Ext._subsample(v, 0) === v
        @test Ext._subsample(v, 1000) === v
        @test Ext._subsample(v, 10^9) === v
        s = Ext._subsample(v, 100)
        @test s isa Vector{Float64} && length(s) == 100 && issorted(s) && allunique(s) && all(in(v), s)

        t = Table(a = collect(1:100), b = collect(0.5:1.0:100.0))
        @test Ext._subsample(t, -1) === t
        @test Ext._subsample(t, 200) === t
        st = Ext._subsample(t, 10)
        @test st isa TypedTables.Table && length(st) == 10 && issorted(st.a) && allunique(st.a)
    end

    @testset "DetectorId LH5 I/O" begin
        mktempdir() do tmpdir
            # Create a temporary file for testing
            test_filename = joinpath(tmpdir, "detidtest.lh5")

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
            test_filename_str = joinpath(tmpdir, "strdetidtest.lh5")
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
#  read_ldata production integration tests: the full call-form matrix
#  (all tiers, all selector forms, filterby / filtertier / n_evts / subgroup /
#  batched & parallel reads) against a real jl-v0.6.0+ production.
#
#  These need new-schema (DetectorId) tier data, which LegendTestData does not
#  ship yet. They activate when LEGEND_TESTDATA_PROD_CONFIG points at a
#  production config, e.g.:
#      ENV["LEGEND_TESTDATA_PROD_CONFIG"] =
#          "/ptmp/oschulz/legend/data/l200/juleana/tmp/jl-v0.7.0/config.json"
#  Period/run are auto-detected from disk; override via
#  LEGEND_TESTDATA_PROD_PERIOD / _RUN / _CALRUN if needed.
# ============================================================================
if isempty(get(ENV, "LEGEND_TESTDATA_PROD_CONFIG", ""))
    @info "Skipping read_ldata production integration tests — set LEGEND_TESTDATA_PROD_CONFIG=<path/to/config.json> of a jl-v0.6.0+ production to enable them."
else
    include("test_ext_legendhdf5io_prod.jl")
end
