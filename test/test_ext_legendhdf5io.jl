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
