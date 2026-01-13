# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using LegendHDF5IO
using LegendTestData
using PropertyFunctions
using TypedTables

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
        @test DetectorlId(det_str) isa DetectorlId
        @test data_fk isa TypedTables.Table
        det = DetectorlId(det_str)

        # test read_ldata
        @test read_ldata(l200_lh5, tier, cat, period, run, det) isa TypedTables.Table
        @test read_ldata(l200_lh5, tier, cat, period, run, det).timestamp == data_fk.timestamp
        @test read_ldata(:timestamp, l200_lh5, tier, cat, period, run, det).timestamp == data_fk.timestamp
        @test read_ldata((:timestamp, :baseline), l200_lh5, tier, cat, period, run, det).timestamp == data_fk.timestamp
        @test read_ldata((@pf (; bltime = $timestamp * $baseline, )), l200_lh5, tier, cat, period, run, det).bltime == data_fk.timestamp .* data_fk.baseline
        @test read_ldata(l200_lh5, tier, fk, ch).timestamp == data_fk.timestamp
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
end
