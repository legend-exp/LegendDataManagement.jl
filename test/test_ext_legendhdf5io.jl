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

    @testset "read_ldata" begin
        # Test the read_ldata function
        period = DataPeriod(3)
        run = DataRun(0)
        cat = DataCategory(:cal)
        tier = DataTier(:dsp)
        fk = search_disk(FileKey, l200_lh5.tier[tier, cat, period, run])[1]

        ch_str, data_fk = lh5open(l200_lh5.tier[tier, fk]) do f
            first(keys(f)), f[Symbol(first(keys(f))), Symbol(tier)][:]
        end
        @test ChannelId(ch_str) isa ChannelId
        @test data_fk isa TypedTables.Table
        ch = ChannelId(ch_str)

        # test read_ldata
        @test read_ldata(l200_lh5, tier, cat, period, run, ch) isa TypedTables.Table
        @test read_ldata(l200_lh5, tier, cat, period, run, ch).timestamp == data_fk.timestamp
        @test read_ldata(:timestamp, l200_lh5, tier, cat, period, run, ch).timestamp == data_fk.timestamp
        @test read_ldata((:timestamp, :baseline), l200_lh5, tier, cat, period, run, ch).timestamp == data_fk.timestamp
        @test read_ldata(@pf($timestamp * $baseline), l200_lh5, tier, cat, period, run, ch) == data_fk.timestamp .* data_fk.baseline
        @test read_ldata(l200_lh5, tier, fk, ch).timestamp == data_fk.timestamp
        @test read_ldata(l200_lh5, tier, cat, period, run) isa TypedTables.Table

        # test parallel read
        @test read_ldata(l200_lh5, tier, cat, period, run, ch; parallel=true) isa TypedTables.Table
        @test read_ldata(l200_lh5, tier, cat, period, run, ch; parallel=true).timestamp == data_fk.timestamp
        @test read_ldata(:timestamp, l200_lh5, tier, cat, period, run, ch; parallel=true).timestamp == data_fk.timestamp
        @test read_ldata((:timestamp, :baseline), l200_lh5, tier, cat, period, run, ch; parallel=true).timestamp == data_fk.timestamp
        @test read_ldata(@pf($timestamp * $baseline), l200_lh5, tier, cat, period, run, ch; parallel=true) == data_fk.timestamp .* data_fk.baseline
        @test read_ldata(l200_lh5, tier, fk, ch; parallel=true).timestamp == data_fk.timestamp
        @test read_ldata(l200_lh5, tier, cat, period, run; parallel=true) isa TypedTables.Table
        
        # test multi run read
        #ToDo: update legend-testdata to make possible
        # rinfo = Table([(period = DataPeriod(3), run = DataRun(0)), (period = DataPeriod(3), run = DataRun(1))])
    end
end
