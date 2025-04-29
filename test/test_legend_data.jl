# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test
using LegendHDF5IO

using StructArrays, PropertyFunctions, TypedTables
using Measurements: uncertainty

include("testing_utils.jl")

@testset "legend_data" begin
    l200 = LegendData(:l200)

    @test @inferred(data_path(l200, "tier", "raw", "cal", "p02", "r006", "l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5")) isa AbstractString
    @test @inferred(data_path(l200, "tier/raw/cal/p02/r006/l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5")) isa AbstractString

    filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")

    @test getproperty(l200, :tier) isa LegendDataManagement.LegendTierData
    @test normalize_path(@inferred(l200.tier[:raw, filekey])) == "/some/other/storage/raw_lh5/cal/p02/r006/l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5"
    @test normalize_path(@inferred(l200.tier[:raw, "l200-p02-r006-cal-20221226T200846Z"])) == "/some/other/storage/raw_lh5/cal/p02/r006/l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5"
    @test normalize_path(@inferred(l200.tier[:dsp, "l200-p02-r006-cal-20221226T200846Z"])) == normalize_path(joinpath(testdata_dir, "generated", "tier", "dsp", "cal", "p02", "r006", "l200-p02-r006-cal-20221226T200846Z-tier_dsp.lh5"))

    props_base_path = data_path(LegendDataConfig().setups.l200, "metadata")
    @test l200.metadata isa LegendDataManagement.PropsDB

    @testset "channelinfo" begin
        # ToDo: Make type-stable:
        @test channelinfo(l200, filekey) isa TypedTables.Table
        chinfo = channelinfo(l200, filekey)
        @test all(filterby(@pf $processable && $usability == :on)(chinfo).processable)
        @test all(filterby(@pf $processable && $usability == :on)(chinfo).usability .== :on)

        # Test the extended channel info with active volume calculation
        extended = channelinfo(l200, filekey, only_usability = :on, extended = true)
        @test extended isa TypedTables.Table

        # Check that some keywords only appear in the extended channelinfo
        extended_keywords = (:cc4, :cc4ch, :daqcrate, :daqcard, :hvcard, :hvch, :enrichment, :mass, :total_volume, :active_volume)
        @test !any(in(columnnames(chinfo)),   extended_keywords)
        @test  all(in(columnnames(extended)), extended_keywords)
        @test !any(iszero.(uncertainty.(extended.fccd)))
        @test !any(iszero.(uncertainty.(extended.active_volume)))

        # ToDo: Make type-stable:
        # @test #=@inferred=#(channel_info(l200, filekey)) isa StructArray
        # chinfo = channel_info(l200, filekey)
        # @test all(filterby(@pf $processable && $usability == :on)(chinfo).processable)
        # @test all(filterby(@pf $processable && $usability == :on)(chinfo).usability .== :on)
    end

    # different config to check lh5 files
    lh5testdata_dir = joinpath(legend_test_data_path(), "data", "lh5", "prod-ref-l200")
    ENV["LEGEND_DATA_CONFIG"] = joinpath(lh5testdata_dir, "config.json")

    l200_lh5 = LegendData(:l200)

    @testset "search_disk" begin
        datasets = search_disk(DataSet, l200)
        # LegendTestData is probably not in the correct formats
        @test_broken !(isempty(datasets))
        # check search_disk
        @test search_disk(DataTier, l200_lh5.tier[]) isa Vector{DataTier}
        @test search_disk(DataCategory, l200_lh5.tier[:dsp]) isa Vector{DataCategory}
        @test search_disk(DataPeriod, l200_lh5.tier[:dsp, :cal]) isa Vector{DataPeriod}
        @test search_disk(DataRun, l200_lh5.tier[:dsp, :cal, :p03]) isa Vector{DataRun}
        @test search_disk(FileKey, l200_lh5.tier[:dsp, :cal, :p03, :r000]) isa Vector{FileKey}
    end

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
