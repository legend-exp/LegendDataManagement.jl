# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

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

    @testset "LegendData" begin
        props_base_path = data_path(LegendDataConfig().setups.l200, "metadata")
        @test l200.metadata isa LegendDataManagement.PropsDB

        @test l200.dataset == :valid
        #@test LegendData(:l200_nu24).dataset == :nu24 
        #@test LegendData(:l200_nu24; dataset = :valid).dataset == :valid
    end

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
end
