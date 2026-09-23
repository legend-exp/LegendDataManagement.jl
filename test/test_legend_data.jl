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

        @test l200.dataset == :default
        @test LegendData(:l200; dataset = :valid).dataset == :valid
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

        # A selection without a category uses :cal
        @test channelinfo(l200, filekey.period, filekey.run) == channelinfo(l200, (filekey.period, filekey.run, :cal))
        @test channelinfo(l200, (filekey.period, filekey.run)) == channelinfo(l200, (filekey.period, filekey.run, :cal))
        @test channelinfo(l200, ((filekey.period, filekey.run), :B99000A)).usability == :off

        # Check error handling for invalid input
        @test_throws ArgumentError channelinfo(l200, "not a period")

        # Period channel info merges the channel info of all runs of a category
        period_chinfo = channelinfo(l200, filekey.period, :cal)
        @test channelinfo(l200, filekey.period) == period_chinfo
        @test period_chinfo isa TypedTables.Table
        @test channelinfo(l200, (filekey.period, :cal)) == period_chinfo
        @test channelinfo(l200, ("p02", "cal")) == period_chinfo
        @test columnnames(period_chinfo) == columnnames(chinfo)
        @test period_chinfo.detector == chinfo.detector
        # Detector B99000A is usable in run r000 only: the period takes the best status over its runs
        @test only(filterby(@pf $detector == DetectorId(:B99000A))(chinfo).usability) == :off
        @test only(filterby(@pf $detector == DetectorId(:B99000A))(period_chinfo).usability) == :on
        @test channelinfo(l200, ((filekey.period, :cal), :B99000A)).usability == :on
        @test all(filterby(@pf $system == :geds)(period_chinfo).usability .== :on)
        @test channelinfo(l200, filekey.period, :cal; system = :geds, only_usability = :on).detector == filterby(@pf $system == :geds)(chinfo).detector
        @test isempty(channelinfo(l200, filekey.period, :cal; only_usability = :off))
        @test all(in(columnnames(channelinfo(l200, filekey.period, :cal; extended = true))), extended_keywords)
        # every run of p02 is an analysis run
        @test channelinfo(l200, filekey.period, :cal; only_analysis_runs = false) == period_chinfo
        @test_throws ArgumentError channelinfo(l200, :p99, :cal)
        @test_throws ArgumentError channelinfo(l200, filekey.period, :xtc)

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
        # LegendTestData holds no files in the tiers of `l200` to find.
        @test_broken !(isempty(search_disk(DataSet, l200)))
        # check search_disk
        @test search_disk(DataTier, l200_lh5.tier[]) isa Vector{DataTier}
        @test search_disk(DataCategory, l200_lh5.tier[:dsp]) isa Vector{DataCategory}
        @test search_disk(DataPeriod, l200_lh5.tier[:dsp, :cal]) isa Vector{DataPeriod}
        @test search_disk(DataRun, l200_lh5.tier[:dsp, :cal, :p03]) isa Vector{DataRun}
        @test search_disk(FileKey, l200_lh5.tier[:dsp, :cal, :p03, :r000]) isa Vector{FileKey}
    end
end
