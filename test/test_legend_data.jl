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

    @testset "run override" begin
        # a par and a metadata database with one file per run and category
        dir = mktempdir()
        runs = [:r000 => "20230101T000000Z", :r001 => "20230201T000000Z", :r002 => "20230301T000000Z", :r003 => "20230401T000000Z", :r004 => "20230501T000000Z"]
        for db in (joinpath(dir, "par", "rpars", "ecal"), joinpath(dir, "metadata", "hardware", "configuration", "channelmaps"))
            mkpath(db)
            write(joinpath(db, "validity.yaml"), join(["""
                - valid_from: $t
                  category: $cat
                  mode: reset
                  apply: [$run-$cat.yaml]
                """ for (run, t) in runs for cat in (:cal, :phy)]))
            for (run, _) in runs, cat in (:cal, :phy)
                write(joinpath(db, "$run-$cat.yaml"), "run: $run\ncategory: $cat\n")
            end
        end
        # r001 uses r002-cal, and in the overrides package r004 uses r003-cal
        mkpath(joinpath(dir, "metadata", "datasets"))
        write(joinpath(dir, "metadata", "datasets", "run_override.yaml"), """
        - valid_from: 20230201T000000Z
          apply:
            - l200-p01-r002-cal-20230301T000000Z
        - valid_from: 20230301T000000Z
          mode: reset
          apply: []
        """)
        mkpath(joinpath(dir, "metadata", "jldataprod", "overrides", "metadata", "datasets"))
        write(joinpath(dir, "metadata", "jldataprod", "overrides", "metadata", "datasets", "run_override.yaml"), """
        - valid_from: 20230501T000000Z
          apply:
            - l200-p01-r003-cal-20230401T000000Z
        """)
        paths = LegendDataManagement.PropDict(:metadata => joinpath(dir, "metadata"), :par => joinpath(dir, "par"), :jlpar => joinpath(dir, "par"))
        ldata = LegendData(SetupConfig(LegendDataManagement.PropDict(:paths => paths)), :l200, :default)

        r001_cal = FileKey("l200-p01-r001-cal-20230201T000000Z")
        @test ldata.par.rpars.ecal(r001_cal).run == "r002"
        @test ldata.par(r001_cal).rpars.ecal.run == "r002"
        @test ldata.jlpar.rpars.ecal(r001_cal).run == "r002"
        @test LegendDataManagement._get_ecal_props(ldata, r001_cal).run == "r002"
        @test ldata.metadata.hardware.configuration.channelmaps(r001_cal).run == "r001"
        # a phy selection keeps its category
        @test ldata.par.rpars.ecal(FileKey("l200-p01-r001-phy-20230205T000000Z")) == LegendDataManagement.PropDict(:run => "r002", :category => "phy")
        # before the override, after its reset, and the override of the overrides package
        @test ldata.par.rpars.ecal(FileKey("l200-p01-r000-phy-20230105T000000Z")).run == "r000"
        @test ldata.par.rpars.ecal(FileKey("l200-p01-r003-phy-20230405T000000Z")).run == "r003"
        @test ldata.par.rpars.ecal(FileKey("l200-p01-r004-phy-20230505T000000Z")) == LegendDataManagement.PropDict(:run => "r003", :category => "phy")
    end

    @testset "channelinfo" begin
        # ToDo: Make type-stable:
        @test channelinfo(l200, filekey) isa TypedTables.Table
        chinfo = channelinfo(l200, filekey)
        # files of the same run and validity entries share one cached channel info
        other_filekey = FileKey("l200-p02-r006-cal-20221226T210000Z")
        @test LegendDataManagement._channelinfo_inputs(l200, other_filekey, false) == LegendDataManagement._channelinfo_inputs(l200, filekey, false)
        @test channelinfo(l200, other_filekey) == chinfo
        @test LegendDataManagement._channelinfo_inputs(l200, FileKey("l200-p02-r007-cal-20221226T210000Z"), false) != LegendDataManagement._channelinfo_inputs(l200, filekey, false)

        # metadata of selections with the same validity entry is read once and shared
        cache = LegendDataManagement.LRU{LegendDataManagement._ValidityKey, Any}(maxsize = 10)
        get_chmaps = () -> l200.metadata.hardware.configuration.channelmaps
        chmaps = LegendDataManagement._validity_cached(identity, cache, get_chmaps, l200, filekey, (:test, :channelmaps))
        @test chmaps == get_chmaps()(filekey)
        @test LegendDataManagement._validity_cached(identity, cache, get_chmaps, l200, other_filekey, (:test, :channelmaps)) === chmaps
        @test LegendDataManagement._validity_cached(identity, cache, get_chmaps, l200, FileKey("l200-p03-r000-cal-20230311T235840Z"), (:test, :channelmaps)) != chmaps
        @test length(cache) == 2
        get_missing = () -> l200.metadata.hardware.configuration.nosuchentry
        @test_throws ArgumentError LegendDataManagement._validity_cached(identity, cache, get_missing, l200, filekey, (:test, :missing))
        @test LegendDataManagement._validity_cached(identity, cache, get_missing, l200, filekey, (:test, :missing); allow_missing = true) == LegendDataManagement.PropDict()
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
        i = findfirst(==(:is_bb_like), columnnames(chinfo))
        @test columnnames(period_chinfo) == columnnames(chinfo) && i + 1 == findfirst(==(:partition), columnnames(chinfo))
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

        # the partitions of each detector that contain the period
        geds = channelinfo(l200, filekey.period, :cal; system = :geds)
        @test geds.partition isa Vector{Vector{DataPartition}}
        @test all(==([DataPartition(:calpartition001a)]), geds.partition)
        @test geds.partition == partitioninfo.(l200, geds.detector, filekey.period, :cal)
        exploded = channelinfo(l200, filekey.period, :cal; system = :geds, explode_partitions = true)
        @test exploded.partition isa Vector{DataPartition}
        @test exploded.detector == geds.detector && all(==(DataPartition(:calpartition001a)), exploded.partition)
        # the partition of each detector that contains the run
        @test isequal(chinfo.partition, channelinfo(l200, ValiditySelection(filekey)).partition)
        run_chinfo = channelinfo(l200, filekey.period, filekey.run, :cal; system = :geds)
        @test isequal(run_chinfo, filterby(@pf $system == :geds)(chinfo))
        @test columnnames(run_chinfo) == columnnames(geds)
        @test run_chinfo.partition == [only(partitioninfo(l200, det, :cal, filekey.period, filekey.run)) for det in run_chinfo.detector]
        @test all(==(DataPartition(:calpartition001a)), run_chinfo.partition)
        @test channelinfo(l200, filekey.period, filekey.run, :cal; system = :geds, explode_partitions = true).partition == DataPartition.(fill(:calpartition001a, length(run_chinfo)))
        # the deprecated partition channelinfo still unfolds the partitions
        @test (@test_deprecated LegendDataManagement.LDMUtils.get_partition_channelinfo(l200, geds, filekey.period, :cal; unfold_partitions = true)).partition == exploded.partition

        # detectors and channels as arguments, one or a vector
        @test channelinfo(l200, filekey.period, :cal, :V99000A).partition == [DataPartition(:calpartition001a)]
        @test channelinfo(l200, filekey.period, :cal, [:B99000A, :V99000A]).detector == DetectorId.([:B99000A, :V99000A])
        @test channelinfo(l200, filekey.period, :cal, [:V99000A]; explode_partitions = true).partition == [DataPartition(:calpartition001a)]
        @test channelinfo(l200, filekey.period, :cal, :V99000A; explode_partitions = true) isa TypedTables.Table
        @test channelinfo(l200, filekey.period, filekey.run, :cal, [:V99000A]).detector == [DetectorId(:V99000A)]
        @test channelinfo(l200, filekey, [:V99000A, :B99000A]).detector == DetectorId.([:V99000A, :B99000A])
        @test_throws "No channel information found for V09999A" channelinfo(l200, filekey.period, :cal, [:V99000A, :V09999A])

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
