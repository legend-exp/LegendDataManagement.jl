# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using Dates

@testset "filekey" begin
    setup = ExpSetup(:l200)
    @test setup.label == :l200
    @test @inferred(string(setup)) == "l200"
    @test @inferred(ExpSetup("l200")) == setup

    period = DataPeriod(2)
    @test period.no == 2
    @test @inferred(string(period)) == "p02"
    @test @inferred(DataPeriod("p02")) == period

    r = DataRun(6)
    @test r.no == 6
    @test @inferred(string(r)) == "r006"
    @test @inferred(DataRun("r006")) == r

    category = DataCategory(:cal)
    @test category.label == :cal
    @test @inferred(string(category)) == "cal"
    @test @inferred(DataCategory("cal")) == category

    timestamp = @inferred(Timestamp("20221226T200846Z"))
    @test timestamp.unixtime == 1672085326
    @test @inferred(string(timestamp)) == "20221226T200846Z"
    
    key = @inferred FileKey("l200-p02-r006-cal-20221226T200846Z")
    @test string(key) == "l200-p02-r006-cal-20221226T200846Z"

    # @test occursin(key, "%-r006-%-phy")
    # @test !occursin(key, "%-r006-%-phy")

    @test FileKey("l200-p02-r006-cal-20221226T200846Z") == key
    @test @inferred(FileKey("tier/raw/cal/p02/r006/l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5")) == key
    @test @inferred(FileKey("l200", "p02", "r006", "cal", "20221226T200846Z")) == key

    @test @inferred(LegendDataManagement._is_filekey_string("l200-p02-r006-cal-20221226T200846Z")) == true
    @test @inferred(LegendDataManagement._is_filekey_string("20221226T200846Z")) == false

    @test @inferred(LegendDataManagement._is_timestamp_string("20221226T200846Z")) == true
    @test @inferred(LegendDataManagement._is_timestamp_string("20221226200846")) == false

    @test @inferred(LegendDataManagement._timestamp_from_string("l200-p02-r006-cal-20221226T200846Z")) == DateTime("2022-12-26T20:08:46")
    @test @inferred(LegendDataManagement._timestamp_from_string("20221226T200846Z")) == DateTime("2022-12-26T20:08:46")
    @test_throws ArgumentError LegendDataManagement._timestamp_from_string("20221226200846Z")

    ch = ChannelId(1083204)
    @test ch.no == 1083204
    @test @inferred(string(ch)) == "ch1083204"
    @test @inferred(ChannelId("ch1083204")) == ch
    ch = ChannelId(98)
    @test ch.no == 98
    @test @inferred(string(ch)) == "ch098"
    @test @inferred(ChannelId("ch098")) == ch

    detector = DetectorId(:V99000A)
    @test detector.label == :V99000A
    @test @inferred(string(detector)) == "V99000A"
    @test @inferred(DetectorId("V99000A")) == detector
end
