# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using Dates
using Unitful

@testset "filekey" begin
    setup = ExpSetup(:l200)
    @test setup.label == :l200
    @test @inferred(string(setup)) == "l200"
    @test @inferred(ExpSetup("l200")) == setup
    @test_throws ArgumentError DataPeriod("invalidsetup")

    period = DataPeriod(2)
    @test period.no == 2
    @test @inferred(string(period)) == "p02"
    @test @inferred(DataPeriod("p02")) == period
    @test_throws ArgumentError DataPeriod("invalidperiod")

    r = DataRun(6)
    @test r.no == 6
    @test @inferred(string(r)) == "r006"
    @test @inferred(DataRun("r006")) == r
    @test_throws ArgumentError DataRun("invalidrun")

    category = DataCategory(:cal)
    @test category.label == :cal
    @test @inferred(string(category)) == "cal"
    @test @inferred(DataCategory("cal")) == category
    @test_throws ArgumentError DataCategory("invalidstring")

    p = DataPartition("calgroup001a")
    @test p.no == 1
    @test p.set == :a
    @test p.cat == DataCategory(:cal)
    @test string(p) == "calpartition001a"
    @test p == DataPartition(001)
    @test p < DataPartition(:calgroup002)
    @test_throws ArgumentError DataPartition("invalidstring")

    timestamp = @inferred(Timestamp("20221226T200846Z"))
    @test timestamp.unixtime == 1672085326
    @test @inferred(string(timestamp)) == "20221226T200846Z"

    unix_timestamp = 1672085326u"s"
    timestamp2 = @inferred(Timestamp(unix_timestamp))
    @test timestamp2.unixtime == 1672085326
    @test @inferred(string(timestamp2)) == "20221226T200846Z"
    
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

    detector = DetectorId(:V99000A)
    @test detector.label == :V99000A
    @test @inferred(Symbol(detector)) == :V99000A
    @test @inferred(convert(Symbol, detector)) == :V99000A
    @test @inferred(string(detector)) == "V99000A"
    @test @inferred(DetectorId("V99000A")) == detector
end
