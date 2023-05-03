# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using Dates

@testset "filekey" begin
    key = @inferred FileKey("l200-p02-r006-cal-20221226T200846Z")
    @test string(key) == "l200-p02-r006-cal-20221226T200846Z"

    # @test occursin(key, "%-r006-%-phy")
    # @test !occursin(key, "%-r006-%-phy")

    @test FileKey("l200-p02-r006-cal-20221226T200846Z") == key
    @test FileKey("tier/raw/cal/p02/r006/l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5") == key

    @test @inferred(LegendDataManagement.is_filekey_string("l200-p02-r006-cal-20221226T200846Z")) == true
    @test @inferred(LegendDataManagement.is_filekey_string("20221226T200846Z")) == false

    @test @inferred(LegendDataManagement.is_timestamp_string("20221226T200846Z")) == true
    @test @inferred(LegendDataManagement.is_timestamp_string("20221226200846")) == false

    @test @inferred(LegendDataManagement.timestamp_from_string("l200-p02-r006-cal-20221226T200846Z")) == DateTime("2022-12-26T20:08:46")
    @test @inferred(LegendDataManagement.timestamp_from_string("20221226T200846Z")) == DateTime("2022-12-26T20:08:46")
    @test_throws ArgumentError LegendDataManagement.timestamp_from_string("20221226200846Z")
end
