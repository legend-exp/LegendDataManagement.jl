# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

include("testing_utils.jl")

@testset "data_config" begin
    dconfig = LegendDataConfig()
    setup = dconfig.setups.l200

    # ToDo: Add proper tests.
    @test @inferred(setup_data_path(setup, ["tier", "raw", "cal", "p02", "r006", "l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5"])) isa AbstractString
    @test @inferred(setup_data_path(setup, "tier/raw/cal/p02/r006/l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5")) isa AbstractString
end
