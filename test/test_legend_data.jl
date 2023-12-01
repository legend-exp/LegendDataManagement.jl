# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using StructArrays, PropertyFunctions

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
    @test l200.metadata == LegendDataManagement.AnyProps(props_base_path)

    # ToDo: Make type-stable:
    @test (channelinfo(l200, filekey)) isa StructArray
    chinfo = channelinfo(l200, filekey)
    @test all(filterby(@pf $processable && $usability == :on)(chinfo).processable)
    @test all(filterby(@pf $processable && $usability == :on)(chinfo).usability .== :on)

    # ToDo: Make type-stable:
    @test #=@inferred=#(channel_info(l200, filekey)) isa StructArray
    chinfo = channel_info(l200, filekey)
    @test all(filterby(@pf $processable && $usability == :on)(chinfo).processable)
    @test all(filterby(@pf $processable && $usability == :on)(chinfo).usability .== :on)
end
