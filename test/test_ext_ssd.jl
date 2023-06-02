# This file is a part of jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using SolidStateDetectors:SolidStateDetector

include("testing_utils.jl")

@testset "test_ext_ssd" begin
    l200 = LegendData(:l200)
    @test SolidStateDetector(l200, :V99000A) isa SolidStateDetector
end
