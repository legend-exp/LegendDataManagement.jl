# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using Test
using LegendDataManagement
using Unitful

include("testing_utils.jl")

l200 = LegendData(:l200)
@testset "Exposure" begin
    for det in (:V99000A, :B99000A)
        @testset "$(det)" begin
            @testset "Period exposure" begin
                period = DataPeriod(2)
                rinfo = runinfo(l200, period)
                period_exposure = get_exposure(l200, det, period)
                @test period_exposure isa Quantity
                @test dimension(period_exposure) == dimension(u"kg*yr")
                @test period_exposure ≈ sum(map(r -> get_exposure(l200, det, period, r), rinfo.run))
            end
            @testset "Partition exposure" begin
                part = DataPartition(:phygroup001a)
                part_exposure = get_exposure(l200, det, part)
                partinfo = partitioninfo(l200, det, part)
                @test part_exposure isa Quantity
                @test dimension(part_exposure) == dimension(u"kg*yr")
                @test part_exposure ≈ sum(map(p -> get_exposure(l200, det, p.period, p.run), partinfo))  
            end
        end
    end
end