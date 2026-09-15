# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using Test
using LegendDataManagement
using Unitful

include("testing_utils.jl")

l200 = LegendData(:l200)

# An exposure is summed over the runs of `runinfo`, which reads the DAQ cycle keys of a run from
# the metadata `datasets/filekeys`. LegendTestData does not hold them.
# TODO: drop the guard once the test data holds them.
has_filekeys = haskey(l200.metadata.datasets, :filekeys)

@testset "Exposure" begin
    for det in (:V99000A, :B99000A)
        @testset "$(det)" begin
            @testset "Period exposure" begin
                period = DataPeriod(2)
                if !has_filekeys
                    @test_broken get_exposure(l200, det, period) isa Quantity
                else
                    rinfo = runinfo(l200, period)
                    period_exposure = get_exposure(l200, det, period)
                    @test period_exposure isa Quantity
                    @test dimension(period_exposure) == dimension(u"kg*yr")
                    @test period_exposure ≈ sum(map(r -> get_exposure(l200, det, period, r), rinfo.run))
                end
            end
            @testset "Partition exposure" begin
                part = DataPartition(:phygroup001a)
                if !has_filekeys
                    @test_broken get_exposure(l200, det, part) isa Quantity
                else
                    part_exposure = get_exposure(l200, det, part)
                    partinfo = partitioninfo(l200, det, part)
                    @test part_exposure isa Quantity
                    @test dimension(part_exposure) == dimension(u"kg*yr")
                    @test part_exposure ≈ sum(map(p -> get_exposure(l200, det, p.period, p.run), partinfo))
                end
            end
        end
    end
end
