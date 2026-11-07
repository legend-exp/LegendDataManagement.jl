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

    @testset "Exposure of the runs of a dataset" begin
        # phy of p02 r004 to r007 is no analysis run, and `partial` does not list it
        config = config_with_runlists("""
            valid:
              cal: {p02: r000..r007, p03: r000..r005}
              phy: {p02: r000..r003, p03: r000..r005}
            partial:
              cal: {p02: r000..r007}
              phy: {p02: r000..r003}
            """)
        run_exposure(r) = get_exposure(l200, :V99000A, DataPeriod(2), DataRun(r))
        withenv("LEGEND_DATA_CONFIG" => config) do
            default = LegendData(:l200)
            partial = LegendData(:l200; dataset = :partial)
            @test get_exposure(default, :V99000A, DataPeriod(2)) ≈ sum(run_exposure, 0:3)
            @test get_exposure(default, :V99000A, DataPeriod(2); is_analysis_run = false) ≈ sum(run_exposure, 0:7)
            @test get_exposure(partial, :V99000A, DataPeriod(2)) ≈ sum(run_exposure, 0:3)
            @test get_exposure(partial, :V99000A, DataPeriod(2); is_analysis_run = false) ≈ sum(run_exposure, 0:3)
        end
    end
end
