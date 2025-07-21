# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using TypedTables
using Unitful

@testset "dataprod_config" begin
    l200 = LegendData(:l200)

    @testset "runinfo" begin
        rinfo = runinfo(l200, (DataPeriod(2), DataRun(6), :cal))
        @test rinfo isa TypedTables.Table
        @test length(rinfo) == 1
        @test only(rinfo).startkey.period   == DataPeriod(2)
        @test only(rinfo).startkey.run      == DataRun(6)
        @test only(rinfo).startkey.category == DataCategory(:cal)
        @test_nowarn empty!(LegendDataManagement._cached_runinfo)
    end

    @testset "analysis_runs" begin
        analysisruns = analysis_runs(l200) 
        @test analysisruns isa TypedTables.Table
        @test hasproperty(analysisruns, :period)
        @test hasproperty(analysisruns, :run)
        @test_nowarn empty!(LegendDataManagement._cached_analysis_runs)
    end

    @testset "partitioninfo" begin
        partinfo = partitioninfo(l200, :V99000A, :cal)
        @test partinfo isa IdDict
        @test partinfo[DataPartition(1)] isa TypedTables.Table
        @test_nowarn empty!(LegendDataManagement._cached_partitioninfo)
    end

    @testset "utils" begin
        sel = (DataPeriod(2), DataRun(6), :phy)
        @test start_filekey(l200, sel) isa FileKey
        @test livetime(l200, sel) isa Unitful.Time

        rsel = (DataPeriod(2), DataRun(6))
        @test LegendDataManagement.is_analysis_cal_run(l200, rsel) 
        @test LegendDataManagement.is_analysis_phy_run(l200, rsel) 
        @test LegendDataManagement.is_analysis_run(l200, sel)
    end
end
