# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using TypedTables
using Unitful

@testset "dataprod_config" begin
    l200 = LegendData(:l200)

    # `runinfo` reads the DAQ cycle keys of a run from the metadata `datasets/filekeys`, which
    # LegendTestData does not hold. TODO: drop the guard once the test data holds them.
    has_filekeys = haskey(l200.metadata.datasets, :filekeys)

    @testset "runinfo" begin
        if !has_filekeys
            @test_broken runinfo(l200) isa TypedTables.Table
        else
            rinfo = runinfo(l200, (DataPeriod(2), DataRun(6), :cal))
            @test rinfo isa TypedTables.Table
            @test length(rinfo) == 1
            @test only(rinfo).startkey.period   == DataPeriod(2)
            @test only(rinfo).startkey.run      == DataRun(6)
            @test only(rinfo).startkey.category == DataCategory(:cal)
            @test only(rinfo).keys isa DataSet && !isempty(only(rinfo).keys)
            @test only(rinfo).keys._name == l200.dataset
            ri = only(runinfo(l200, (DataPeriod(2), DataRun(6))))
            @test ri.keys == sort(vcat(ri.cal.keys, ri.phy.keys), by = Timestamp)
            @test all(issorted(row.keys, by = Timestamp) for row in runinfo(l200))
            @test first(ri.keys).time in ri.keys && last(ri.keys).time in ri.keys
            @test !(Timestamp(first(ri.keys).time.unixtime - 1) in ri.keys)
            @test DataSet(l200)._name == l200.dataset
            @test DataSet(l200, DataPeriod(2), DataRun(6)) == ri.keys
            @test DataSet(l200, DataPeriod(2)) == sort(reduce(vcat, runinfo(l200, DataPeriod(2)).keys), by = Timestamp)
            @test find_filekey(ri.keys, first(ri.keys).time) == first(ri.keys)
            @test find_filekey(l200, last(ri.keys).time) == last(ri.keys)
            @test find_filekey(l200, Timestamp((ri.keys[1].time.unixtime + ri.keys[2].time.unixtime) ÷ 2)) == ri.keys[1]   # between two cycles -> the earlier one
        end
        @test_nowarn empty!(LegendDataManagement._cached_runinfo)
    end

    @testset "analysis_runs" begin
        analysisruns = analysis_runs(l200, :cal)
        @test analysisruns isa TypedTables.Table
        @test hasproperty(analysisruns, :period)
        @test hasproperty(analysisruns, :run)
        @test_nowarn empty!(LegendDataManagement._cached_analysis_runs)
    end

    @testset "partitioninfo" begin
        if !has_filekeys
            @test_broken partitioninfo(l200, :V99000A, :cal) isa IdDict
        else
            partinfo = partitioninfo(l200, :V99000A, :cal)
            @test partinfo isa IdDict
            @test partinfo[DataPartition(1)] isa TypedTables.Table
        end
        @test_nowarn empty!(LegendDataManagement._cached_partitioninfo)
    end

    @testset "utils" begin
        sel = (DataPeriod(2), DataRun(6), :phy)
        rsel = (DataPeriod(2), DataRun(6))
        if !has_filekeys
            @test_broken start_filekey(l200, sel) isa FileKey
        else
            @test start_filekey(l200, sel) isa FileKey
            @test livetime(l200, sel) isa Unitful.Time
            @test LegendDataManagement.is_analysis_cal_run(l200, rsel)
            @test LegendDataManagement.is_analysis_phy_run(l200, rsel)
            @test LegendDataManagement.is_analysis_run(l200, sel)
        end
    end
end
