# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using TypedTables
using Unitful
import YAML

include("testing_utils.jl")

@testset "dataprod_config" begin
    l200 = LegendData(:l200)

    @testset "runinfo" begin
        rinfo = runinfo(l200, (DataPeriod(2), DataRun(6), :cal))
        @test rinfo isa TypedTables.Table
        @test length(rinfo) == 1
        @test only(rinfo).startkey.period   == DataPeriod(2)
        @test only(rinfo).startkey.run      == DataRun(6)
        @test only(rinfo).startkey.category == DataCategory(:cal)
        @test only(rinfo).keys isa DataSet && !isempty(only(rinfo).keys)
        @test only(rinfo).keys._name == l200.dataset
        @test only(rinfo).startkey == first(only(rinfo).keys)
        @test only(rinfo).endkey == last(only(rinfo).keys)
        ri = only(runinfo(l200, (DataPeriod(2), DataRun(6))))
        @test ri.keys == sort(vcat(ri.cal.keys, ri.phy.keys), by = Timestamp)
        @test all(issorted(row.keys, by = Timestamp) for row in runinfo(l200))
        @test all(row.cal.startkey == first(row.cal.keys) && row.cal.endkey == last(row.cal.keys) for row in runinfo(l200, DataPeriod(3)))
        # a category with a start key but no end key in datasets/runinfo
        @test ismissing(only(runinfo(l200, (:p13, :r001, :ant))).endkey)
        @test !isempty(only(runinfo(l200, (:p13, :r001, :ant))).keys)
        @test first(ri.keys).time in ri.keys && last(ri.keys).time in ri.keys
        @test !(Timestamp(first(ri.keys).time.unixtime - 1) in ri.keys)
        @test DataSet(l200)._name == l200.dataset
        @test DataSet(l200, DataPeriod(2), DataRun(6)) == ri.keys
        @test DataSet(l200, DataPeriod(2)) == sort(reduce(vcat, runinfo(l200, DataPeriod(2)).keys), by = Timestamp)
        @test find_filekey(ri.keys, first(ri.keys).time) == first(ri.keys)
        @test find_filekey(l200, last(ri.keys).time) == last(ri.keys)
        @test find_filekey(l200, Timestamp((ri.keys[1].time.unixtime + ri.keys[2].time.unixtime) ÷ 2)) == ri.keys[1]   # between two cycles -> the earlier one
        @test_throws "before the first DAQ cycle" find_filekey(ri.keys, Timestamp(first(ri.keys).time.unixtime - 1))
        @test_throws "before the first DAQ cycle" find_filekey(DataSet(FileKey[]), first(ri.keys).time)
        @test_nowarn empty!(LegendDataManagement._cached_runinfo)

        # The cycle key lists of the metadata are checked against datasets/runinfo.
        mktempdir() do tmpdir
            metadata = joinpath(tmpdir, "metadata")
            cp(joinpath(testdata_dir, "metadata"), metadata)
            chmod(metadata, 0o755; recursive = true)   # the artifact's files are read-only
            config = joinpath(tmpdir, "config.yaml")
            open(config, "w") do f
                YAML.write(f, Dict("setups" => Dict("l200" => Dict("paths" => Dict("metadata" => metadata)))))
            end
            fkfile = joinpath(metadata, "datasets", "filekeys", "p02", "r006.yaml")
            fk = YAML.load_file(fkfile)
            withenv("LEGEND_DATA_CONFIG" => config) do
                # a category whose cycle list is empty
                YAML.write_file(fkfile, merge(fk, Dict("cal" => [])))
                @test_throws "No DAQ cycle keys listed for period p02 run r006 category cal" runinfo(LegendData(:l200))
                # a run without a cycle list
                rm(fkfile)
                @test_throws r"No DAQ cycle keys listed for period p02 run r006 category (cal|phy)" runinfo(LegendData(:l200))
                # a cycle list that does not start or end at the keys of datasets/runinfo
                YAML.write_file(fkfile, Dict("cal" => fk["cal"][2:end-1], "phy" => fk["phy"]))
                @test_warn r"Start key .* is not the first DAQ cycle key" runinfo(LegendData(:l200))
                empty!(LegendDataManagement._cached_runinfo)
                @test_warn r"End key .* is not the last DAQ cycle key" runinfo(LegendData(:l200))
                empty!(LegendDataManagement._cached_runinfo)
                # no cycle lists at all
                rm(joinpath(metadata, "datasets", "filekeys"); recursive = true)
                @test_throws "No DAQ cycle key lists found in metadata datasets/filekeys" runinfo(LegendData(:l200))
            end
        end
    end

    @testset "analysis_runs" begin
        analysisruns = analysis_runs(l200, :cal)
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
        rsel = (DataPeriod(2), DataRun(6))
        @test start_filekey(l200, sel) isa FileKey
        @test livetime(l200, sel) isa Unitful.Time
        @test LegendDataManagement.is_analysis_cal_run(l200, rsel)
        @test LegendDataManagement.is_analysis_phy_run(l200, rsel)
        @test LegendDataManagement.is_analysis_run(l200, sel)
    end
end
