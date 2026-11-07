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
        @test find_filekey(l200, Timestamp(ri.keys[1].time.unixtime + (ri.keys[2].time.unixtime - ri.keys[1].time.unixtime) ÷ 2)) == ri.keys[1]   # between two cycles -> the earlier one
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

    @testset "runinfo of a dataset" begin
        # `partial` lists cal of p03 and p16 r006, but phy only of p03 r000 to r002
        config = config_with_runlists("""
            valid:
              cal: {p02: r000..r007, p03: r000..r005}
              phy: {p02: r000..r007, p03: r000..r005}
            partial:
              cal: {p03: r000..r005, p16: r006}
              phy: {p03: r000..r002}
            """)
        withenv("LEGEND_DATA_CONFIG" => config) do
            partial = LegendData(:l200; dataset = :partial)
            rinfo = runinfo(partial)
            @test Set(zip(rinfo.period, rinfo.run)) == Set([[(DataPeriod(3), DataRun(r)) for r in 0:5]; (DataPeriod(16), DataRun(6))])
            # a listed category keeps its entry (`isequal`: a cal livetime is `NaN`)
            @test isequal(only(runinfo(partial, (:p03, :r002, :phy))), only(runinfo(l200, (:p03, :r002, :phy))))
            @test isequal(only(runinfo(partial, (:p03, :r003, :cal))), only(runinfo(l200, (:p03, :r003, :cal))))
            @test is_lrun(partial, (:p03, :r003, :cal)) && is_analysis_run(partial, (:p03, :r003, :cal))
            # only the categories that the run lists name are columns
            @test Set(propertynames(rinfo)) == Set([:period, :run, :keys, :cal, :phy])
            @test hasproperty(runinfo(l200), :fft) && hasproperty(runinfo(l200), :pzc)
            @test_throws "Dataset partial has no category fft" runinfo(partial, (:p16, :r006, :fft))
            @test_throws "Dataset partial has no category pzc" is_analysis_run(partial, (:p16, :r006, :pzc))
            @test_throws "Dataset partial has no category fft" start_filekey(partial, (:p16, :r006, :fft))
            # a category the dataset does not list for a run has no entry there
            for sel in ((:p03, :r003, :phy), (:p16, :r006, :phy))
                @test !ismissing(only(runinfo(l200, sel)).startkey)
                entry = only(runinfo(partial, sel))
                @test ismissing(entry.startkey) && ismissing(entry.endkey)
                @test isempty(entry.keys) && isnan(entry.livetime) && !entry.is_analysis_run
                @test !is_lrun(partial, sel) && !is_analysis_run(partial, sel)
                @test ismissing(start_filekey(partial, sel))
            end
            row = only(runinfo(partial, (:p03, :r003)))
            @test row.keys == row.cal.keys
            @test !any(k -> k.run == DataRun(3) && k.category == DataCategory(:phy), DataSet(partial, DataPeriod(3)))
            @test isempty(DataSet(partial, DataPeriod(16), DataRun(6)).keys ∩ only(runinfo(l200, (:p16, :r006, :fft))).keys.keys)
            # the cycle of a timestamp does not depend on the dataset
            phy_keys = only(runinfo(l200, (:p03, :r003, :phy))).keys
            @test find_filekey(partial, phy_keys[2].time) == phy_keys[2]
            @test find_filekey(partial, last(phy_keys).time) == last(phy_keys)
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

        # a dataset without the runs of a partition: the partition has an empty table of the
        # same type and contains no period
        meta = joinpath(mktempdir(), "metadata")
        cp(joinpath(testdata_dir, "metadata"), meta)
        chmod(meta, 0o755; recursive = true)
        open(io -> write(io, "p03only:\n  cal:\n    p03: r000..r005\n"),
            joinpath(meta, "datasets", "runlists.yaml"), "a")
        override = joinpath(dirname(meta), "override.yaml")
        write(override, "setups:\n  l200:\n    paths:\n      metadata: '$meta'\n")
        withenv("LEGEND_DATA_CONFIG" => "$override:$julia_config") do
            p03only = LegendData(:l200; dataset = :p03only)
            parts = partitioninfo(p03only, :V99000A, :cal)
            @test isempty(only(values(parts)))
            @test eltype(only(values(parts))) == eltype(partinfo[DataPartition(1)])
            @test isempty(partitioninfo(p03only, :V99000A, :cal, DataPeriod(3)))
            chinfo = channelinfo(p03only, DataPeriod(3), :cal; system = :geds)
            @test all(isempty, chinfo.partition)
            exploded = channelinfo(p03only, DataPeriod(3), :cal; system = :geds, explode_partitions = true)
            @test isempty(exploded) && exploded.partition isa Vector{DataPartition}
            @test all(ismissing, channelinfo(p03only, DataPeriod(3), DataRun(0), :cal; system = :geds).partition)
        end

        # a category without groupings: the partitions are all missing
        rm(joinpath(meta, "datasets", "cal_groupings.yaml"))
        withenv("LEGEND_DATA_CONFIG" => "$override:$julia_config") do
            nogroupings = LegendData(:l200)
            @test all(ismissing, channelinfo(nogroupings, (DataPeriod(2), DataRun(6), :cal)).partition)
            @test all(ismissing, channelinfo(nogroupings, DataPeriod(2), :cal).partition)
            @test isempty(channelinfo(nogroupings, DataPeriod(2), :cal; explode_partitions = true))
        end
    end

    @testset "get_dsp_pars" begin
        # the DSP config reads `ppars` by default and `rpars` for V99000B; only `ppars` exists
        meta = joinpath(mktempdir(), "metadata")
        cp(joinpath(testdata_dir, "metadata"), meta)
        chmod(meta, 0o755; recursive = true)
        dsp = mkpath(joinpath(meta, "jldataprod", "config", "dsp"))
        write(joinpath(dsp, "validity.yaml"), "- valid_from: 20220101T000000Z\n  apply:\n    - dsp.yaml\n")
        write(joinpath(dsp, "dsp.yaml"), "default:\n  pars_type: ppars\nV99000B:\n  pars_type: rpars\n")
        par = joinpath(dirname(meta), "par")
        pz = mkpath(joinpath(par, "ppars", "pz"))
        write(joinpath(pz, "validity.yaml"), "- valid_from: 20220101T000000Z\n  apply:\n    - pz.yaml\n")
        write(joinpath(pz, "pz.yaml"), "V99000A:\n  tau: 500.0\n")
        override = joinpath(dirname(meta), "override.yaml")
        write(override, "setups:\n  l200:\n    paths:\n      metadata: '$meta'\n      par: '$par'\n")
        withenv("LEGEND_DATA_CONFIG" => "$override:$julia_config") do
            data = LegendData(:l200)
            sel = start_filekey(data, (DataPeriod(2), DataRun(6), :cal))
            @test get_dsp_pars(data, sel, :V99000A, :pz).tau == 500.0
            @test isempty(get_dsp_pars(data, sel, :V99000C, :pz))
            @test_throws "No metadata pars/rpars/pz" get_dsp_pars(data, sel, :V99000B, :pz)
        end
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

@testset "parse_runs" begin
    @test LegendDataManagement.parse_runs("r002..r004") == DataRun.(2:4)
    @test LegendDataManagement.parse_runs(["r000", "r002..r003"]) == DataRun.([0, 2, 3])
    # YAML lists are read as Vector{Any}; an empty list gives no runs
    @test LegendDataManagement.parse_runs(Any["r001", "r003..r004"]) == DataRun.([1, 3, 4])
    @test LegendDataManagement.parse_runs(Any[]) == DataRun[]
    @test_throws ArgumentError LegendDataManagement.parse_runs(["r1"])
end
