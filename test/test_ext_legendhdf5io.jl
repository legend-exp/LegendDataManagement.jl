# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using LegendHDF5IO
using LegendTestData
using PropertyFunctions
using TypedTables

using HDF5

@testset "test_ext_legendhdf5io" begin

    lh5testdata_dir = joinpath(legend_test_data_path(), "data", "lh5", "prod-ref-l200")
    ENV["LEGEND_DATA_CONFIG"] = joinpath(lh5testdata_dir, "config.json")

    l200_lh5 = LegendData(:l200)

    # LegendTestData ships ChannelId-keyed tiers (ch1104000/...), which read_ldata no longer
    # accepts, so the read_ldata tests below build their own DetectorId-keyed tier tree.
    @testset "read_ldata" begin
        mktempdir() do tmpdir
            tierdir = joinpath(tmpdir, "generated", "tier")
            config = joinpath(tmpdir, "config.json")
            write(config, """{"setups": {"l200": {"paths": {"tier": "$(tierdir)"}}}}""")
            ENV["LEGEND_DATA_CONFIG"] = config

            l200 = LegendData(:l200)
            cat, period, run = DataCategory(:cal), DataPeriod(3), DataRun(0)
            tier, filter_tier = DataTier(:jldsp), DataTier(:jlhit)
            dets = DetectorId.(["V99000A", "B99000A"])
            det = first(dets)

            fks = FileKey.(["l200-p03-r000-cal-20230311T235840Z", "l200-p03-r000-cal-20230311T235952Z"])
            n = 20
            cols = Dict(fk => (
                    timestamp = collect(1.0:n) .+ 100 * i,
                    baseline = collect(n:-1.0:1),
                    daqenergy = collect(range(500.0, 2000.0, length = n)),
                ) for (i, fk) in enumerate(fks))

            for fk in fks
                path = l200.tier[tier, fk]
                mkpath(dirname(path))
                lh5open(path, "w") do f
                    for d in dets
                        f["$(d)/$(tier)"] = Table(cols[fk])
                    end
                end
                hit_path = l200.tier[filter_tier, fk]
                mkpath(dirname(hit_path))
                lh5open(hit_path, "w") do f
                    for d in dets
                        f["$(d)/$(filter_tier)"] = Table(is_valid_hit = isodd.(1:n))
                    end
                end
            end

            fk = first(fks)
            data_fk = lh5open(l200.tier[tier, fk]) do f
                f[Symbol(det), Symbol(tier)][:]
            end
            @test data_fk isa TypedTables.Table

            # single filekey
            @test read_ldata(l200, tier, fk, det) isa TypedTables.Table
            @test read_ldata(l200, tier, fk, det).timestamp == data_fk.timestamp
            @test read_ldata(:timestamp, l200, tier, fk, det).timestamp == data_fk.timestamp
            @test read_ldata((:timestamp, :baseline), l200, tier, fk, det).timestamp == data_fk.timestamp
            @test read_ldata((@pf (; bltime = $timestamp * $baseline, )), l200, tier, fk, det).bltime ==
                data_fk.timestamp .* data_fk.baseline

            # no detector given: one entry per detector in the file, each matching the
            # result of reading that detector on its own
            perdet = read_ldata(l200, tier, fk)
            @test perdet isa NamedTuple
            @test Set(keys(perdet)) == Set(Symbol.(dets))
            @test perdet[Symbol(det)].timestamp == data_fk.timestamp
            for d in dets
                @test perdet[Symbol(d)] == read_ldata(l200, tier, fk, d)
            end
            @test read_ldata((:timestamp,), l200, (tier, fk))[Symbol(det)] ==
                read_ldata((:timestamp,), l200, (tier, fk, det))
            @test read_ldata(l200, tier, fk; filterby = @pf $daqenergy > 1000)[Symbol(det)] ==
                read_ldata(l200, tier, fk, det; filterby = @pf $daqenergy > 1000)

            # whole run (both filekeys, flattened)
            all_ts = vcat((cols[k].timestamp for k in fks)...)
            @test read_ldata(l200, tier, cat, period, run, det) isa TypedTables.Table
            @test read_ldata(l200, tier, cat, period, run, det).timestamp == all_ts
            @test read_ldata(:timestamp, l200, tier, cat, period, run, det).timestamp == all_ts
            @test read_ldata((:timestamp, :baseline), l200, tier, cat, period, run, det).timestamp == all_ts

            # parallel read
            @test read_ldata(l200, tier, cat, period, run, det; parallel = true) isa TypedTables.Table
            @test read_ldata(l200, tier, cat, period, run, det; parallel = true).timestamp == all_ts
            @test read_ldata(:timestamp, l200, tier, cat, period, run, det; parallel = true).timestamp == all_ts

            @testset "n_evts subsampling" begin
                @test length(read_ldata(l200, tier, fk, det; n_evts = 5)) == 5

                # rows are drawn without replacement
                r = read_ldata(l200, tier, fk, det; n_evts = n)
                @test length(r) == n
                @test sort(r.timestamp) == sort(data_fk.timestamp)
                @test allunique(read_ldata(l200, tier, fk, det; n_evts = n - 1).timestamp)

                # every column of a subsampled read keeps the rows of the same events
                sub = read_ldata(l200, tier, fk, det; n_evts = 5)
                rows = [findfirst(isequal(t), data_fk.timestamp) for t in sub.timestamp]
                @test sub.baseline == data_fk.baseline[rows]
                @test sub.daqenergy == data_fk.daqenergy[rows]

                # subgroups of one file are subsampled together, not independently
                grouped = l200.tier[DataTier(:jlgrp), fk]
                mkpath(dirname(grouped))
                lh5open(grouped, "w") do f
                    f["$(det)/jlgrp/geds"] = Table(evtno = collect(1:n))
                    f["$(det)/jlgrp/spms"] = Table(evtno = collect(1:n))
                end
                g = read_ldata(l200, :jlgrp, fk, det; n_evts = 5)
                @test g.geds.evtno == g.spms.evtno

                # more events requested than the file holds returns everything
                @test length(read_ldata(l200, tier, fk, det; n_evts = 10 * n)) == n
            end

            # filterby on the tier being read
            cut = @pf $daqenergy > 1000
            keep = findall(data_fk.daqenergy .> 1000)
            @test read_ldata(l200, tier, fk, det; filterby = cut) isa TypedTables.Table
            @test all(read_ldata(l200, tier, fk, det; filterby = cut).daqenergy .> 1000)
            @test read_ldata(:timestamp, l200, tier, fk, det; filterby = cut).timestamp == data_fk.timestamp[keep]
            @test read_ldata((:timestamp, :baseline), l200, tier, fk, det; filterby = cut).timestamp ==
                data_fk.timestamp[keep]
            @test read_ldata((@pf (; bltime = $timestamp * $baseline, )), l200, tier, fk, det; filterby = cut).bltime ==
                data_fk.timestamp[keep] .* data_fk.baseline[keep]

            # n_evts draws from the rows passing the filter
            @test length(read_ldata(l200, tier, fk, det; filterby = cut, n_evts = length(keep))) == length(keep)
            let sub = read_ldata(l200, tier, fk, det; filterby = cut, n_evts = 3)
                @test length(sub) == 3
                @test all(sub.daqenergy .> 1000)
            end

            # multi-run read over a run table
            rinfo = Table([(period = period, run = run)])
            @test read_ldata(l200, tier, cat, rinfo, det).timestamp == all_ts

            # a file holding no detector and no tier group is an error, not an empty result
            empty_fk = FileKey("l200-p03-r000-cal-20230311T235959Z")
            empty_path = l200.tier[tier, empty_fk]
            lh5open(empty_path, "w") do f
                f["not_a_detector"] = Table(a = collect(1.0:5))
            end
            @test_throws "No `DetectorId` or `DataTier` key found" read_ldata(l200, tier, empty_fk)
            rm(empty_path)

            # selector combinations without a read_ldata method are reported, not recursed on
            @test_throws "does not support the selector combination" read_ldata(l200, (tier,))
            @test_throws "does not support the selector combination" read_ldata(l200, (tier, cat))
            @test_throws "does not support the selector combination" read_ldata(l200, (:jldsp, :cal))

            @testset "cross-tier filterby" begin
                valid = isodd.(1:n)
                hit_cut = @pf $is_valid_hit

                r = read_ldata(l200, tier, fk, det; filterby = filter_tier => hit_cut)
                @test r isa TypedTables.Table
                @test r.timestamp == data_fk.timestamp[valid]
                @test r.baseline == data_fk.baseline[valid]

                # column selection composes with the cross-tier filter
                @test read_ldata((:timestamp,), l200, (tier, fk, det); filterby = filter_tier => hit_cut).timestamp ==
                    data_fk.timestamp[valid]

                # naming the tier being read is the same as a plain filterby
                @test read_ldata(l200, tier, fk, det; filterby = tier => cut).timestamp ==
                    read_ldata(l200, tier, fk, det; filterby = cut).timestamp

                # n_evts subsamples the surviving rows
                @test length(read_ldata(l200, tier, fk, det; filterby = filter_tier => hit_cut, n_evts = 3)) == 3

                # whole-run cross-tier read
                @test read_ldata(l200, tier, cat, period, run, det; filterby = filter_tier => hit_cut).timestamp ==
                    vcat((cols[k].timestamp[valid] for k in fks)...)

                # a detector is required to line the two tiers up
                @test_throws "requires a DetectorId" read_ldata(l200, tier, fk, ""; filterby = filter_tier => hit_cut)

                # the predicate has to name its source columns
                @test_throws "PropertyFunction" read_ldata(l200, tier, fk, det; filterby = filter_tier => (row -> true))

                # rows correspond by position, so the row counts must agree
                short = l200.tier[filter_tier, last(fks)]
                lh5open(short, "w") do f
                    for d in dets
                        f["$(d)/$(filter_tier)"] = Table(is_valid_hit = isodd.(1:(n - 5)))
                    end
                end
                @test_throws DimensionMismatch read_ldata(l200, tier, last(fks), det; filterby = filter_tier => hit_cut)
            end
        end
    end
    
    @testset "DataSelector LH5 I/O" begin
        # Every DataSelector registered by the extension must survive a write/read round trip,
        # as a scalar and as an array.
        selectors = Any[
            ExpSetup(:l200), DataTier(:jldsp), DataCategory(:cal), DataPeriod(3), DataRun(0),
            DataPartition(1), Timestamp("20230311T235840Z"),
            FileKey("l200-p03-r000-cal-20230311T235840Z"),
            ChannelId(1104000), DetectorId("V99000A"),
        ]

        mktempdir() do tmpdir
            filename = joinpath(tmpdir, "selectors.lh5")
            lh5open(filename, "w") do f
                for (i, sel) in enumerate(selectors)
                    f["scalar_$(i)"] = sel
                    f["array_$(i)"] = [sel, sel]
                end
            end

            lh5open(filename, "r") do f
                for (i, sel) in enumerate(selectors)
                    @testset "$(typeof(sel))" begin
                        @test f["scalar_$(i)"] isa typeof(sel)
                        @test f["scalar_$(i)"] == sel
                        arr = f["array_$(i)"][:]
                        @test eltype(arr) <: typeof(sel)
                        @test arr == [sel, sel]
                    end
                end
            end
        end
    end

    @testset "DetectorId encoding" begin
        det = DetectorId("V99999J")
        dets = DetectorId.(["V99999J", "B59231A", "PULS99ANA"])

        mktempdir() do tmpdir
            # DetectorIds are stored as UInt32 rather than as strings
            filename = joinpath(tmpdir, "detid.lh5")
            lh5open(filename, "w") do f
                f["single_det"] = det
                f["det_array"] = dets
            end
            HDF5.h5open(filename, "r") do h5f
                @test read(h5f["single_det"]) === UInt32(det)
                @test read(h5f["det_array"]) == UInt32.(dets)
            end

            # Strings are still read back, for files written before the UInt32 encoding
            str_filename = joinpath(tmpdir, "detid_str.lh5")
            HDF5.h5open(str_filename, "w") do h5f
                h5f["det_string"] = string(det)
                HDF5.attributes(h5f["det_string"])["datatype"] = "detectorid"
                h5f["det_array_string"] = string.(dets)
                HDF5.attributes(h5f["det_array_string"])["datatype"] = "array<1>{detectorid}"
            end
            lh5open(str_filename, "r") do f
                @test f["det_string"] === det
                @test f["det_array_string"][:] == dets
            end
        end
    end
end
