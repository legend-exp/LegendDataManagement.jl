# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using LegendHDF5IO
using LegendTestData
using PropertyFunctions
using TypedTables

using YAML
using HDF5

@testset "test_ext_legendhdf5io" begin

    lh5testdata_dir = joinpath(legend_test_data_path(), "data", "lh5", "prod-ref-l200")
    ENV["LEGEND_DATA_CONFIG"] = joinpath(lh5testdata_dir, "config.json")

    l200_lh5 = LegendData(:l200)

    @testset "read_ldata" begin
        l200 = l200_lh5
        cat, period, run = DataCategory(:cal), DataPeriod(18), DataRun(0)
        tier, filter_tier = DataTier(:jldsp), DataTier(:raw)
        fks = search_disk(FileKey, l200.tier[tier, cat, period, run])
        fk = first(fks)
        # PULS93 is recorded in the raw tier but not processed into jldsp
        dets = DetectorId.(["B93099C", "V97061A"])
        det = first(dets)

        # the tables the reads are checked against, straight from the files
        tbl(t, k, d) = lh5open(f -> f["$(t)/$(d)"][:], l200.tier[DataTier(t), k])
        data_fk = tbl(tier, fk, det)
        @test TypedTables.Tables.istable(data_fk)
        n = length(data_fk)
        all_ts = vcat((tbl(tier, k, det).timestamp for k in fks)...)

        # single filekey
        @test read_ldata(l200, tier, fk, det) isa TypedTables.Table
        @test read_ldata(l200, tier, fk, det).timestamp == data_fk.timestamp
        @test read_ldata(:timestamp, l200, tier, fk, det).timestamp == data_fk.timestamp
        @test read_ldata((:timestamp, :e_fc), l200, tier, fk, det).timestamp == data_fk.timestamp
        @test read_ldata((@pf (; twice = $e_fc * 2, )), l200, tier, fk, det).twice == data_fk.e_fc .* 2

        # no detector given: one entry per detector in the file, each matching the
        # result of reading that detector on its own
        perdet = read_ldata(l200, tier, fk)
        @test perdet isa NamedTuple
        @test Set(keys(perdet)) == Set(Symbol.(dets))
        @test perdet[Symbol(det)].timestamp == data_fk.timestamp
        for d in dets
            @test perdet[Symbol(d)] == read_ldata(l200, tier, fk, d)
        end
        # the raw tier of the same run holds a channel that jldsp does not
        @test Set(keys(read_ldata(l200, filter_tier, fk))) == Set(Symbol.([dets..., DetectorId("PULS93")]))
        # a column selection acts on each detector's table
        @test read_ldata((:timestamp,), l200, (tier, fk))[Symbol(det)] ==
            read_ldata((:timestamp,), l200, (tier, fk, det))

        # several detectors at once, keyed by detector and in the order given
        @test keys(read_ldata(l200, tier, fk, dets)) == Symbol.(Tuple(dets))
        @test keys(read_ldata(l200, tier, fk, reverse(dets))) == Symbol.(Tuple(reverse(dets)))
        @test keys(read_ldata(:timestamp, l200, tier, fk, string.(dets))) == Symbol.(Tuple(dets))
        @test read_ldata(:timestamp, l200, tier, fk, dets)[Symbol(det)].timestamp ==
            read_ldata(:timestamp, l200, tier, fk, det).timestamp
        @test keys(read_ldata(l200, tier, fk, [det, DetectorId("PULS93")]; ignore_missing = true)) ==
            (Symbol(det),)
        @test_throws "not found" read_ldata(l200, tier, fk, [det, DetectorId("PULS93")])

        # a detector the tier does not hold, and a column a table does not have
        @test_throws "not found" read_ldata(l200, tier, fk, DetectorId("PULS93"))
        @test isnothing(read_ldata(l200, tier, fk, DetectorId("PULS93"); ignore_missing = true))
        @test_throws "not found" read_ldata(:not_a_column, l200, tier, fk, det)
        @test isnothing(read_ldata(:not_a_column, l200, tier, fk, det; ignore_missing = true))

        # whole run (both filekeys, flattened)
        @test read_ldata(l200, tier, cat, period, run, det) isa TypedTables.Table
        @test read_ldata(l200, tier, cat, period, run, det).timestamp == all_ts
        @test read_ldata(:timestamp, l200, tier, cat, period, run, det).timestamp == all_ts
        @test read_ldata((:timestamp, :e_fc), l200, tier, cat, period, run, det).timestamp == all_ts

        # whole run without a detector: one entry per detector, flattened over the files
        nodet_run = read_ldata(:timestamp, l200, tier, cat, period, run)
        @test Set(keys(nodet_run)) == Set(Symbol.(dets))
        @test nodet_run[Symbol(det)].timestamp == all_ts
        @test read_ldata(:timestamp, l200, tier, cat, period, run, dets)[Symbol(det)].timestamp == all_ts

        # parallel read
        @test read_ldata(l200, tier, cat, period, run, det; parallel = true) isa TypedTables.Table
        @test read_ldata(l200, tier, cat, period, run, det; parallel = true).timestamp == all_ts

        # multi-run read over a run table
        rinfo = Table([(period = period, run = run)])
        @test read_ldata(l200, tier, cat, rinfo, det).timestamp == all_ts

        # a per-detector file of a tier that holds a level below the detector
        pls_det = DetectorId("PULS93")
        pls = read_ldata(l200, DataTier(:jlpls), fk, pls_det)
        @test pls isa NamedTuple && keys(pls) == (:tags,)
        # a selection naming that level picks it; a path reads into it
        @test read_ldata(:tags, l200, DataTier(:jlpls), fk, pls_det).timestamp == pls.tags.timestamp
        @test read_ldata((@pf $tags.timestamp), l200, DataTier(:jlpls), fk, pls_det) == pls.tags.timestamp

        # a per-detector file whose level holds tables of differing length
        hit = read_ldata(l200, DataTier(:jlhit), fk, det)
        @test hit isa NamedTuple
        @test Set(keys(hit)) == Set((:dataPulser, :dataQC, :pulserTag, :qc))
        @test hit.pulserTag isa AbstractVector{Bool}
        @test read_ldata(:qc, l200, DataTier(:jlhit), fk, det) == hit.qc
        @test keys(read_ldata((:qc, :pulserTag), l200, DataTier(:jlhit), fk, det)) == (:qc, :pulserTag)
        @test read_ldata((@pf $qc.is_pileup), l200, DataTier(:jlhit), fk, det) == hit.qc.is_pileup
        @test_throws "not found under" read_ldata((:qc, :nope), l200, DataTier(:jlhit), fk, det)

        # a property path continues into the type a column stores, past the groups of the file
        wf = lh5open(f -> f["raw/$(det)/waveform_presummed"][:], l200.tier[filter_tier, fk])
        @test read_ldata((@pf $waveform_presummed.signal), l200, filter_tier, fk, det) == wf.signal
        @test read_ldata((@pf $waveform_presummed.values), l200, filter_tier, fk, det) == wf.signal

        # an event tier is one table for the whole file, and a path reads its leaves
        pfk = first(search_disk(FileKey, l200.tier[DataTier(:jlevt), :phy, DataPeriod(18), DataRun(3)]))
        evt = read_ldata(l200, DataTier(:jlevt), pfk)
        @test evt isa TypedTables.Table
        @test :geds in propertynames(evt) && :aux in propertynames(evt)
        @test read_ldata((@pf $aux.pulser.aux_trig), l200, DataTier(:jlevt), pfk) == evt.aux.pulser.aux_trig
        @test read_ldata((:geds,), l200, DataTier(:jlevt), pfk).geds.timestamp == evt.geds.timestamp

        # a file that is not on disk names what was looked for, whatever `ignore_missing` says
        missing_fk = FileKey("l200-p18-r000-cal-20251107T000000Z")
        @test_throws "not found" read_ldata(l200, tier, missing_fk)
        @test_throws "found" read_ldata(l200, tier, missing_fk, det)
        @test_throws "found" read_ldata(l200, tier, missing_fk, det; ignore_missing = true)

        # selector combinations without a read_ldata method are reported, not recursed on
        @test_throws "does not support the selector combination" read_ldata(l200, (tier,))
        @test_throws "does not support the selector combination" read_ldata(l200, (tier, cat))
        @test_throws "does not support the selector combination" read_ldata(l200, (:jldsp, :cal))

        # a selection given loosely is converted to the types the positions call for
        @test read_ldata(:timestamp, l200, Symbol(tier), fk, string(det)).timestamp == data_fk.timestamp
        @test read_ldata(:timestamp, l200, string(tier), fk, Symbol(det)).timestamp == data_fk.timestamp
        @test read_ldata(:timestamp, l200, Symbol(tier), Symbol(cat), Symbol(period), Symbol(run), det).timestamp == all_ts
        @test keys(read_ldata(l200, Symbol(tier), fk, "")) == keys(read_ldata(l200, tier, fk))
        @test keys(read_ldata(l200, tier, fk, nothing)) == keys(read_ldata(l200, tier, fk))
        @test_throws "Ambiguous selector types" read_ldata(l200, (tier, cat, :raw, det))
        @test_throws "Runtable doesn't provide" read_ldata(l200, tier, cat, Table(a = [1]), det)

        # filterby on the tier being read
        ecut = sort(data_fk.e_fc)[end ÷ 2]
        cut = @pf $e_fc > ecut
        keep = findall(data_fk.e_fc .> ecut)
        @test read_ldata(l200, tier, fk, det; filterby = cut) isa TypedTables.Table
        @test all(read_ldata(l200, tier, fk, det; filterby = cut).e_fc .> ecut)
        @test read_ldata(:timestamp, l200, tier, fk, det; filterby = cut).timestamp == data_fk.timestamp[keep]
        @test read_ldata((@pf (; twice = $e_fc * 2, )), l200, tier, fk, det; filterby = cut).twice ==
            data_fk.e_fc[keep] .* 2

        @testset "cross-tier filterby" begin
            # the raw tier holds one row per trigger of the same detector, as jldsp does
            raw_fk = tbl(filter_tier, fk, det)
            dcut = sort(raw_fk.daqenergy)[end ÷ 2]
            hit_cut = @pf $daqenergy > dcut
            valid = raw_fk.daqenergy .> dcut

            r = read_ldata(l200, tier, fk, det; filterby = filter_tier => hit_cut)
            @test r isa TypedTables.Table
            @test r.timestamp == data_fk.timestamp[valid]
            @test r.e_fc == data_fk.e_fc[valid]

            # column selection composes with the cross-tier filter
            @test read_ldata((:timestamp,), l200, (tier, fk, det); filterby = filter_tier => hit_cut).timestamp ==
                data_fk.timestamp[valid]

            # the tier of a pair is converted like any other selector
            @test read_ldata(l200, tier, fk, det; filterby = Symbol(filter_tier) => hit_cut).timestamp ==
                r.timestamp

            # naming the tier being read is the same as a plain filterby
            @test read_ldata(l200, tier, fk, det; filterby = tier => cut).timestamp ==
                read_ldata(l200, tier, fk, det; filterby = cut).timestamp

            # whole-run cross-tier read
            @test read_ldata(l200, tier, cat, period, run, det; filterby = filter_tier => hit_cut).timestamp ==
                vcat((tbl(tier, k, det).timestamp[tbl(filter_tier, k, det).daqenergy .> dcut] for k in fks)...)

            # without a detector every detector is filtered by its own rows
            nodet = read_ldata(l200, tier, fk; filterby = filter_tier => hit_cut)
            @test nodet isa NamedTuple
            for d in dets
                @test nodet[Symbol(d)] == read_ldata(l200, tier, fk, d; filterby = filter_tier => hit_cut)
            end

            # the signature takes only PropertyFunctions, which name their source columns
            @test_throws TypeError read_ldata(l200, tier, fk, det; filterby = filter_tier => (row -> true))
            @test_throws TypeError read_ldata(l200, tier, fk, det; filterby = row -> true)

            @testset "several filter tiers" begin
                # rows have to pass every pair
                both = valid .& (data_fk.e_fc .> ecut)
                r2 = read_ldata(l200, tier, fk, det; filterby = (filter_tier => hit_cut, tier => cut))
                @test r2.timestamp == data_fk.timestamp[both]
                # the pairs may be given in any order
                @test read_ldata(l200, tier, fk, det; filterby = (tier => cut, filter_tier => hit_cut)).timestamp ==
                    r2.timestamp
                # a one-element tuple is the single-pair form
                @test read_ldata(l200, tier, fk, det; filterby = (filter_tier => hit_cut,)).timestamp ==
                    data_fk.timestamp[valid]

                @test_throws "must name at least one" read_ldata(l200, tier, fk, det; filterby = ())
                @test_throws TypeError read_ldata(l200, tier, fk, det;
                    filterby = (filter_tier => hit_cut, hit_cut))
                # a chained pair is not a predicate
                @test_throws TypeError read_ldata(l200, tier, fk, det; filterby = filter_tier => (tier => cut))
            end

            # rows correspond by position: an event tier has one row per event, not per trigger
            @test_throws DimensionMismatch read_ldata(l200, tier, pfk, DetectorId("V97061A");
                filterby = DataTier(:jlevt) => @pf $aux.pulser.aux_trig)
        end

        # Layouts the test data has no example of: a filter tier that holds one table for
        # the whole file, and a channel whose name is not a `DetectorId`.
        @testset "channels beyond the detectors" begin
            mktempdir() do tmpdir
                config = joinpath(tmpdir, "config.yaml")
                open(config, "w") do f
                    YAML.write(f, Dict("setups" => Dict("l200" => Dict("paths" =>
                        Dict("tier" => joinpath(tmpdir, "generated", "tier"))))))
                end
                withenv("LEGEND_DATA_CONFIG" => config) do
                    l200_tmp = LegendData(:l200)
                    m = 20
                    valid = isodd.(1:m)
                    chan_tier, evt_tier = DataTier(:jlchn), DataTier(:jlflt)
                    mkpath(dirname(l200_tmp.tier[chan_tier, fk]))
                    lh5open(l200_tmp.tier[chan_tier, fk], "w") do f
                        f["$(chan_tier)/$(det)"] = Table(e = collect(1.0:m))
                        f["$(chan_tier)/BF862-01"] = Table(e = collect(m:-1.0:1))
                    end
                    mkpath(dirname(l200_tmp.tier[evt_tier, fk]))
                    lh5open(l200_tmp.tier[evt_tier, fk], "w") do f
                        f["$(evt_tier)"] = Table(is_good = valid)
                    end

                    r = read_ldata(l200_tmp, chan_tier, fk; filterby = evt_tier => @pf $is_good)
                    @test Set(keys(r)) == Set([Symbol(det), Symbol("BF862-01")])
                    @test r[Symbol("BF862-01")].e == collect(m:-1.0:1)[valid]
                    @test r[Symbol(det)].e == collect(1.0:m)[valid]
                    @test read_ldata(l200_tmp, chan_tier, fk, det; filterby = evt_tier => @pf $is_good).e ==
                        collect(1.0:m)[valid]
                end
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
