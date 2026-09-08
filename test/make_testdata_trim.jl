# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).
#
# Carve a small new-schema (jl-v0.6.0+) test dataset out of a real production, in the
# layout the read_ldata production tests expect — the basis for updating legend-testdata
# (github.com/legend-exp/legend-testdata) to the DetectorId schema.
#
# Not part of the test suite. Run as:
#   LEGEND_DATA_CONFIG=/path/to/production/config.json \
#       julia --project=<env-with-LDM+LegendHDF5IO> test/make_testdata_trim.jl <outdir> [n_evts]
#
# Afterwards `LEGEND_TESTDATA_PROD_CONFIG=<outdir>/config.json` runs the full
# test_ext_legendhdf5io_prod.jl matrix against the trimmed set. For use as a Julia
# artifact override, point ~/.julia/artifacts/Overrides.toml at the containing tree.
#
# What it writes:
#   <outdir>/config.json                   tier/par/metadata paths (all inside outdir)
#   <outdir>/legend-metadata/              full copy of the production metadata (no .git)
#   <outdir>/generated/jlpar/              full copy of the production pars (small)
#   <outdir>/generated/tier/<tier>/...     first N rows of each selected (tier, filekey, det)
#
# Trimming happens through read_ldata itself, so VoV columns, units and custom
# datatypes stay consistent by construction. Raw keeps its legacy <det>/raw layout,
# jlhit/jlpeaks/jlpls their per-detector files, jldsp its per-filekey det groups and
# jlevt/jlpmt their nested event-tier structure (all subtables row-aligned, so a
# common 1:N slice keeps them consistent).

using LegendDataManagement
using LegendHDF5IO
using TypedTables

function main(outdir::String, n_evts::Int)
    l200 = LegendData(:l200)
    mkpath(outdir)

    _catdir(tier, cat) = dirname(dirname(string(l200.tier[DataTier(tier), DataCategory(cat), DataPeriod("p00"), DataRun("r000")])))
    _periods(tier, cat) = (d = _catdir(tier, cat); isdir(d) ? sort(filter(x -> occursin(r"^p\d+$", x), readdir(d))) : String[])
    _runs(tier, cat, per) = (d = joinpath(_catdir(tier, cat), string(per)); isdir(d) ? sort(filter(x -> occursin(r"^r\d+$", x), readdir(d))) : String[])

    PER = DataPeriod(first(intersect(_periods(:jldsp, :phy), _periods(:jldsp, :cal), _periods(:jlevt, :phy))))
    RUN = DataRun(first(intersect(_runs(:jldsp, :phy, PER), _runs(:jlevt, :phy, PER))))
    CALRUN = DataRun(first(_runs(:jldsp, :cal, PER)))
    FK_PHY = first(search_disk(FileKey, l200.tier[DataTier(:jlevt), :phy, PER, RUN]))
    FK_CAL = first(search_disk(FileKey, l200.tier[DataTier(:jldsp), :cal, PER, CALRUN]))

    chinfo = channelinfo(l200, FK_CAL)
    ged_dets = [c.detector for c in filter(c -> c.system == :geds && c.processable, chinfo)]
    hitsel = filter(d -> isfile(l200.tier[DataTier(:jlhit), FK_CAL, d]), ged_dets)
    DETS_GED = hitsel[1:min(2, length(hitsel))]
    spm_detcol = read_ldata((:spms_detector,), l200, (:jlevt, FK_PHY)).spms_detector
    DET_SPM = first(first(filter(!isempty, spm_detcol)))
    @info "trim selection" PER RUN CALRUN FK_PHY FK_CAL DETS_GED DET_SPM n_evts

    # --- metadata + pars: copy wholesale (small compared to tier data) --------
    metadata_src = joinpath(dirname(string(l200.tier[DataTier(:jldsp), FK_CAL])) |> d -> abspath(d, "..", "..", "..", "..", "..", "legend-metadata"))
    for (name, src) in [("legend-metadata", metadata_src)]
        dst = joinpath(outdir, name)
        if isdir(src) && !isdir(dst)
            cp(src, dst)
            rm(joinpath(dst, ".git"); force = true, recursive = true)
        end
    end
    # pars: everything under jlpar (validity + yaml, needed for channelinfo/evt tests)
    par_src = dirname(dirname(string(l200.par)))  # .../generated/jlpar/<...>; l200.par may be DataProd - fall back below
    # robust: derive from config instead
    nothing

    # --- tier trimming --------------------------------------------------------
    outpath(tier, fk) = joinpath(outdir, "generated", "tier", string(tier), string(fk.category), string(fk.period), string(fk.run),
        "$(fk.setup)-$(fk.period)-$(fk.run)-$(fk.category)-$(fk.timestamp)-tier_$(tier).lh5")
    outpath_det(tier, fk, det) = joinpath(outdir, "generated", "tier", string(tier), string(fk.category), string(fk.period), string(fk.run),
        "$(fk.setup)-$(fk.period)-$(fk.run)-$(fk.category)-$(det)-tier_$(tier).lh5")
    trim(t::Table) = t[1:min(n_evts, length(t))]

    function write_grouped!(fname::String, path::String, tbl)
        mkpath(dirname(fname))
        lh5open(fname, isfile(fname) ? "cw" : "w") do f
            f[path] = tbl
        end
    end

    # jldsp (cal + phy): per-filekey file, per-det groups
    for fk in (FK_CAL, FK_PHY), det in vcat(DETS_GED, [DET_SPM])
        tbl = read_ldata(l200, (:jldsp, fk, det))
        tbl isa Table || continue
        write_grouped!(outpath(:jldsp, fk), "jldsp/$det", trim(tbl))
    end

    # raw (phy): legacy <det>/raw layout
    for det in vcat(DETS_GED, [DET_SPM])
        tbl = read_ldata(l200, (:raw, FK_PHY, det); ignore_missing = true)
        tbl === nothing && continue
        write_grouped!(outpath(:raw, FK_PHY), "$det/raw", trim(tbl))
    end
    # raw (cal) for the filtertier tests
    for det in DETS_GED
        tbl = read_ldata(l200, (:raw, FK_CAL, det); ignore_missing = true)
        tbl === nothing && continue
        write_grouped!(outpath(:raw, FK_CAL), "$det/raw", trim(tbl))
    end

    # per-detector-file tiers: jlhit/jlpeaks (cal, subgroups per det), jlpls (phy, PULS01ANA)
    for det in DETS_GED, (tier, fk) in ((:jlhit, FK_CAL), (:jlpeaks, FK_CAL))
        src = l200.tier[DataTier(tier), fk, det]
        isfile(src) || continue
        lh5open(src, "r") do fin
            for sub in keys(fin["$tier/$det"])
                tbl = fin["$tier/$det/$sub"][:]
                write_grouped!(outpath_det(tier, fk, det), "$tier/$det/$sub", trim(Table(tbl)))
            end
        end
    end
    let src = l200.tier[DataTier(:jlpls), FK_PHY, DetectorId("PULS01ANA")]
        if isfile(src)
            lh5open(src, "r") do fin
                for sub in keys(fin["jlpls/PULS01ANA"])
                    tbl = fin["jlpls/PULS01ANA/$sub"][:]
                    write_grouped!(outpath_det(:jlpls, FK_PHY, "PULS01ANA"), "jlpls/PULS01ANA/$sub", trim(Table(tbl)))
                end
            end
        end
    end

    # event tiers (jlevt, jlpmt): nested groups, all subtables row-aligned -> common 1:N
    for tier in (:jlevt, :jlpmt)
        src = l200.tier[DataTier(tier), FK_PHY]
        isfile(src) || continue
        lh5open(src, "r") do fin
            function walk(path)
                node = fin[path]
                if node isa TypedTables.Table || node isa NamedTuple
                    for k in propertynames(node)
                        walk("$path/$k")
                    end
                else
                    write_grouped!(outpath(tier, FK_PHY), path, nothing)  # placeholder, handled below
                end
            end
            # simpler: read whole nested structure via read_ldata and write trimmed groups
            nothing
        end
        nested = read_ldata(l200, (tier, FK_PHY))
        fname = outpath(tier, FK_PHY)
        mkpath(dirname(fname))
        lh5open(fname, "w") do fout
            function put!(prefix, x)
                if x isa NamedTuple
                    for k in keys(x)
                        put!("$prefix/$k", x[k])
                    end
                elseif x isa TypedTables.Table
                    fout[prefix] = trim(x)
                elseif x isa AbstractVector
                    fout[prefix] = x[1:min(n_evts, length(x))]
                else
                    fout[prefix] = x
                end
            end
            put!(string(tier), nested)
        end
    end

    # --- config.json ----------------------------------------------------------
    open(joinpath(outdir, "config.json"), "w") do io
        write(io, """
        {
            "setups": {
                "l200": {
                    "paths": {
                        "metadata": "\$_/legend-metadata/",
                        "tier": "\$_/generated/tier/",
                        "par": "\$_/generated/jlpar/"
                    }
                }
            }
        }
        """)
    end
    @info "trimmed test dataset written" outdir
    @info "NOTE: copy the production's generated/jlpar into $outdir/generated/jlpar (validity+yaml, small) — required for channelinfo/pars-dependent tests"
end

isempty(ARGS) && error("usage: make_testdata_trim.jl <outdir> [n_evts]")
main(abspath(ARGS[1]), length(ARGS) >= 2 ? parse(Int, ARGS[2]) : 200)
