# This file is a part of jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using Dates
using PropDicts

using LegendDataManagement: PropsDB, AnyProps, ValiditySelection

include("testing_utils.jl")

@testset "props_db" begin
    props_base_path = data_path(LegendDataConfig().setups.l200, "metadata")

    @test AnyProps(props_base_path) isa PropsDB
    @test_throws ArgumentError AnyProps("/no/such/props/db")

    pd = AnyProps(props_base_path)

    filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
    @test pd(filekey) isa PropsDB{ValiditySelection}
    @test pd("20221226T200846Z", :all) isa PropsDB{ValiditySelection}

    @test pd.hardware.configuration.channelmaps isa PropsDB
    @test pd.hardware.configuration.channelmaps(filekey) isa PropDict
    @test pd.hardware(filekey).configuration.channelmaps isa PropDict
end

@testset "props_db writes below the primary base" begin
    mktempdir() do dir
        par, ovr = mkpath(joinpath(dir, "par")), mkpath(joinpath(dir, "overrides", "ppars", "fltopt", "V00048B"))
        writeprops(joinpath(ovr, "calpartition008a.yaml"), PropDict(:trap => PropDict(:ft => 2.8)))
        write(joinpath(dirname(ovr), "validity.yaml"), "- valid_from: 20250827T125510Z\n  apply:\n    - V00048B/calpartition008a.yaml\n")
        pd = AnyProps(par, override_base = joinpath(dir, "overrides"))
        fltopt = joinpath(par, "ppars", "fltopt")

        @test pd.ppars.fltopt.V00048B.calpartition008a.trap.ft == 2.8
        writelprops(pd.ppars.fltopt.V00048B, :calpartition008a, PropDict(:trap => PropDict(:ft => 1.0, :rt => 3.0)))
        @test isfile(joinpath(fltopt, "V00048B", "calpartition008a.yaml"))
        @test readprops(joinpath(ovr, "calpartition008a.yaml")).trap.ft == 2.8
        @test pd.ppars.fltopt.V00048B.calpartition008a.trap.ft == 2.8
        @test pd.ppars.fltopt.V00048B.calpartition008a.trap.rt == 3.0

        @test pd.ppars.fltopt.V01404A isa LegendDataManagement.NoSuchPropsDBEntry
        @test PropDict(pd.ppars.fltopt[:V01404A, :calpartition009a]) == PropDict()
        writelprops(pd.ppars.fltopt.V01404A, :calpartition009a, PropDict(:trap => PropDict(:ft => 4.0)))
        LegendDataManagement.LDMUtils.writevalidity(pd.ppars.fltopt.V01404A, FileKey("l200-p16-r000-cal-20250827T125510Z"), "calpartition009a.yaml")
        @test isfile(joinpath(fltopt, "V01404A", "validity.yaml"))
        @test !ispath(joinpath(dirname(ovr), "V01404A"))
        @test AnyProps(par, override_base = joinpath(dir, "overrides")).ppars.fltopt.V01404A(FileKey("l200-p16-r000-cal-20250827T125510Z")).trap.ft == 4.0
    end
end
