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

    @testset "missing entry next to a validity.yaml" begin
        # a pars directory that already has entries and a validity.yaml (e.g. ppars/pz after the first
        # partition run) must still allow new entries to be created for detectors added later
        mktempdir() do dir
            mkpath(joinpath(dir, "V00048A"))
            writelprops(joinpath(dir, "V00048A", "calpartition001a.yaml"), PropDict(:τ => 400.0))
            write(joinpath(dir, "validity.yaml"), "- valid_from: 20221226T200846Z\n  apply:\n    - V00048A/calpartition001a.yaml\n")
            pd = AnyProps(dir)
            @test pd.V00048A isa PropsDB
            @test pd.B00000C isa LegendDataManagement.NoSuchPropsDBEntry
            writelprops(pd.B00000C, :calpartition001a, PropDict(:τ => 500.0))
            @test AnyProps(dir).B00000C.calpartition001a.τ == 500.0
        end
    end
end
