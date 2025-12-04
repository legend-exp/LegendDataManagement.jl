# This file is a part of jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using SolidStateDetectors
using SolidStateDetectors: ConstantImpurityDensity
using Unitful
using LegendHDF5IO

include("testing_utils.jl")

@testset "test_ext_ssd" begin
    l200 = LegendData(:l200)
    for detname in (:V99000A, :B99000A, :C99000A, :P99000A)
        @testset "$(detname)" begin
            det = SolidStateDetector(l200, detname)
            @test det isa SolidStateDetector
            det = SolidStateDetector{Float64}(l200, detname) 
            @test det isa SolidStateDetector
            @test !isfile("$(detname).yaml")
            
            sim = Simulation{Float64}(l200, detname, ssd_config_filename = "$detname.yaml")
            @test sim isa Simulation

            sim2 = Simulation{Float64}("$detname.yaml")
            @test sim2 == sim


            # Compare active volume from SSD to active volume from LegendDataManagement
            detector_props = getproperty(l200.metadata.hardware.detectors.germanium.diodes, detname)
            fccd = detector_props.characterization.combined_0vbb_analysis.fccd_in_mm.value
            
            SolidStateDetectors.apply_initial_state!(sim, ElectricPotential, Grid(sim, max_tick_distance = 0.1u"mm"))
            active_volume_ssd = SolidStateDetectors.get_active_volume(sim.point_types)
            active_volume_ldm = LegendDataManagement.get_active_volume(l200.metadata.hardware.detectors.germanium.diodes[Symbol(detname)], fccd)
            @test isapprox(active_volume_ssd, active_volume_ldm, rtol = 0.01)

            # The creation via config files allows to save Simulations to files using LegendHDF5IO
            lh5name = "$(detname).lh5"
            isfile(lh5name) && rm(lh5name)
            @test_nowarn ssd_write(lh5name, sim)
            @test isfile(lh5name)
            @test sim == ssd_read(lh5name, Simulation)
            @test_nowarn rm(lh5name)
            @test !isfile(lh5name)
        end
    end

    @testset "Test HPGeEnvironment" begin
        detname = :V99000A
        env = LegendDataManagement.HPGeEnvironment("LAr", 87u"K")
        sim = Simulation(l200, detname, env)
        @test sim isa Simulation
        sim = Simulation{Float64}(l200, detname, env)
        @test sim isa Simulation
        @test sim.medium == SolidStateDetectors.material_properties[:LAr]
        @test sim.detector.semiconductor.temperature == 87
    end
end
