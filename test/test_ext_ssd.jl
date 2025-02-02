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
            det = SolidStateDetector{Float64}(l200, detname) 
            @test det isa SolidStateDetector
            sim = Simulation{Float64}(l200, detname, crystal_impurity = true)
            @test sim isa Simulation

            SolidStateDetectors.apply_initial_state!(sim, ElectricPotential, Grid(sim, max_tick_distance = 0.1u"mm"))

            # Check that all crystals are p-type
            @test all(sim.q_eff_imp.data .<= 0)

            # Compare active volume from SSD to active volume from LegendDataManagement
            active_volume_ssd = SolidStateDetectors.get_active_volume(sim.point_types)
            active_volume_ldm = LegendDataManagement.get_active_volume(l200.metadata.hardware.detectors.germanium.diodes[Symbol(detname)], 0.0)
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
end
