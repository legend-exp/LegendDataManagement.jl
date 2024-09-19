# This file is a part of jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test
using SolidStateDetectors

include("testing_utils.jl")

function SolidStateDetectors.Simulation(l200, detname; medium::Union{String, Symbol} = "vacuum")

    config_dict = Dict()
    radius = l200.metadata.hardware.detectors.germanium.diodes[Symbol(detname)].geometry.radius_in_mm / 1000
    height = l200.metadata.hardware.detectors.germanium.diodes[Symbol(detname)].geometry.height_in_mm / 1000
    det = SolidStateDetector(l200, detname);

    function get_material_name(material::NamedTuple)
        collect(keys(SolidStateDetectors.material_properties))[findfirst(getindex.(values(SolidStateDetectors.material_properties), :name) .== material.name)]
    end
        
    config_dict["name"] = "$(detname)"
    config_dict["units"] = Dict(
        "length" => "m",
        "angle" => "deg",
        "potential" => "V",
        "temperature" => "K"
    )
    config_dict["grid"] = Dict(
        "coordinates" => "cylindrical",
        "axes" => Dict(
            "r" => Dict(
                "to" => "$(radius + 0.01)",
                "boundaries" => "inf"
            ),
            "phi" => Dict(
                "from" => 0,
                "to" => 0,
                "boundaries" => "periodic"
            ),
            "z" => Dict(
                "from" => "-0.01",
                "to" => "$(height + 0.01)",
                "boundaries" => Dict(
                    "left" => "inf",
                    "right" => "inf"
                )
            )
        )
    )
    config_dict["medium"] = medium
    config_dict["detectors"] = [
        Dict(
            "semiconductor" => Dict(
                "material" => "$(get_material_name(det.semiconductor.material))",
                "temperature" => det.semiconductor.temperature,
                "geometry" => SolidStateDetectors.ConstructiveSolidGeometry.Dictionary(det.semiconductor.geometry)
                # charge_drift_model ?
                # impurity_density ?
            ),
            "contacts" => [
                Dict(
                    "material" => "$(get_material_name(c.material))",
                    "id" => c.id,
                    "potential" => c.potential,
                    "geometry" => SolidStateDetectors.ConstructiveSolidGeometry.Dictionary(c.geometry)
                )
                for c in det.contacts
            ]
        ) 
    ]
    T = SolidStateDetectors.get_precision_type(det)
    Simulation{T}(config_dict)
end

@testset "test_ext_ssd" begin
    l200 = LegendData(:l200)
    for detname in (:V99000A, :B99000A, :C99000A, :P99000A)
        @testset "$(detname)" begin
            det = SolidStateDetector(l200, detname) 
            @test det isa SolidStateDetector
            sim = Simulation(l200, detname)
            @test sim isa Simulation

            # Compare active volume from SSD to active volume from LegendDataManagement
            sim.detector = SolidStateDetector(sim.detector, contact_id = 1, contact_potential = 0) # Set potential to 0V to avoid long simulation times
            sim.detector = SolidStateDetector(sim.detector, contact_id = 2, contact_potential = 0) # (we just need the PointTypes for the active volume)
            calculate_electric_potential!(sim, max_tick_distance = 0.1u"mm", refinement_limits = missing, verbose = false)
            active_volume_ssd = SolidStateDetectors.get_active_volume(sim.point_types)
            active_volume_ldm = LegendDataManagement.get_active_volume(l200.metadata.hardware.detectors.germanium.diodes[Symbol(detname)], 0.0)
            @test isapprox(active_volume_ssd, active_volume_ldm, rtol = 0.01)
        end
    end
end
