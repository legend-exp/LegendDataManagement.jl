# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

module LegendDataManagementSolidStateDetectorsExt

using SolidStateDetectors
import SolidStateDetectors.ConstructiveSolidGeometry as CSG

using LegendDataManagement
using Unitful
using PropDicts


const _SSDDefaultNumtype = Float32

"""
    SolidStateDetector[{T<:Real}](data::LegendData, detector::DetectorIdLike
    SolidStateDetector[{T<:Real}(::Type{LegendData}, detector_props::AbstractDict)
    SolidStateDetector[{T<:Real}(::Type{LegendData}, json_filename::AbstractString)

LegendDataManagement provides an extension for SolidStateDetectors, a
`SolidStateDetector` can be constructed from LEGEND metadata  using the
methods above.
"""
function SolidStateDetectors.SolidStateDetector(data::LegendData, detector::DetectorIdLike)
    SolidStateDetectors.SolidStateDetector{_SSDDefaultNumtype}(data, detector)
end

function SolidStateDetectors.SolidStateDetector{T}(data::LegendData, detector::DetectorIdLike) where {T<:Real}
    detector_props = getproperty(data.metadata.hardware.detectors.germanium.diodes, Symbol(detector))
    SolidStateDetector{T}(LegendData, detector_props)
end


to_SSD_units(::Type{T}, x, unit) where {T} = T(SolidStateDetectors.to_internal_units(x*unit)) 


function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, filename::String) where {T<:Real}
    SolidStateDetector{T}(LegendData, readprops(filename, subst_pathvar = false, subst_env = false, trim_null = false))
end

function SolidStateDetectors.SolidStateDetector(::Type{LegendData}, filename::String)
    SolidStateDetector{_SSDDefaultNumtype}(LegendData, filename)
end

function SolidStateDetectors.SolidStateDetector(::Type{LegendData}, meta::AbstractDict)
    SolidStateDetectors.SolidStateDetector{_SSDDefaultNumtype}(LegendData, meta)
end

function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, meta::AbstractDict) where {T<:Real}
    SolidStateDetectors.SolidStateDetector{T}(LegendData, convert(PropDict, meta))
end

function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, meta::PropDict) where {T<:Real}
    # Not all possible configurations are yet implemented!
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_1.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_2.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_3.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_4.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_5.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_6.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_7.pdf

    gap = to_SSD_units(T, 1, u"mm")

    dl_thickness_in_mm = :dl_thickness_in_mm in keys(meta.geometry) ? meta.geometry.dl_thickness_in_mm : 0
    li_thickness =  to_SSD_units(T, dl_thickness_in_mm, u"mm")

    crystal_radius = to_SSD_units(T, meta.geometry.radius_in_mm, u"mm")
    crystal_height = to_SSD_units(T, meta.geometry.height_in_mm, u"mm")
    
    is_coax = meta.type == "coax"

    # main crystal
    semiconductor_geometry = CSG.Cone{T}(CSG.ClosedPrimitive; 
        r = crystal_radius, 
        hZ = crystal_height / 2, 
        origin = CartesianPoint{T}(0, 0, crystal_height / 2)
    )

    # borehole
    has_borehole = haskey(meta.geometry, :borehole)
    if is_coax && !has_borehole
        error("Coax detectors should have boreholes")
    end
    if has_borehole
        borehole_depth = to_SSD_units(T, meta.geometry.borehole.depth_in_mm, u"mm")
        borehole_radius = to_SSD_units(T, meta.geometry.borehole.radius_in_mm, u"mm")
        semiconductor_geometry -= CSG.Cone{T}(CSG.ClosedPrimitive; 
            r = borehole_radius, 
            hZ = borehole_depth / 2 + gap, 
            origin = CartesianPoint{T}(0, 0, is_coax ? borehole_depth/2 - gap : crystal_height - borehole_depth/2 + gap)
        )    
    end
    
    # borehole taper
    has_borehole_taper = haskey(meta.geometry.taper, :borehole)
    if has_borehole_taper
        borehole_taper_height = to_SSD_units(T, meta.geometry.taper.borehole.height_in_mm, u"mm")
        if haskey(meta.geometry.taper.borehole, :radius_in_mm)
            borehole_taper_radius = to_SSD_units(T, meta.geometry.taper.borehole.radius_in_mm, u"mm")
            borehole_taper_angle = atan(borehole_taper_radius, borehole_taper_height)
        elseif haskey(meta.geometry.taper.borehole, :angle_in_deg)
            borehole_taper_angle = to_SSD_units(T, meta.geometry.taper.borehole.angle_in_deg, u"°")
            borehole_taper_radius = borehole_taper_height * tan(borehole_taper_angle)
        else
            error("The borehole taper needs either radius_in_mm or angle_in_deg")
        end
        has_borehole_taper = borehole_taper_height > 0 && borehole_taper_angle > 0
        if has_borehole_taper && !has_borehole 
            error("A detector without a borehole cannot have a borehole taper.")
        end
        if has_borehole_taper && is_coax
            error("Coax detectors should not have borehole tapers")
        end
        if has_borehole_taper
            r_center = borehole_radius + borehole_taper_height * tan(borehole_taper_angle) / 2
            hZ = borehole_taper_height/2
            Δr = hZ * tan(borehole_taper_angle)         
            r_out_bot = r_center - Δr
        r_out_top = r_center + Δr * (1 + 2*gap/hZ)
        r = ((r_out_bot,), (r_out_top,))
            semiconductor_geometry -= CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r,
                hZ = hZ + gap, 
                origin = CartesianPoint{T}(0, 0, crystal_height - borehole_taper_height/2 + gap)
            )
        end
    end

    # top taper
    if haskey(meta.geometry.taper, :top)
        top_taper_height = to_SSD_units(T, meta.geometry.taper.top.height_in_mm, u"mm")
        if haskey(meta.geometry.taper.top, :radius_in_mm)
            top_taper_radius = to_SSD_units(T, meta.geometry.taper.top.radius_in_mm, u"mm")
            top_taper_angle = atan(top_taper_radius, top_taper_height)
        elseif haskey(meta.geometry.taper.top, :angle_in_deg)
            top_taper_angle = to_SSD_units(T, meta.geometry.taper.top.angle_in_deg, u"°")
            top_taper_radius = top_taper_height * tan(top_taper_angle)
        else
            error("The top taper needs either radius_in_mm or angle_in_deg")
        end
        has_top_taper = top_taper_height > 0 && top_taper_angle > 0
        if has_top_taper
            r_center = crystal_radius - top_taper_height * tan(top_taper_angle) / 2
            hZ = top_taper_height/2 + 1gap
            Δr = hZ * tan(top_taper_angle)         
            r_in_bot = r_center + Δr
            r_in_top = r_center - Δr
            r_out = max(r_in_top, r_in_bot) + gap # ensure that r_out is always bigger as r_in
            r = ((r_in_bot, r_out),(r_in_top, r_out))
            semiconductor_geometry -= CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r,
                hZ = hZ, 
                origin = CartesianPoint{T}(0, 0, crystal_height - top_taper_height/2)
            )
        end
    end

    # bot outer taper
    bot_taper_height = to_SSD_units(T, meta.geometry.taper.bottom.height_in_mm, u"mm")
    if :radius_in_mm in keys(meta.geometry.taper.bottom)
        bot_taper_radius = to_SSD_units(T, meta.geometry.taper.bottom.radius_in_mm, u"mm")
        bot_taper_angle = atan(bot_taper_radius, bot_taper_height)
    elseif :angle_in_deg in keys(meta.geometry.taper.bottom)
        bot_taper_angle = to_SSD_units(T, meta.geometry.taper.bottom.angle_in_deg, u"°")
        bot_taper_radius = bot_taper_height * tan(bot_taper_angle)
    else
        error("The bottom outer tape needs either radius_in_mm or angle_in_deg")
    end
    has_bot_taper = bot_taper_height > 0 && bot_taper_angle > 0
    if has_bot_taper
        r_center = crystal_radius - bot_taper_height * tan(bot_taper_angle) / 2
        hZ = bot_taper_height/2 + 1gap
        Δr = hZ * tan(bot_taper_angle)         
        r_in_bot = r_center - Δr
        r_in_top = r_center + Δr
        r_out = max(r_in_top, r_in_bot) + gap # ensure that r_out is always bigger as r_in
        r = ((r_in_bot, r_out),(r_in_top, r_out))
        semiconductor_geometry -= CSG.Cone{T}(CSG.ClosedPrimitive; 
            r = r,
            hZ = hZ, 
            origin = CartesianPoint{T}(0, 0, bot_taper_height/2)
        )
    end

    # groove
    has_groove = haskey(meta.geometry, :groove)
    if has_groove
        groove_inner_radius = to_SSD_units(T, meta.geometry.groove.radius_in_mm.inner, u"mm")
        groove_outer_radius = to_SSD_units(T, meta.geometry.groove.radius_in_mm.outer, u"mm")
        groove_depth = to_SSD_units(T, meta.geometry.groove.depth_in_mm, u"mm")
        has_groove = groove_outer_radius > 0 && groove_depth > 0 && groove_inner_radius > 0
        if has_groove
            hZ = groove_depth / 2 + gap
            r_in = groove_inner_radius
            r_out = groove_outer_radius
            r = ((r_in, r_out), (r_in, r_out))
            semiconductor_geometry -= CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r, 
                hZ = hZ,
                origin = CartesianPoint{T}(0, 0, groove_depth / 2 - gap)
            )
        end
    end
    
    
    # bulletization
    # is_bulletized = !all(values(meta.geometry.bulletization) .== 0)
    # is_bulletized && @warn "Bulletization is not implemented yet, ignore for now."

    # extras
    haskey(meta.geometry, :extra) && @warn "Extras are not implemented yet, ignore for now."


    ### P+ CONTACT ###

    pp_radius = to_SSD_units(T, meta.geometry.pp_contact.radius_in_mm, u"mm")
    pp_depth = to_SSD_units(T, meta.geometry.pp_contact.depth_in_mm, u"mm")
    pp_contact_geometry = if is_coax
        CSG.Cone{T}(CSG.ClosedPrimitive;
            r = ((borehole_radius, borehole_radius), (borehole_radius, borehole_radius)),
            hZ = borehole_depth/2,
            origin = CartesianPoint{T}(0, 0, borehole_depth / 2)
        ) + CSG.Cone{T}(CSG.ClosedPrimitive;
            r = borehole_radius,
            hZ = 0,
            origin = CartesianPoint{T}(0, 0, borehole_depth)
        ) + CSG.Cone{T}(CSG.ClosedPrimitive;
            r = ((borehole_radius, pp_radius), (borehole_radius, pp_radius)),
            hZ = 0
        )
    else
        CSG.Cone{T}(CSG.ClosedPrimitive; 
            r = pp_radius, 
            hZ = pp_depth / 2, 
            origin = CartesianPoint{T}(0, 0, pp_depth / 2)
        )
    end


    ### MANTLE CONTACT ###
    
    mantle_contact_geometry = begin # top plate
        top_plate = begin
            r = if !has_borehole || is_coax
                !has_top_taper ? crystal_radius : crystal_radius - top_taper_radius
            else has_borehole && !is_coax
                r_in = borehole_radius
                r_out = crystal_radius
                if has_borehole_taper r_in += borehole_taper_radius end
                if has_top_taper r_out -= top_taper_radius end
                ((r_in, r_out), (r_in, r_out))
            end
            CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r, 
                hZ = li_thickness / 2, 
                origin = CartesianPoint{T}(0, 0, crystal_height - li_thickness / 2)
            )
        end
        mc_geometry = top_plate
        
        # borehole at outer taper
        if has_top_taper
            Δr_li_thickness = li_thickness / cos(top_taper_angle)
            hZ = top_taper_height/2
            r_bot = crystal_radius 
            r_top = crystal_radius - top_taper_radius
            r = ((r_bot - Δr_li_thickness, r_bot),(r_top - Δr_li_thickness, r_top))
            mc_geometry += CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r,
                hZ = hZ, 
                origin = CartesianPoint{T}(0, 0, crystal_height - top_taper_height/2)
            )
        end

        # contact in borehole
        if has_borehole_taper
            Δr_li_thickness = li_thickness / cos(borehole_taper_angle)
            hZ = borehole_taper_height/2    
            r_bot = borehole_radius
            r_top = borehole_radius + borehole_taper_radius
            r = ((r_bot, r_bot+Δr_li_thickness),(r_top, r_top+Δr_li_thickness))
            mc_geometry += CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r,
                hZ = hZ, 
                origin = CartesianPoint{T}(0, 0, crystal_height - borehole_taper_height/2)
            )

            hZ = (borehole_depth - borehole_taper_height) / 2
            r = ((borehole_radius, borehole_radius+Δr_li_thickness),(borehole_radius, borehole_radius+Δr_li_thickness))
            mc_geometry += CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r,
                hZ = hZ, 
                origin = CartesianPoint{T}(0, 0, crystal_height - borehole_taper_height - hZ)
            )
        elseif has_borehole && !is_coax # but no borehole taper
            hZ = borehole_depth / 2
            r = ((borehole_radius, borehole_radius+li_thickness),(borehole_radius, borehole_radius+li_thickness))
            mc_geometry += CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r,
                hZ = hZ, 
                origin = CartesianPoint{T}(0, 0, crystal_height - hZ)
            )
        end

        if has_borehole && !is_coax
            r = borehole_radius + li_thickness
            mc_geometry += CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r, 
                hZ = li_thickness / 2, 
                origin = CartesianPoint{T}(0, 0, crystal_height - borehole_depth - li_thickness / 2)
            )
        end

        # outer surface of mantle contact
        begin
            r = ((crystal_radius-li_thickness, crystal_radius),(crystal_radius-li_thickness, crystal_radius))
            hZ = crystal_height
            if has_top_taper hZ -= top_taper_height end
            z_origin = hZ/2
            if has_bot_taper 
                hZ -= bot_taper_height 
                z_origin += bot_taper_height/2
            end
            mc_geometry += CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r, 
                hZ = hZ / 2, 
                origin = CartesianPoint{T}(0, 0, z_origin)
            )
        end

        # bottom outer taper contact
        if has_bot_taper
            Δr_li_thickness = li_thickness / cos(bot_taper_angle)
            hZ = bot_taper_height/2
            r_bot = crystal_radius - bot_taper_radius
            r_top = crystal_radius
            r = ((r_bot - Δr_li_thickness, r_bot),(r_top - Δr_li_thickness, r_top))
            mc_geometry += CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r,
                hZ = hZ, 
                origin = CartesianPoint{T}(0, 0, hZ)
            )
        end  

        # bottom surface of mantle contact (only if it has a groove ?)
        if has_groove && groove_outer_radius > 0
            r_in = groove_outer_radius 
            r_out = crystal_radius
            if has_bot_taper r_out -= bot_taper_radius end
            r = ((r_in, r_out), (r_in, r_out))
            mc_geometry += CSG.Cone{T}(CSG.ClosedPrimitive; 
                r = r, 
                hZ = li_thickness / 2, 
                origin = CartesianPoint{T}(0, 0, li_thickness / 2)
            )
        end

        mc_geometry
    end


    # Hardcoded parameter values: In future, should be defined in config file
    temperature = T(78) 
    material = SolidStateDetectors.material_properties[:HPGe]
    
    # Impurity Model: Information are stored in `meta.production.impcc`
    # For now: Constant impurity density: 
    #   n-type: positive impurity density
    #   p-type: negative impurity density
    # Assume p-type
    constant_impurity_density = ustrip(uconvert(u"m^-3", T(-1e9) * u"cm^-3"))
    impurity_density_model = SolidStateDetectors.CylindricalImpurityDensity{T}(
        (0, 0, constant_impurity_density), # offsets
        (0, 0, 0)                          # linear slopes
    )

    # Charge Drift Model: 
    # Use example ADL charge drift model from SSD (Crystal axis <100> is at φ = 0):
    adl_charge_drift_config_file = joinpath(dirname(dirname(pathof(SolidStateDetectors))), 
        "examples/example_config_files/ADLChargeDriftModel/drift_velocity_config.yaml")
    charge_drift_model = SolidStateDetectors.ADLChargeDriftModel{T}(adl_charge_drift_config_file);

    semiconductor = SolidStateDetectors.Semiconductor(temperature, material, impurity_density_model, charge_drift_model, semiconductor_geometry)

    operation_voltage = T(meta.characterization.manufacturer.recommended_voltage_in_V)
    pp_contact = SolidStateDetectors.Contact( zero(T), material, 1, "Point Contact", pp_contact_geometry )
    mantle_contact = SolidStateDetectors.Contact( operation_voltage, material, 2, "Mantle Contact", mantle_contact_geometry )

    semiconductor, (pp_contact, mantle_contact)

    passives = missing # possible holding structure around the detector
    virtual_drift_volumes = missing
    SolidStateDetector{T}( meta.name, semiconductor, [pp_contact, mantle_contact], passives, virtual_drift_volumes )
end

end # module LegendDataManagementSolidStateDetectorsExt
