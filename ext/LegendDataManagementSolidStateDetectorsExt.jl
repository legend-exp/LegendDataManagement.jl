# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

module LegendDataManagementSolidStateDetectorsExt

using SolidStateDetectors
using LegendDataManagement
using Unitful
using PropDicts

const _SSDDefaultNumtype = Float32


"""
    SolidStateDetector[{T<:AbstractFloat}](data::LegendData, detector::DetectorIdLike)
    SolidStateDetector[{T<:AbstractFloat}(::Type{LegendData}, detector_props::AbstractDict)
    SolidStateDetector[{T<:AbstractFloat}(::Type{LegendData}, json_filename::AbstractString)

LegendDataManagement provides an extension for SolidStateDetectors, a
`SolidStateDetector` can be constructed from LEGEND metadata using the
methods above.
"""
function SolidStateDetectors.SolidStateDetector(data::LegendData, detector::DetectorIdLike)
    SolidStateDetectors.SolidStateDetector{_SSDDefaultNumtype}(data, detector)
end

function SolidStateDetectors.SolidStateDetector{T}(data::LegendData, detector::DetectorIdLike) where {T<:AbstractFloat}
    detector_props = getproperty(data.metadata.hardware.detectors.germanium.diodes, Symbol(detector))
    xtal_props = getproperty(data.metadata.hardware.detectors.germanium.crystals, Symbol(string(detector)[1:end-1]))
    SolidStateDetector{T}(LegendData, detector_props, xtal_props)
end

function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, filename::String) where {T<:AbstractFloat}
    SolidStateDetector{T}(LegendData, readprops(filename, subst_pathvar = false, subst_env = false, trim_null = false))
end

function SolidStateDetectors.SolidStateDetector(::Type{LegendData}, filename::String)
    SolidStateDetector{_SSDDefaultNumtype}(LegendData, filename)
end

function SolidStateDetectors.SolidStateDetector(::Type{LegendData}, meta::AbstractDict)
    SolidStateDetectors.SolidStateDetector{_SSDDefaultNumtype}(LegendData, meta)
end

function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, meta::AbstractDict) where {T<:AbstractFloat}
    SolidStateDetectors.SolidStateDetector{T}(LegendData, convert(PropDict, meta), LegendDataManagement.NoSuchPropsDBEntry("",[]))
end

function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, meta::PropDict, xtal_meta::Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}) where {T<:AbstractFloat}
    config_dict = create_SSD_config_dict_from_LEGEND_metadata(meta, xtal_meta)
    return SolidStateDetector{T}(config_dict, SolidStateDetectors.construct_units(config_dict))
end

"""
    Simulation[{T<:AbstractFloat}](data::LegendData, detector::DetectorIdLike)
    Simulation[{T<:AbstractFloat}(::Type{LegendData}, detector_props::AbstractDict)
    Simulation[{T<:AbstractFloat}(::Type{LegendData}, json_filename::AbstractString)

LegendDataManagement provides an extension for SolidStateDetectors, a
`Simulation` can be constructed from LEGEND metadata using the
methods above.
"""
function SolidStateDetectors.Simulation(data::LegendData, detector::DetectorIdLike)
    SolidStateDetectors.Simulation{_SSDDefaultNumtype}(data, detector)
end

function SolidStateDetectors.Simulation{T}(data::LegendData, detector::DetectorIdLike) where {T<:AbstractFloat}
    detector_props = getproperty(data.metadata.hardware.detectors.germanium.diodes, Symbol(detector))
    xtal_props = getproperty(data.metadata.hardware.detectors.germanium.crystals, Symbol(string(detector)[1:end-1]))
    Simulation{T}(LegendData, detector_props, xtal_props)
end

function SolidStateDetectors.Simulation{T}(::Type{LegendData}, filename::String) where {T<:AbstractFloat}
    Simulation{T}(LegendData, readprops(filename, subst_pathvar = false, subst_env = false, trim_null = false))
end

function SolidStateDetectors.Simulation(::Type{LegendData}, filename::String)
    Simulation{_SSDDefaultNumtype}(LegendData, filename)
end

function SolidStateDetectors.Simulation(::Type{LegendData}, meta::AbstractDict)
    SolidStateDetectors.Simulation{_SSDDefaultNumtype}(LegendData, meta)
end

function SolidStateDetectors.Simulation{T}(::Type{LegendData}, meta::AbstractDict) where {T<:AbstractFloat}
    SolidStateDetectors.Simulation{T}(LegendData, convert(PropDict, meta), LegendDataManagement.NoSuchPropsDBEntry("", []))
end

function SolidStateDetectors.Simulation{T}(::Type{LegendData}, meta::PropDict, xtal_meta::Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}) where {T<:AbstractFloat}
    config_dict = create_SSD_config_dict_from_LEGEND_metadata(meta, xtal_meta)
    return Simulation{T}(config_dict)
end


function create_SSD_config_dict_from_LEGEND_metadata(meta::PropDict, xtal_meta::X; dicttype = Dict{String,Any}) where {X <: Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}}

    # Not all possible configurations are yet implemented!
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_1.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_2.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_3.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_4.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_5.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_6.pdf
    # https://github.com/legend-exp/legend-metadata/blob/main/hardware/detectors/detector-metadata_7.pdf

    gap = 1.0

    dl_thickness_in_mm = :dl_thickness_in_mm in keys(meta.geometry) ? meta.geometry.dl_thickness_in_mm : 0
    li_thickness =  dl_thickness_in_mm

    crystal_radius = meta.geometry.radius_in_mm
    crystal_height = meta.geometry.height_in_mm
    
    is_coax = meta.type == "coax"
    has_bottom_cylinder = haskey(meta.geometry, :extra)  && haskey(meta.geometry.extra, :bottom_cylinder)
    has_top_groove = haskey(meta.geometry, :extra) && haskey(meta.geometry.extra, :topgroove)

    config_dict = dicttype(
        "name" => meta.name,
        "units" => dicttype(
            "length" => "mm",
            "potential" => "V",
            "angle" => "deg",
            "temperature" => "K"
        ),
        "grid" => dicttype(
            "coordinates" => "cylindrical",
            "axes" => dicttype(
                "r" => dicttype(
                    "to" => crystal_radius * 1.2,
                    "boundaries" => "inf"
                ),
                "phi" => dicttype(
                    "from" => 0,
                    "to" => 0,
                    "boundaries" => "periodic"
                ),
                "z" => dicttype(
                    "from" => -0.2 * crystal_height,
                    "to" => 1.2 * crystal_height,
                    "boundaries" => "inf"
                )
            )
        ),
        "medium" => "vacuum",
        "detectors" => []
    )

    push!(config_dict["detectors"], dicttype(
        "semiconductor" => dicttype(
            "material" => "HPGe",
            "charge_drift_model" => dicttype(
                "include" => joinpath(SolidStateDetectors.get_path_to_example_config_files(), "ADLChargeDriftModel", "drift_velocity_config.yaml"),
            ),
            # "impurity_density" => dicttype("parameters" => Vector()),
            "geometry" => dicttype(),
            "temperature" => 78
        ),
        "contacts" => []
        ))
    
    # main crystal
    semiconductor_geometry_basis = dicttype("cone" => dicttype(
        "r" => crystal_radius,
        "h" => crystal_height,
        "origin" => [0, 0, crystal_height / 2]
    ))

    if has_bottom_cylinder

        cyl_radius = meta.geometry.extra.bottom_cylinder.radius_in_mm
        cyl_height = meta.geometry.extra.bottom_cylinder.height_in_mm
        cyl_transition = meta.geometry.extra.bottom_cylinder.transition_in_mm
        upper_height = crystal_height - cyl_height - cyl_transition

        semiconductor_geometry_basis = dicttype(
            "union" => [
                dicttype(
                    "tube" => dicttype(
                        "r" => crystal_radius,
                        "h" => upper_height,
                        "origin" => dicttype(
                            "z" => crystal_height - upper_height/2
                        )
                    )
                ),
                dicttype(
                    "cone" => dicttype(
                        "r" => dicttype(
                            "bottom" => cyl_radius,
                            "top" => crystal_radius
                        ),
                        "h" => cyl_transition,
                        "origin" => dicttype(
                            "z" => cyl_height + cyl_transition/2
                        )
                    )
                ),
                dicttype(
                    "tube" => dicttype(
                        "r" => cyl_radius,
                        "h" => cyl_height,
                        "origin" => dicttype(
                            "z" => cyl_height/2 
                        )
                    )
                )
            ]
        )
    end



    semiconductor_geometry_subtractions = []
    begin
        # borehole
        has_borehole = haskey(meta.geometry, :borehole)
        if is_coax && !has_borehole
            error("Coax detectors should have boreholes")
        end
        if has_borehole
            borehole_depth = meta.geometry.borehole.depth_in_mm
            borehole_radius = meta.geometry.borehole.radius_in_mm
            push!(semiconductor_geometry_subtractions, dicttype("cone" => dicttype(
                "r" => borehole_radius,
                "h" => borehole_depth + 2*gap,
                "origin" => [0, 0, is_coax ? borehole_depth/2 - gap : crystal_height - borehole_depth/2 + gap]
            )))
        end
        
        # borehole taper
        has_borehole_taper = haskey(meta.geometry.taper, :borehole)
        if has_borehole_taper
            borehole_taper_height = meta.geometry.taper.borehole.height_in_mm
            if haskey(meta.geometry.taper.borehole, :radius_in_mm)
                borehole_taper_radius = meta.geometry.taper.borehole.radius_in_mm
                borehole_taper_angle = atand(borehole_taper_radius, borehole_taper_height)
            elseif haskey(meta.geometry.taper.borehole, :angle_in_deg)
                borehole_taper_angle = meta.geometry.taper.borehole.angle_in_deg
                borehole_taper_radius = borehole_taper_height * tand(borehole_taper_angle)
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
                r_center = borehole_radius + borehole_taper_radius / 2
                hZ = borehole_taper_height/2
                Δr = hZ * tand(borehole_taper_angle)   
                r_out_bot = r_center - Δr
                r_out_top = r_center + Δr * (1 + 2*gap/hZ)
                push!(semiconductor_geometry_subtractions, dicttype("cone" => dicttype(
                    "r" => dicttype(
                        "bottom" => r_out_bot,
                        "top" => r_out_top
                    ),
                    "h" => 2 * hZ,
                    "origin" => [0, 0, crystal_height - borehole_taper_height / 2 + gap]
                )))
            end
        end

        # top taper
        if haskey(meta.geometry.taper, :top)
            top_taper_height = meta.geometry.taper.top.height_in_mm
            if haskey(meta.geometry.taper.top, :radius_in_mm)
                top_taper_radius = meta.geometry.taper.top.radius_in_mm
                top_taper_angle = atand(top_taper_radius, top_taper_height)
            elseif haskey(meta.geometry.taper.top, :angle_in_deg)
                top_taper_angle = meta.geometry.taper.top.angle_in_deg
                top_taper_radius = top_taper_height * tand(top_taper_angle)
            else
                error("The top taper needs either radius_in_mm or angle_in_deg")
            end
            has_top_taper = top_taper_height > 0 && top_taper_angle > 0
            if has_top_taper
                r_center = crystal_radius - top_taper_radius / 2
                hZ = top_taper_height/2 + 1gap
                h = 2 * hZ
                Δr = hZ * tand(top_taper_angle)         
                r_in_bot = r_center + Δr
                r_in_top = r_center - Δr
                r_out = max(r_in_top, r_in_bot) + gap # ensure that r_out is always bigger as r_in
                push!(semiconductor_geometry_subtractions, dicttype("cone" => dicttype(
                    "r" => dicttype(
                        "bottom" => dicttype(
                            "from" => r_in_bot,
                            "to" => r_out
                        ),
                        "top" => dicttype(
                            "from" => r_in_top,
                            "to" => r_out
                        )
                    ),
                    "h" => h,
                    "origin" => [0, 0, crystal_height - top_taper_height / 2]
                )))
            end
        end

        # bot outer taper
        bot_taper_height = meta.geometry.taper.bottom.height_in_mm
        if :radius_in_mm in keys(meta.geometry.taper.bottom)
            bot_taper_radius = meta.geometry.taper.bottom.radius_in_mm
            bot_taper_angle = atand(bot_taper_radius, bot_taper_height)
        elseif :angle_in_deg in keys(meta.geometry.taper.bottom)
            bot_taper_angle = meta.geometry.taper.bottom.angle_in_deg
            bot_taper_radius = bot_taper_height * tand(bot_taper_angle)
        else
            error("The bottom outer tape needs either radius_in_mm or angle_in_deg")
        end
        has_bot_taper = bot_taper_height > 0 && bot_taper_angle > 0
        if has_bot_taper
            r_center = (has_bottom_cylinder ? cyl_radius : crystal_radius) - bot_taper_radius / 2
            hZ = bot_taper_height/2 + 1gap
            Δr = hZ * tand(bot_taper_angle)         
            r_in_bot = r_center - Δr
            r_in_top = r_center + Δr
            r_out = max(r_in_top, r_in_bot) + gap # ensure that r_out is always bigger as r_in
            push!(semiconductor_geometry_subtractions, dicttype("cone" => dicttype(
                "r" => dicttype(
                        "bottom" => dicttype(
                            "from" => r_in_bot,
                            "to" => r_out
                        ),
                        "top" => dicttype(
                            "from" => r_in_top,
                            "to" => r_out
                        )
                    ),
                "h" => 2 * hZ,
                "origin" => [0, 0, bot_taper_height / 2]
            )))
        end

        # groove
        has_groove = haskey(meta.geometry, :groove)
        if has_groove
            groove_inner_radius = meta.geometry.groove.radius_in_mm.inner
            groove_outer_radius = meta.geometry.groove.radius_in_mm.outer
            groove_depth = meta.geometry.groove.depth_in_mm
            has_groove = groove_outer_radius > 0 && groove_depth > 0 && groove_inner_radius > 0
            if has_groove
                hZ = groove_depth / 2 + gap
                r_in = groove_inner_radius
                r_out = groove_outer_radius
                push!(semiconductor_geometry_subtractions, dicttype("cone" => dicttype(
                    "r" => dicttype(
                        "from" => r_in,
                        "to" => r_out
                    ),
                    "h" => 2 * hZ,
                    "origin" => [0, 0, groove_depth / 2 - gap]
                )))
            end
        end
    end

    if has_top_groove
        top_groove_radius = meta.geometry.extra.topgroove.radius_in_mm
        top_groove_height = meta.geometry.extra.topgroove.depth_in_mm
        push!(semiconductor_geometry_subtractions, dicttype(
                "tube" => dicttype(
                    "r" => top_groove_radius,
                    "h" => top_groove_height + gap,
                    "origin" => dicttype(
                        "z" => crystal_height - top_groove_height/2 + gap/2
                    )
                )
            )
        )
    end

    if isempty(semiconductor_geometry_subtractions)
        config_dict["detectors"][1]["semiconductor"]["geometry"] = semiconductor_geometry_basis
    else
        config_dict["detectors"][1]["semiconductor"]["geometry"] = dicttype(
            "difference" => [semiconductor_geometry_basis, semiconductor_geometry_subtractions...]
        )
    end

    
    
    # bulletization
    # is_bulletized = !all(values(meta.geometry.bulletization) .== 0)
    # is_bulletized && @warn "Bulletization is not implemented yet, ignore for now."

    ### P+ CONTACT ###

    pp_radius = meta.geometry.pp_contact.radius_in_mm
    pp_depth = meta.geometry.pp_contact.depth_in_mm
    push!(config_dict["detectors"][1]["contacts"], dicttype(
        "material" => "HPGe",
        "geometry" => dicttype(),
        "id" => 1,
        "potential" => 0
    ))
    config_dict["detectors"][1]["contacts"][1]["geometry"] = if is_coax
        dicttype("union" => [
            dicttype("cone" => dicttype(
                "r" => dicttype(
                    "from" => borehole_radius,
                    "to" => borehole_radius
                ),
                "h" => borehole_depth,
                "origin" => [0, 0, borehole_depth / 2]
            )),
            dicttype("cone" => dicttype(
                "r" => borehole_radius,
                "h" => 0,
                "origin" => [0, 0, borehole_depth]
            )),
            dicttype("cone" => dicttype(
                "r" => dicttype(
                    "from" => borehole_radius,
                    "to" => pp_radius
                ),
                "h" => 0
            ))
        ])
    else
        dicttype("cone" => dicttype(
            "r" => pp_radius,
            "h" => pp_depth,
            "origin" => [0, 0, pp_depth / 2]
        ))
    end


    ### MANTLE CONTACT ###

    push!(config_dict["detectors"][1]["contacts"], dicttype(
        "material" => "HPGe",
        "geometry" => dicttype("union" => []),
        "id" => 2,
        "potential" => meta.characterization.manufacturer.recommended_voltage_in_V
    ))
    config_dict["detectors"][1]["contacts"][2]["geometry"]["union"] = begin
        mantle_contact_parts = []
        top_plate = begin
            r = if !has_borehole || is_coax
                !has_top_taper ? crystal_radius : crystal_radius - top_taper_radius
            else has_borehole && !is_coax
                r_in = has_top_groove ? top_groove_radius : borehole_radius
                r_out = crystal_radius
                if has_borehole_taper r_in += borehole_taper_radius end
                if has_top_taper r_out -= top_taper_radius end
                dicttype("from" => r_in, "to" => r_out)
            end
            dicttype("cone" => dicttype(
                "r" => r,
                "h" => li_thickness,
                "origin" => [0, 0, crystal_height - li_thickness / 2]
            ))
        end
        push!(mantle_contact_parts, top_plate)
        
        if has_top_groove
            push!(mantle_contact_parts, dicttype(
                    "tube" => dicttype(
                        "r" => dicttype(
                            "from" => top_groove_radius,
                            "to" => top_groove_radius
                        ),
                        "h" => top_groove_height,
                        "origin" => dicttype(
                            "z" => crystal_height - top_groove_height/2
                        )
                    )
                )
            )
            push!(mantle_contact_parts, dicttype(
                    "tube" => dicttype(
                        "r" => dicttype(
                            "from" => borehole_radius,
                            "to" => top_groove_radius
                        ),
                        "h" => 0,
                        "origin" => dicttype(
                            "z" => crystal_height - top_groove_height
                        )
                    )
                )
            )
        end

        if has_top_taper
            Δr_li_thickness = li_thickness / cosd(top_taper_angle)
            h = top_taper_height
            r_bot = crystal_radius 
            r_top = crystal_radius - top_taper_radius
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "bottom" => dicttype(
                        "from" => r_bot - Δr_li_thickness,
                        "to" => r_bot
                    ),
                    "top" => dicttype(
                        "from" => r_top - Δr_li_thickness,
                        "to" => r_top
                    )
                ),
                "h" => h,
                "origin" => [0, 0, crystal_height - top_taper_height / 2]
            )))
        end

        if has_borehole_taper
            Δr_li_thickness = li_thickness / cosd(borehole_taper_angle)
            h = borehole_taper_height    
            r_bot = borehole_radius
            r_top = borehole_radius + borehole_taper_radius
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "bottom" => dicttype(
                        "from" => r_bot,
                        "to" => r_bot + Δr_li_thickness
                    ),
                    "top" => dicttype(
                        "from" => r_top,
                        "to" => r_top + Δr_li_thickness
                    )
                ),
                "h" => h,
                "origin" => [0, 0, crystal_height - borehole_taper_height / 2]
            )))

            h = (borehole_depth - borehole_taper_height)
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "from" => borehole_radius,
                    "to" => borehole_radius + Δr_li_thickness
                ),
                "h" => h,
                "origin" => [0, 0, crystal_height - borehole_taper_height - h / 2]
            )))
        elseif has_borehole && !is_coax # but no borehole taper
            h = borehole_depth
            z_origin = crystal_height - h/2
            if has_top_groove 
                h -= top_groove_height 
                z_origin -= top_groove_height/2
            end
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "from" => borehole_radius,
                    "to" => borehole_radius + li_thickness
                ),
                "h" => h,
                "origin" => [0, 0, z_origin]
            )))
        end

        if has_borehole && !is_coax
            r = borehole_radius + li_thickness
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => r,
                "h" => li_thickness / 2,
                "origin" => [0, 0, crystal_height - borehole_depth - li_thickness / 2]
            )))
        end

        if has_bottom_cylinder

            # upper side contact
            h = upper_height
            if has_top_taper h -= top_taper_height end
            z_origin = cyl_height + cyl_transition + h/2
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "from" => crystal_radius - li_thickness,
                    "to" => crystal_radius
                ),
                "h" => h,
                "origin" => [0, 0, z_origin]
            )))

            # transition contact (ignoring li thickness for now)
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                        "r" => dicttype(
                            "bottom" => dicttype(
                                "from" => cyl_radius,
                                "to" => cyl_radius
                                ),
                            "top" => dicttype(
                                "from" => crystal_radius,
                                "to" => crystal_radius
                            )
                        ),
                        "h" => cyl_transition,
                        "origin" => dicttype(
                            "z" => cyl_height + cyl_transition/2
                        )
                    )
                )
            )

            if has_bot_taper
                # lower side contact
                push!(mantle_contact_parts, dicttype("tube" => dicttype(
                            "r" => dicttype(
                                "from" => cyl_radius - li_thickness,
                                "to" => cyl_radius
                            ),
                            "h" => cyl_height - bot_taper_height,
                            "origin" => dicttype(
                                "z" => cyl_height/2 + bot_taper_height/2
                            )
                        )
                    )
                )

                # bottom plate contact
                push!(mantle_contact_parts, dicttype(
                        "cone" => dicttype(
                            "r" => dicttype(
                                "bottom" => dicttype(
                                    "from" => cyl_radius - bot_taper_radius,
                                    "to" => cyl_radius - bot_taper_radius
                                ),
                                "top" => dicttype(
                                    "from" => cyl_radius,
                                    "to" => cyl_radius
                                )
                            ),
                            "h" => bot_taper_height,
                            "origin" => [0, 0, bot_taper_height / 2]
                        )
                    )
                )

                # bottom plate contact
                push!(mantle_contact_parts, dicttype(
                        "tube" => dicttype(
                            "r" => dicttype(
                                "from" => groove_outer_radius,
                                "to" => cyl_radius - bot_taper_radius
                            ),
                            "h" => li_thickness,
                            "origin" => [0, 0, li_thickness / 2]
                        )
                    )
                )

            else
            # lower side contact
            push!(mantle_contact_parts, dicttype("tube" => dicttype(
                        "r" => dicttype(
                            "from" => cyl_radius - li_thickness,
                            "to" => cyl_radius
                        ),
                        "h" => cyl_height,
                        "origin" => dicttype(
                            "z" => cyl_height/2
                        )
                    )
                )
            )

            # bottom plate contact
            push!(mantle_contact_parts, dicttype(
                    "tube" => dicttype(
                        "r" => dicttype(
                            "from" => groove_outer_radius,
                            "to" => cyl_radius
                        ),
                        "h" => li_thickness,
                        "origin" => [0, 0, li_thickness / 2]
                    )
                )
            )
            end

        else 

            h = crystal_height
            if has_top_taper h -= top_taper_height end
            z_origin = h/2
            if has_bot_taper 
                h -= bot_taper_height 
                z_origin += bot_taper_height/2
            end
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "from" => crystal_radius - li_thickness,
                    "to" => crystal_radius
                ),
                "h" => h,
                "origin" => [0, 0, z_origin]
            )))

            if has_bot_taper
                Δr_li_thickness = li_thickness / cosd(bot_taper_angle)
                h = bot_taper_height
                r_bot = crystal_radius - bot_taper_radius
                r_top = crystal_radius
                push!(mantle_contact_parts, dicttype("cone" => dicttype(
                    "r" => dicttype(
                        "bottom" => dicttype(
                            "from" => r_bot - Δr_li_thickness,
                            "to" => r_bot
                        ),
                        "top" => dicttype(
                            "from" => r_top - Δr_li_thickness,
                            "to" => r_top
                        )
                    ),
                    "h" => h,
                    "origin" => [0, 0, h / 2]
                )))
            end

            if has_groove && groove_outer_radius > 0
                r_in = groove_outer_radius 
                r_out = crystal_radius
                if has_bot_taper r_out -= bot_taper_radius end
                push!(mantle_contact_parts, dicttype("cone" => dicttype(
                    "r" => dicttype(
                        "from" => r_in,
                        "to" => r_out
                    ),
                    "h" => li_thickness,
                    "origin" => [0, 0, li_thickness / 2]
                )))
            end
        end

        mantle_contact_parts
    end

 
    config_dict["detectors"][1]["semiconductor"]["impurity_density"] = dicttype(
        "name" => "constant", 
        "value" => "-1e9cm^-3"
    )

    # extras
    if haskey(meta.geometry, :extra) 
        if haskey(meta.geometry.extra, :crack)
            
            # cut the crack volume from the semiconductor
            radius_crack = meta.geometry.extra.crack.radius_in_mm
            α = meta.geometry.extra.crack.angle_in_deg

            crack = dicttype(
                "translate" => dicttype(
                    "box" => dicttype(
                        "widths" => [100,200,200],
                        "rotation" => dicttype("Y" => α),
                    ),
                    "x" => crystal_radius - radius_crack + 50*cosd(α),
                    "z" => -50*sind(α)
                )
            )
            push!(config_dict["detectors"][1]["semiconductor"]["geometry"]["difference"], crack)

            # cut the crack from the mantle contact 
            semiconductor_volume = config_dict["detectors"][1]["semiconductor"]["geometry"]

            config_dict["detectors"][1]["contacts"][2]["geometry"] = dicttype(
                "union" => [
                    dicttype(
                        "difference" => vcat(
                            config_dict["detectors"][1]["contacts"][2]["geometry"],
                            # cut out from the contact
                            dicttype(
                                "translate" => dicttype(
                                    "box" => dicttype(
                                        "widths" => [100,200,200],
                                        "rotation" => dicttype("Y" => α),
                                    ),
                                    "x" => crystal_radius - radius_crack + 50*cosd(α),
                                    "z" => -50*sind(α)
                                )
                            )
                        )
                    ),
                    #addition to the contact 
                    dicttype(
                        "intersection" => [
                            dicttype(
                                "translate" => dicttype(
                                    "box" => dicttype(
                                        "widths" => [0,200,200],
                                        "rotation" => dicttype("Y" => α),
                                    ),
                                    "x" => crystal_radius - radius_crack,
                                )
                            ),
                            semiconductor_volume
                        ]
                    )
                ]
            )

            # make sure that the simulation is performed in 3D
            config_dict["grid"]["axes"]["phi"]["to"] = 360
        end
    end
    

    # evaluate "include" statements - needed for the charge drift model
    SolidStateDetectors.scan_and_merge_included_json_files!(config_dict, "")

    return config_dict
end

end # module LegendDataManagementSolidStateDetectorsExt
