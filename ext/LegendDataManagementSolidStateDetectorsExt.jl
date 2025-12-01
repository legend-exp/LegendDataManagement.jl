# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

module LegendDataManagementSolidStateDetectorsExt

using SolidStateDetectors
using LegendDataManagement
using Unitful
using PropDicts
using OrderedCollections
using YAML

const _SSDDefaultNumtype = Float32
const DEFAULT_OPERATIONAL_VOLTAGE_IN_V = 5000
const DEFAULT_N_THICKNESS_IN_MM = 1.0
const DEFAULT_P_THICKNESS_IN_MM = 0.1

"""
    SolidStateDetector{T<:AbstractFloat}(data::LegendData, detector::DetectorIdLike; kwargs...)
    SolidStateDetector{T<:AbstractFloat}(::Type{LegendData}, diode_filename::String; kwargs...)
    SolidStateDetector{T<:AbstractFloat}(::Type{LegendData}, diode_filename::String, xtal_filename::String; kwargs...)
    SolidStateDetector{T<:AbstractFloat}(::Type{LegendData}, diode_meta::PropDict, xtal_meta::Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}; kwargs...)

LegendDataManagement provides an extension for SolidStateDetectors, a
`SolidStateDetector` can be constructed from LEGEND metadata using the
methods above. Uses LEGEND defaults, such as ADLChargeDriftModel2016.

## Arguments
* `data::LegendData`: LEGEND data structure containing metadata.
* `detector::DetectorIdLike`: Identifier for the detector, in the form of a symbol.
* `diode_filename::String`: Path to the diode metadata file.
* `xtal_filename::String`: Path to the crystal metadata file.
* `diode_meta::PropDict`: Diode metadata as a `PropDict`.
* `xtal_meta::Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}`: Crystal metadata as a `PropDict` or `NoSuchPropsDBEntry` if not available.
* `env::HPGeEnvironment`: Environment configuration for the detector. Default is `HPGeEnvironment()`: Vacuum and 77 K.

## Keywords
* `operational_voltage::Number`: Operational voltage for the n+ contact. Accepts units, if non are given will interpret as `V`. If not provided, it will be taken from the metadata if available or defaulted.
* `n_thickness::Number`: Thickness of the n+ contact in mm. Accepts units, if non are given will interpret as `mm`. If not provided, it will be taken from the metadata if available or defaulted.
* `verbose::Bool`: Whether to print detailed information during the creation process. Default is `true`.
* `save_ssd_config::Bool`: Whether to save the SSD configuration to a YAML file. Default is `false`.
"""

function SolidStateDetectors.SolidStateDetector(data::LegendData, detector::DetectorIdLike, env::HPGeEnvironment = HPGeEnvironment(); kwargs...)
    SolidStateDetector{_SSDDefaultNumtype}(data, detector, env; kwargs...)
end

function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, diode_filename::String, env::HPGeEnvironment = HPGeEnvironment(); kwargs...) where {T<:AbstractFloat}
    diode_meta = readlprops(diode_filename)
    SolidStateDetector{T}(LegendData, diode_meta, LegendDataManagement.NoSuchPropsDBEntry("",[]), env; kwargs...)
end

function SolidStateDetectors.SolidStateDetector{T}(data::LegendData, detector::DetectorIdLike, env::HPGeEnvironment = HPGeEnvironment(); kwargs...) where {T<:AbstractFloat}
    detector_props = getproperty(data.metadata.hardware.detectors.germanium.diodes, Symbol(detector))
    xtal_props = getproperty(data.metadata.hardware.detectors.germanium.crystals, Symbol(string(detector)[1:end-1]))
    SolidStateDetector{T}(LegendData, detector_props, xtal_props, env; kwargs...)
end

function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, diode_filename::String, xtal_filename::String, env::HPGeEnvironment = HPGeEnvironment(); kwargs...) where {T<:AbstractFloat}
    diode_meta = readlprops(diode_filename)
    xtal_meta = readlprops(xtal_filename)
    SolidStateDetector{T}(LegendData, diode_meta, xtal_meta, env; kwargs...)
end

function SolidStateDetectors.SolidStateDetector{T}(::Type{LegendData}, diode_meta::PropDict, xtal_meta::Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}, env::HPGeEnvironment = HPGeEnvironment(); save_ssd_config::Bool = false, kwargs...) where {T<:AbstractFloat}
    if xtal_meta isa LegendDataManagement.NoSuchPropsDBEntry
        @warn "Crystal metadata not provided. No impurity density information will be passed to the simulation."
    end
    config_dict = create_SSD_config_dict_from_LEGEND_metadata(diode_meta, xtal_meta, env; kwargs...)
    if save_ssd_config YAML.write_file(config_dict["name"] * "_ssd_config.yaml", config_dict) end
    SolidStateDetector{T}(config_dict, SolidStateDetectors.construct_units(config_dict))
end

"""
    Simulation{T<:AbstractFloat}(data::LegendData, detector::DetectorIdLike; kwargs...)
    Simulation{T<:AbstractFloat}(::Type{LegendData}, diode_filename::String; kwargs...)
    Simulation{T<:AbstractFloat}(::Type{LegendData}, diode_filename::String, xtal_filename::String; kwargs...)
    Simulation{T<:AbstractFloat}(::Type{LegendData}, diode_meta::PropDict, xtal_meta::Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}; kwargs...)

LegendDataManagement provides an extension for SolidStateDetectors, a
`Simulation` can be constructed from LEGEND metadata using the
methods above. Uses LEGEND defaults, such as ADLChargeDriftModel2016.

## Arguments
* `data::LegendData`: LEGEND data structure containing metadata.
* `detector::DetectorIdLike`: Identifier for the detector, in the form of a symbol.
* `diode_filename::String`: Path to the diode metadata file.
* `xtal_filename::String`: Path to the crystal metadata file.
* `diode_meta::PropDict`: Diode metadata as a `PropDict`.
* `xtal_meta::Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}`: Crystal metadata as a `PropDict` or `NoSuchPropsDBEntry` if not available.
* `env::HPGeEnvironment`: Environment configuration for the detector. Default is `HPGeEnvironment()`: Vacuum and 77 K.

## Keywords
* `operational_voltage::Number`: Operational voltage for the n+ contact. Accepts units, if non are given will interpret as `V`. If not provided, it will be taken from the metadata if available or defaulted.
* `n_thickness::Number`: Thickness of the n+ contact in mm. Accepts units, if non are given will interpret as `mm`. If not provided, it will be taken from the metadata if available or defaulted.
* `verbose::Bool`: Whether to print detailed information during the creation process. Default is `true`.

"""
function SolidStateDetectors.Simulation(data::LegendData, detector::DetectorIdLike, env::HPGeEnvironment = HPGeEnvironment(); kwargs...)
    Simulation{_SSDDefaultNumtype}(data, detector, env; kwargs...)
end

function SolidStateDetectors.Simulation{T}(data::LegendData, detector::DetectorIdLike, env::HPGeEnvironment = HPGeEnvironment(); kwargs...) where {T<:AbstractFloat}
    detector_props = getproperty(data.metadata.hardware.detectors.germanium.diodes, Symbol(detector))
    xtal_props = getproperty(data.metadata.hardware.detectors.germanium.crystals, Symbol(string(detector)[1:end-1]))
    Simulation{T}(LegendData, detector_props, xtal_props, env; kwargs...)
end

function SolidStateDetectors.Simulation{T}(::Type{LegendData}, diode_filename::String, env::HPGeEnvironment = HPGeEnvironment(); kwargs...) where {T<:AbstractFloat}
    diode_meta = readlprops(diode_filename)
    Simulation{T}(LegendData, diode_meta, LegendDataManagement.NoSuchPropsDBEntry("",[]), env; kwargs...)
end

function SolidStateDetectors.Simulation{T}(::Type{LegendData}, diode_filename::String, xtal_filename::String, env::HPGeEnvironment = HPGeEnvironment(); kwargs...) where {T<:AbstractFloat}
    diode_meta = readlprops(diode_filename)
    xtal_meta = readlprops(xtal_filename)
    Simulation{T}(LegendData, diode_meta, xtal_meta, env; kwargs...)
end

function SolidStateDetectors.Simulation{T}(::Type{LegendData}, diode_meta::PropDict, xtal_meta::Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}, env::HPGeEnvironment = HPGeEnvironment(); save_ssd_config::Bool = false, kwargs...) where {T<:AbstractFloat}
    if xtal_meta isa LegendDataManagement.NoSuchPropsDBEntry
        @warn "Crystal metadata not provided. No impurity density information will be passed to the simulation."
    end
    config_dict = create_SSD_config_dict_from_LEGEND_metadata(diode_meta, xtal_meta, env; kwargs...)
    if save_ssd_config YAML.write_file(config_dict["name"] * "_ssd_config.yaml", config_dict) end
    Simulation{T}(config_dict)
end

get_unicode_rep(s::String) = get_unicode_rep(Val(Symbol(s)))

function get_unicode_rep(::Val{:icpc})
    "╭───╮ ╭───╮", 
    "│   │ │   │",
    "│   │ │   │",
    "│   ╰─╯   │",
    "│         │",
    "╰── ─── ──╯"
end

function get_unicode_rep(::Val{:bege})
    "           ",
    "╭─────────╮", 
    "│         │",
    "╰── ─── ──╯",
    "           ",
    "           "
end

function get_unicode_rep(::Val{:ppc})
    "           ",
    "╭─────────╮", 
    "│         │",
    "│         │",
    "╰    .    ╯",
    "           "
end

function get_unicode_rep(::Val{:coax})
    "╭─────────╮", 
    "│   ╭─╮   │",
    "│   │ │   │",
    "│   │ │   │",
    "│   │ │   │",
    "╰───╯ ╰───╯"
end

function create_SSD_config_dict_from_LEGEND_metadata(diode_meta::PropDict, xtal_meta::X, env::HPGeEnvironment = HPGeEnvironment(); 
    dicttype = OrderedDict{String,Any}, verbose::Bool = true, operational_voltage::Number = NaN, n_thickness::Number = NaN) where {X <: Union{PropDict, LegendDataManagement.NoSuchPropsDBEntry}}

    # Not all possible configurations are yet implemented!
    gap = 1.0 # to ensure negative volumes do not match at surfaces

    dl_thickness_in_mm, dl_val_used = if ustrip(n_thickness) >= 0
        n_thickness isa Unitful.Quantity ? ustrip(u"mm", n_thickness) : n_thickness, "✔ n⁺contact thickness (user):"
    elseif hasproperty(diode_meta.characterization, :combined_0vbb_analysis) && diode_meta.characterization.combined_0vbb_analysis.fccd_in_mm.value > 0
        diode_meta.characterization.combined_0vbb_analysis.fccd_in_mm.value, "✔ n⁺contact thickness (0νββ analysis):"
    elseif hasproperty(diode_meta.characterization.manufacturer, :dl_thickness_in_mm) && diode_meta.characterization.manufacturer.dl_thickness_in_mm > 0
        diode_meta.characterization.manufacturer.dl_thickness_in_mm, "✔ n⁺contact thickness (manufacturer):"
    else
        DEFAULT_N_THICKNESS_IN_MM, "⚠ n⁺contact thickness (DEFAULT):"
    end
    
    li_thickness =  dl_thickness_in_mm
    pp_thickness = DEFAULT_P_THICKNESS_IN_MM

    crystal_radius = diode_meta.geometry.radius_in_mm
    crystal_height = diode_meta.geometry.height_in_mm

    pp_radius = diode_meta.geometry.pp_contact.radius_in_mm
    pp_depth = diode_meta.geometry.pp_contact.depth_in_mm
    
    is_coax = diode_meta.type == "coax"

    config_dict = dicttype(
        "name" => diode_meta.name,
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
                    "boundaries" => "reflecting"
                ),
                "z" => dicttype(
                    "from" => -0.2 * crystal_height,
                    "to" => 1.2 * crystal_height,
                    "boundaries" => "inf"
                )
            )
        ),
        "medium" => env.medium,
        "detectors" => []
    )

    push!(config_dict["detectors"], dicttype(
        "semiconductor" => dicttype(
            "material" => "HPGe",
            "charge_drift_model" => dicttype(
                "include" => joinpath(SolidStateDetectors.get_path_to_example_config_files(), "ADLChargeDriftModel", "drift_velocity_config_2016.yaml"), #change to 2016
            ),
            # "impurity_density" => dicttype("parameters" => Vector()),
            "geometry" => dicttype(),
            "temperature" => ustrip(u"K", env.temperature)
        ),
        "contacts" => []
        ))
    
    # main crystal
    semiconductor_geometry_basis = dicttype("cone" => dicttype(
        "r" => crystal_radius,
        "h" => crystal_height,
        "origin" => [0, 0, crystal_height / 2]
    ))
    semiconductor_geometry_subtractions = []
    begin
        # borehole
        has_borehole = hasproperty(diode_meta.geometry, :borehole)
        if is_coax && !has_borehole
            error("Coax detectors should have boreholes")
        end
        if has_borehole
            borehole_depth = diode_meta.geometry.borehole.depth_in_mm
            borehole_radius = diode_meta.geometry.borehole.radius_in_mm
            push!(semiconductor_geometry_subtractions, dicttype("cone" => dicttype(
                "r" => borehole_radius,
                "h" => borehole_depth + 2*gap,
                "origin" => [0, 0, is_coax ? borehole_depth/2 - gap : crystal_height - borehole_depth/2 + gap]
            )))
        end

        ## pp dimple
        if pp_depth > 0
            push!(semiconductor_geometry_subtractions, dicttype("cone" => dicttype(
                "r" => pp_radius,
                "h" => pp_depth + 2*gap,
                "origin" => [0, 0, pp_depth / 2 - gap]
            )))
        end
        
        # borehole taper
        has_borehole_taper = hasproperty(diode_meta.geometry.taper, :borehole)
        if has_borehole_taper
            borehole_taper_height = diode_meta.geometry.taper.borehole.height_in_mm
            if hasproperty(diode_meta.geometry.taper.borehole, :radius_in_mm)
                borehole_taper_radius = diode_meta.geometry.taper.borehole.radius_in_mm
                borehole_taper_angle = atand(borehole_taper_radius, borehole_taper_height)
            elseif hasproperty(diode_meta.geometry.taper.borehole, :angle_in_deg)
                borehole_taper_angle = diode_meta.geometry.taper.borehole.angle_in_deg
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
                hZ = borehole_taper_height + 2*gap
                Δr = hZ * tand(borehole_taper_angle)         
                r_out_bot = borehole_radius
                r_out_top = borehole_radius + Δr
                push!(semiconductor_geometry_subtractions, dicttype("cone" => dicttype(
                    "r" => dicttype(
                        "bottom" => r_out_bot,
                        "top" => r_out_top
                    ),
                    "h" => hZ,
                    "origin" => [0, 0, crystal_height - borehole_taper_height/2 + gap]
                )))
            end
        end

        # top taper
        if hasproperty(diode_meta.geometry.taper, :top)
            top_taper_height = diode_meta.geometry.taper.top.height_in_mm
            if hasproperty(diode_meta.geometry.taper.top, :radius_in_mm)
                top_taper_radius = diode_meta.geometry.taper.top.radius_in_mm
                top_taper_angle = atand(top_taper_radius, top_taper_height)
            elseif hasproperty(diode_meta.geometry.taper.top, :angle_in_deg)
                top_taper_angle = diode_meta.geometry.taper.top.angle_in_deg
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
        bot_taper_height = diode_meta.geometry.taper.bottom.height_in_mm
        if hasproperty(diode_meta.geometry.taper.bottom, :radius_in_mm)
            bot_taper_radius = diode_meta.geometry.taper.bottom.radius_in_mm
            bot_taper_angle = atand(bot_taper_radius, bot_taper_height)
        elseif hasproperty(diode_meta.geometry.taper.bottom, :angle_in_deg)
            bot_taper_angle = diode_meta.geometry.taper.bottom.angle_in_deg
            bot_taper_radius = bot_taper_height * tand(bot_taper_angle)
        else
            error("The bottom outer tape needs either radius_in_mm or angle_in_deg")
        end
        has_bot_taper = bot_taper_height > 0 && bot_taper_angle > 0
        if has_bot_taper
            r_center = crystal_radius - bot_taper_radius / 2
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
        has_groove = hasproperty(diode_meta.geometry, :groove)
        if has_groove
            groove_inner_radius = diode_meta.geometry.groove.radius_in_mm.inner
            groove_outer_radius = diode_meta.geometry.groove.radius_in_mm.outer
            groove_depth = diode_meta.geometry.groove.depth_in_mm
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

    if isempty(semiconductor_geometry_subtractions)
        config_dict["detectors"][1]["semiconductor"]["geometry"] = semiconductor_geometry_basis
    else
        config_dict["detectors"][1]["semiconductor"]["geometry"] = dicttype(
            "difference" => [semiconductor_geometry_basis, semiconductor_geometry_subtractions...]
        )
    end

    
    
    # bulletization
    # is_bulletized = !all(values(diode_meta.geometry.bulletization) .== 0)
    # is_bulletized && @warn "Bulletization is not implemented yet, ignore for now."

    # extras
    hasproperty(diode_meta.geometry, :extra) && @warn "Extras are not implemented yet, ignore for now."


    ### P+ CONTACT ###

    push!(config_dict["detectors"][1]["contacts"], dicttype(
        "material" => "HPGe",
        "name" => "p+ contact",
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
        if pp_depth > 0
            dicttype("union" => [
                dicttype("cone" => dicttype(
                    "r" => pp_radius + pp_thickness,
                    "h" => pp_thickness,
                    "origin" => [0, 0, pp_depth + pp_thickness / 2]
                )),
                dicttype("cone" => dicttype(
                    "r" => dicttype(
                        "from" => pp_radius,
                        "to" => pp_radius + pp_thickness
                    ),
                    "h" => pp_depth,
                    "origin" => [0, 0, pp_depth / 2]
                ))
            ])
        else
            dicttype("cone" => dicttype(
                "r" => pp_radius,
                "h" => pp_thickness,
                "origin" => [0, 0, pp_thickness / 2]
            ))
        end
    end


    ### MANTLE CONTACT ###
    Vop, Vop_val_used = if ustrip(operational_voltage) >= 0
        operational_voltage isa Unitful.Quantity ? ustrip(u"V", operational_voltage) : operational_voltage, "✔ Operational voltage (user):"
    elseif hasproperty(diode_meta.characterization.l200_site, :recommended_voltage_in_V) && diode_meta.characterization.l200_site.recommended_voltage_in_V > 0
        diode_meta.characterization.l200_site.recommended_voltage_in_V,  "✔ Operational voltage (L200 characterization):"
    elseif hasproperty(diode_meta.characterization.manufacturer, :recommended_voltage_in_V) && diode_meta.characterization.manufacturer.recommended_voltage_in_V > 0
        diode_meta.characterization.manufacturer.recommended_voltage_in_V,  "✔ Operational voltage (manufacturer):"
    else
        DEFAULT_OPERATIONAL_VOLTAGE_IN_V,   "⚠ Operational voltage (DEFAULT):"
    end

    push!(config_dict["detectors"][1]["contacts"], dicttype(
        "material" => "HPGe",
        "name" => "n+ contact",
        "geometry" => dicttype("union" => []),
        "id" => 2,
        "potential" => Vop
    ))
    config_dict["detectors"][1]["contacts"][2]["geometry"]["union"] = begin
        mantle_contact_parts = []
        top_plate = begin
            r = if !has_borehole || is_coax
                !has_top_taper ? crystal_radius : crystal_radius - top_taper_radius
            else has_borehole && !is_coax
                r_in = borehole_radius
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
            h =  Δr_li_thickness / tand(top_taper_angle)
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "top" => dicttype(
                        "from" => r_bot - Δr_li_thickness,
                        "to" => r_bot
                    ),
                    "bottom" => dicttype(
                        "from" => r_bot,
                        "to" => r_bot
                    )
                ),
                "h" => h,
                "origin" => [0, 0, crystal_height - top_taper_height - h / 2]
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
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "from" => borehole_radius,
                    "to" => borehole_radius + li_thickness
                ),
                "h" => h,
                "origin" => [0, 0, crystal_height - h / 2]
            )))
        end

        if has_borehole && !is_coax
            r = borehole_radius + li_thickness
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => r,
                "h" => li_thickness,
                "origin" => [0, 0, crystal_height - borehole_depth - li_thickness / 2]
            )))
        end

        begin
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
        end

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
            h =  Δr_li_thickness / tand(bot_taper_angle)
            push!(mantle_contact_parts, dicttype("cone" => dicttype(
                "r" => dicttype(
                    "bottom" => dicttype(
                        "from" => r_top - Δr_li_thickness,
                        "to" => r_top
                    ),
                    "top" => dicttype(
                        "from" => r_top,
                        "to" => r_top
                    )
                ),
                "h" => h,
                "origin" => [0, 0, bot_taper_height + h / 2]
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
        
        mantle_contact_parts
    end
    
    slice = Symbol(diode_meta.name[end])
    config_dict["detectors"][1]["semiconductor"]["impurity_density"] = if hasproperty(xtal_meta,:impurity_curve) && hasproperty(xtal_meta.slices, slice)
        impurity_scale =  hasproperty(xtal_meta.impurity_curve.corrections, :scale) ? xtal_meta.impurity_curve.corrections.scale : 1.0
        impurity_offset = hasproperty(xtal_meta.impurity_curve.corrections, :offset) ? xtal_meta.impurity_curve.corrections.offset * -1e6 : 0.0 ## 1e9cm^-3 -> mm^-3
        impurity_corrections_dict = dicttype(
            "scale" => impurity_scale, 
            "offset" => impurity_offset,
        )
        if xtal_meta.impurity_curve.model == "constant_boule"
            dicttype(
                "name" => "constant", 
                "value" => xtal_meta.impurity_curve.parameters.value * -1e6, ## 1e9cm^-3 -> mm^-3
                "corrections" => impurity_corrections_dict
            )
        elseif xtal_meta.impurity_curve.model == "linear_boule"
            dicttype(
                "name" => xtal_meta.impurity_curve.model, 
                "a" => xtal_meta.impurity_curve.parameters.a * -1e6, ## 1e9cm^-3 -> mm^-3
                "b" => xtal_meta.impurity_curve.parameters.b * -1e6, ## 1e9cm^-3 * mm^-1 -> mm^-4
                "det_z0" => xtal_meta.slices[slice].detector_offset_in_mm, ## already in mm
                "corrections" => impurity_corrections_dict
            )
        elseif xtal_meta.impurity_curve.model == "parabolic_boule"
            dicttype(
                "name" => xtal_meta.impurity_curve.model, 
                "a" => xtal_meta.impurity_curve.parameters.a * -1e6, ## 1e9cm^-3 -> mm^-3
                "b" => xtal_meta.impurity_curve.parameters.b * -1e6, ## 1e9cm^-3 * mm^-1 -> mm^-4
                "c" => xtal_meta.impurity_curve.parameters.c * -1e6, ## 1e9cm^-3 * mm^-2 -> mm^-5
                "det_z0" => xtal_meta.slices[slice].detector_offset_in_mm, ## already in mm
                "corrections" => impurity_corrections_dict
            )
        elseif xtal_meta.impurity_curve.model == "linear_exponential_boule"
            dicttype(
                "name" => xtal_meta.impurity_curve.model, 
                "a" => xtal_meta.impurity_curve.parameters.a * -1e6, ## 1e9cm^-3 -> mm^-3
                "b" => xtal_meta.impurity_curve.parameters.b * -1e6, ## 1e9cm^-3 * mm^-1 -> mm^-4
                "n" => xtal_meta.impurity_curve.parameters.n * -1e6, ## 1e9cm^-3 -> mm^-3
                "l" => xtal_meta.impurity_curve.parameters.l, ## already in mm
                "m" => xtal_meta.impurity_curve.parameters.m, ## already in mm
                "det_z0" => xtal_meta.slices[slice].detector_offset_in_mm, ## already in mm
                "corrections" => impurity_corrections_dict
            )
        elseif xtal_meta.impurity_curve.model == "parabolic_exponential_boule"
            dicttype(
                "name" => xtal_meta.impurity_curve.model, 
                "a" => xtal_meta.impurity_curve.parameters.a * -1e6, ## 1e9cm^-3 -> mm^-3
                "b" => xtal_meta.impurity_curve.parameters.b * -1e6, ## 1e9cm^-3 * mm^-1 -> mm^-4
                "c" => xtal_meta.impurity_curve.parameters.c * -1e6, ## 1e9cm^-3 * mm^-2 -> mm^-5
                "n" => xtal_meta.impurity_curve.parameters.n * -1e6, ## 1e9cm^-3 -> mm^-3
                "l" => xtal_meta.impurity_curve.parameters.l, ## already in mm
                "m" => xtal_meta.impurity_curve.parameters.m, ## already in mm
                "det_z0" => xtal_meta.slices[slice].detector_offset_in_mm, ## already in mm
                "corrections" => impurity_corrections_dict
            )
        end
    else
        dicttype(
            "name" => "constant", 
            "value" => 0,
        )
    end
    if verbose
        imp_model = config_dict["detectors"][1]["semiconductor"]["impurity_density"]["name"]
        imp_val = if hasproperty(xtal_meta,:impurity_curve) && hasproperty(xtal_meta.slices, slice)
            join(["$k: $v" for (k, v) in xtal_meta.impurity_curve.parameters], ", ")
        else
            "value: 0"
        end
        det_offset = hasproperty(xtal_meta.slices, slice) ? xtal_meta.slices[slice].detector_offset_in_mm*u"mm" : "unknown"
        imp_warn = hasproperty(xtal_meta,:impurity_curve) && hasproperty(xtal_meta.slices, slice) ? ("✔", "") : ("⚠","(DEFAULT)")
        imp_scale = hasproperty(xtal_meta.impurity_curve.corrections, :scale) ? xtal_meta.impurity_curve.corrections.scale : "-"
        imp_offset = hasproperty(xtal_meta.impurity_curve.corrections, :offset) ? xtal_meta.impurity_curve.corrections.offset : "-"
        g1,g2,g3,g4,g5,g6 = get_unicode_rep(diode_meta.type)
        vol = round(typeof(1u"cm^3"), LegendDataManagement.get_active_volume(diode_meta, Val(Symbol(diode_meta.type)), .0))
        actvol = round(typeof(1u"cm^3"), LegendDataManagement.get_active_volume(diode_meta, Val(Symbol(diode_meta.type)), 1.0*li_thickness))
        actvol_check = actvol == vol ? "⚠" : "✔"
        @info """
        Legend SolidStateDetector - $(diode_meta.name)
        $g1  ╰─ $Vop_val_used $(Vop isa Rational ? float(Vop) : Vop) V
        $g2  ╰─ $dl_val_used $(dl_thickness_in_mm isa Rational ? float(dl_thickness_in_mm) : dl_thickness_in_mm) mm
        $g3  ╰─ $(imp_warn[1]) Impurity model $(imp_warn[2]) / Detector Offset: $imp_model / $det_offset
        $g4     ╰─ $imp_val
        $g5     ╰─ Corrections: Scale / Offset: $imp_scale / $imp_offset
        $g6  ╰─ $actvol_check Volume / Active volume: $vol / $actvol
        """
    end

    # evaluate "include" statements - needed for the charge drift model
    SolidStateDetectors.scan_and_merge_included_json_files!(config_dict, "")
    return config_dict
end

end # module LegendDataManagementSolidStateDetectorsExt
