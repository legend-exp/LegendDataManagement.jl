# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

# These are function needed to calculate the active volume based on 
# LEGEND detector metadata and measured dead layer thicknesses


@inline get_truncated_cone_volume(rin, rout, h) = π * h/3 * (rout^2 + rout*rin + rin^2) - π * h * rin^2
@inline get_inner_taper_volume(r1, r2, h) = get_truncated_cone_volume(extrema((r1, r2))..., h)
@inline get_outer_taper_volume(r1, r2, h) = -get_truncated_cone_volume(reverse(extrema((r1, r2)))..., h)

function get_extra_volume(geometry::PropDict, ::Val{:crack}, fccd::T) where {T <: AbstractFloat}
    # Find a picture of the definition of crack here:
    # https://github.com/legend-exp/legend-metadata/blob/archived/hardware/detectors/detector-metadata_5.pdf
    r = geometry.radius_in_mm - fccd
    H = geometry.height_in_mm - 2*fccd
    alpha = geometry.extra.crack.angle_in_deg
    p0 = geometry.extra.crack.radius_in_mm + fccd * (secd(alpha) - tand(alpha) - 1)
    return if iszero(alpha)
        # Vertical crack
        (r^2 * acos(1 - p0/r) - sqrt(2r*p0 - p0^2) * (r - p0)) * H
    else 
        # Inclined crack
        t = max(p0 - H * tand(alpha), p0 * 0)
        int11 = (1 - t/r) * acos(1 - t/r) - sqrt(1 - (1 - t/r)^2)
        int12 = (1 - p0/r) * acos(1 - p0/r) - sqrt(1 - (1 - p0/r)^2)
        -cotd(alpha) * (r^3 * (int12 - int11) + ((2*r*p0 - p0^2)^(3/2) - (2*r*t - t^2)^(3/2))/ 3)
    end
end

function get_extra_volume(geometry::PropDict, ::Val{:topgroove}, fccd::AbstractFloat)
    # Find a picture of the definition of topgroove here:
    # https://github.com/legend-exp/legend-metadata/blob/archived/hardware/detectors/detector-metadata_4.pdf
    rb = geometry.borehole.radius_in_mm
    db = geometry.borehole.radius_in_mm
    rg = geometry.extra.topgroove.radius_in_mm
    dg = geometry.extra.topgroove.depth_in_mm
    db <= dg && @warn "The depth of the borehole ($(db)mm) should be bigger than the depth of the topgroove ($(dg)mm)."
    return π * ((rg + fccd)^2 - (rb + fccd)^2) * dg
end

function get_extra_volume(geometry::PropDict, ::Val{:bottom_cylinder}, fccd::AbstractFloat)
    # Find a picture of the definition of bottom_cylinder here:
    # https://github.com/legend-exp/legend-metadata/blob/archived/hardware/detectors/detector-metadata_6.pdf
    r = geometry.extra.bottom_cylinder.radius_in_mm - fccd
    h = geometry.extra.bottom_cylinder.height_in_mm - fccd
    t = geometry.extra.bottom_cylinder.transition_in_mm
    R = geometry.radius_in_mm - fccd
    return get_outer_taper_volume(R, r, t) + π * h * (R^2 - r^2)
end

function get_extra_volume(geometry::PropDict, fccd::T = .0) where {T <: AbstractFloat}
    if isa(geometry.extra, PropDicts.MissingProperty)
        return zero(T)
    else
        return get_extra_volume(geometry, Val(first(keys(geometry.extra))), fccd)
    end
end

# @inline get_mass(volume::U, enrichment::V) where {U <: AbstractFloat, V <: AbstractFloat} = 
#     volume / 1000 * (5.327 * enrichment * 76/72 + 5.327 * (1 - enrichment))

# Detector specific active volume calculation
function get_active_volume(pd::PropDict, ::Val{:bege}, fccd::T = .0) where {T <: AbstractFloat}

    g = pd.geometry
    
    R = g.radius_in_mm - fccd
    H = g.height_in_mm - 2 * fccd

    # Groove
    groove_volume = π * g.groove.depth_in_mm * (g.groove.radius_in_mm.outer^2 - g.groove.radius_in_mm.inner^2)
    
    # Top taper
    α = g.taper.top.angle_in_deg / 360 * 2π
    h = g.taper.top.height_in_mm + fccd * (1 - sin(α) - cos(α))/sin(α)
    x = h * tan(α)
    taper_top_volume = iszero(x) || !isfinite(x) ? zero(T) : get_outer_taper_volume(R, R - x, h)
    
    # Bottom taper
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    h = g.taper.bottom.height_in_mm + fccd * (1 - sin(α) - cos(α))/sin(α)
    x = h * tan(α)
    taper_bottom_volume = iszero(x) || !isfinite(x) ? zero(T) : get_outer_taper_volume(R, R - x, h)

    # p+ contact
    rp = g.pp_contact.radius_in_mm
    dp = g.pp_contact.depth_in_mm
    pp_volume = π * rp^2 * dp
    
    return (
        # base volume
        π * R^2 * H + π * g.groove.radius_in_mm.outer^2 * fccd +
        # remove p+ contact, groove and tapers
        - (pp_volume + groove_volume + taper_top_volume + taper_bottom_volume) +
        # remove extras
        - get_extra_volume(g, fccd)
    ) * 1e-3u"cm^3"
end

function get_active_volume(pd::PropDict, ::Val{:icpc}, fccd::T = .0) where {T <: AbstractFloat}

    g = pd.geometry
    
    R = g.radius_in_mm - fccd
    H = g.height_in_mm - 2 * fccd
    
    # Borehole
    rb = g.borehole.radius_in_mm + fccd
    db = g.borehole.depth_in_mm
    borehole_volume = π * rb^2 * db
    
    # Groove
    groove_volume = π * g.groove.depth_in_mm * (g.groove.radius_in_mm.outer^2 - g.groove.radius_in_mm.inner^2)

    # Top taper
    α = g.taper.top.angle_in_deg / 360 * 2π
    h = g.taper.top.height_in_mm + fccd * (1 - sin(α) - cos(α))/sin(α)
    x = h * tan(α)
    taper_top_volume = iszero(x) || !isfinite(x) ? zero(T) : get_outer_taper_volume(R, R - x, h)
    
    # Bottom taper
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    h = g.taper.bottom.height_in_mm + fccd * (1 - sin(α) - cos(α))/sin(α)
    x = h * tan(α)
    taper_bottom_volume = iszero(x) || !isfinite(x) ? zero(T) : get_outer_taper_volume(R, R - x, h)
    
    # Borehole taper
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    h = g.taper.bottom.height_in_mm + fccd * (1 - sin(α) - cos(α))/sin(α)
    x = h * tan(α)
    taper_borehole_volume = iszero(x) || !isfinite(x) ? zero(T) : get_inner_taper_volume(R, R - x, h)

    # p+ contact
    rp = g.pp_contact.radius_in_mm
    dp = g.pp_contact.depth_in_mm
    pp_volume = π * rp^2 * dp
    
    return (
        # base volume
        π * R^2 * H + π * g.groove.radius_in_mm.outer^2 * fccd +
        # remove p+ contact, groove, borehole and tapers
        - (pp_volume + groove_volume + borehole_volume + taper_top_volume + taper_bottom_volume + taper_borehole_volume) +
        # remove extras
        - get_extra_volume(g, fccd)
    ) * 1e-3u"cm^3"
end

function get_active_volume(pd::PropDict, ::Val{:coax}, fccd::T = .0) where {T <: AbstractFloat}
    
    g = pd.geometry
    
    R = g.radius_in_mm - fccd
    H = g.height_in_mm - 2 * fccd
    
    # Borehole
    rb = g.borehole.radius_in_mm
    db = g.borehole.depth_in_mm
    borehole_volume = π * rb^2 * db
    
    # Groove
    groove_volume = π * g.groove.depth_in_mm * (g.groove.radius_in_mm.outer^2 - g.groove.radius_in_mm.inner^2)
    
    # Top taper
    α = g.taper.top.angle_in_deg / 360 * 2π
    h = g.taper.top.height_in_mm + fccd * (1 - sin(α) - cos(α))/sin(α)
    x = h * tan(α)
    taper_top_volume = iszero(x) || !isfinite(x) ? zero(T) : get_outer_taper_volume(R, R - x, h)
    
    # Bottom taper
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    h = g.taper.bottom.height_in_mm + fccd * (1 - sin(α) - cos(α))/sin(α)
    x = h * tan(α)
    taper_bottom_volume = iszero(x) || !isfinite(x) ? zero(T) : get_outer_taper_volume(R, R - x, h)

    return (
        # base volume
        π * R^2 * H + π * g.groove.radius_in_mm.outer^2 * fccd +
        # remove 
        - (taper_top_volume + taper_bottom_volume + groove_volume + borehole_volume) + 
        # remove extras
        - get_extra_volume(g, fccd)
    ) * 1e-3u"cm^3"
end


function get_active_volume(pd::PropDict, ::Val{:ppc}, fccd::T = .0) where {T <: AbstractFloat}
    
    g = pd.geometry
    
    R = g.radius_in_mm - fccd
    H = g.height_in_mm - fccd
    
    # Top taper
    α = g.taper.top.angle_in_deg / 360 * 2π
    h = g.taper.top.height_in_mm + fccd * (1 - sin(α) - cos(α))/sin(α)
    x = h * tan(α)
    taper_top_volume = iszero(x) || !isfinite(x) ? zero(T) : get_outer_taper_volume(R, R - x, h)
    
    # Bottom taper
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    h = g.taper.bottom.height_in_mm + fccd * (1 - cos(α)) / sin(α)
    x = h * tan(α) - fccd
    taper_bottom_volume = iszero(x) || !isfinite(x) ? zero(T) : get_outer_taper_volume(R, R - x, h)
    
    return (
        # base volume
        π * R^2 * H +
        # remove tapers
        - (taper_top_volume + taper_bottom_volume) + 
        # remove extras
        - get_extra_volume(g, fccd)
    ) * 1e-3u"cm^3"
end

function get_active_volume(pd::PropDict, fccd::AbstractFloat)
    get_active_volume(pd, Val(Symbol(pd.type)), fccd)
end
