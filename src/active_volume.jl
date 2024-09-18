# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

# These are function needed to calculate the active volume based on 
# LEGEND detector metadata and measured dead layer thicknesses


@inline get_inner_taper_volume(x, y, h, r) = π * (r^2 * y / 3 - (r - x)^2 * (y + 2h) / 3)
@inline get_outer_taper_volume(x, y, h, r) = π * (r^2 * h - (r - x)^2 * h) - get_inner_taper_volume(x, y, h, r)

function get_extra_volume(geometry::PropDict, ::Val{:crack}, fccd::T) where {T <: AbstractFloat}
    r = geometry.radius_in_mm
    H = geometry.height_in_mm
    p0 = geometry.extra.crack.radius_in_mm
    alpha = geometry.extra.crack.angle_in_deg
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
    r = geometry.extra.topgroove.radius_in_mm
    d = geometry.extra.topgroove.depth_in_mm
    return π * r^2 * d
end

function get_extra_volume(geometry::PropDict, ::Val{:bottom_cylinder}, fccd::AbstractFloat)
    r = geometry.extra.bottom_cylinder.radius_in_mm - fccd
    h = geometry.extra.bottom_cylinder.height_in_mm - fccd
    t = geometry.extra.bottom_cylinder.transition_in_mm
    R = geometry.radius_in_mm - fccd
    return get_outer_taper_volume(R - r, t * R / (R - r), t, R) + π * h * (R^2 - r^2)
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
    h = g.taper.top.height_in_mm
    α = g.taper.top.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_top_volume = iszero(x) ? zero(T) : get_outer_taper_volume(x, h * R / x, h, R)
    
    # Bottom taper
    h = g.taper.bottom.height_in_mm
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_bottom_volume = iszero(x) ? zero(T) : get_outer_taper_volume(x, h * R / x, h, R)
    
    return (π * R^2 * H - (groove_volume + taper_top_volume + taper_bottom_volume) +
            π * g.groove.radius_in_mm.outer^2 * fccd - get_extra_volume(g, fccd)) * 1e-3u"cm^3"
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
    h = g.taper.top.height_in_mm
    α = g.taper.top.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_top_volume = iszero(x) ? zero(T) : get_outer_taper_volume(x, h * R / x, h, R)
    
    # Bottom taper
    h = g.taper.bottom.height_in_mm
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_bottom_volume = iszero(x) ? zero(T) : get_outer_taper_volume(x, h * R / x, h, R)
    
    # Borehole taper
    h = g.taper.bottom.height_in_mm
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_borehole_volume = iszero(x) ? zero(T) : get_inner_taper_volume(x, h * (rb + x) / x, h, rb + x)
    
    return (π * R^2 * H - (groove_volume + borehole_volume) - 
            taper_top_volume + taper_bottom_volume + taper_borehole_volume +
            π * g.groove.radius_in_mm.outer^2 * fccd - get_extra_volume(g, fccd)) * 1e-3u"cm^3"
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
    h = g.taper.top.height_in_mm
    α = g.taper.top.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_top_volume = iszero(x) ? zero(T) : get_outer_taper_volume(x, h * R / x, h, R)    
    
    # Bottom taper
    h = g.taper.bottom.height_in_mm
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_bottom_volume = iszero(x) ? zero(T) : get_outer_taper_volume(x, h * R / x, h, R)
        
    return (π * R^2 * H - (taper_top_volume + taper_bottom_volume + groove_volume + borehole_volume) + 
            π * g.groove.radius_in_mm.outer^2 * fccd - get_extra_volume(g, fccd)) * 1e-3u"cm^3"
end


function get_active_volume(pd::PropDict, ::Val{:ppc}, fccd::T = .0) where {T <: AbstractFloat}
    
    g = pd.geometry
    
    R = g.radius_in_mm - fccd
    H = g.height_in_mm - 2 * fccd
    
    # Top taper
    h = g.taper.top.height_in_mm
    α = g.taper.top.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_top_volume = iszero(x) ? zero(T) : get_outer_taper_volume(x, h * R / x, h, R)
    
    # Bottom taper
    h = g.taper.bottom.height_in_mm
    α = g.taper.bottom.angle_in_deg / 360 * 2π
    x = h * tan(α)
    taper_bottom_volume = iszero(x) ? zero(T) : get_outer_taper_volume(x, h * R / x, h, R)
    
    return (π * R^2 * H - (taper_top_volume + taper_bottom_volume) +
            π * g.pp_contact.radius_in_mm^2 * fccd - get_extra_volume(g, fccd)) * 1e-3u"cm^3"
end

function get_active_volume(pd::PropDict, fccd::AbstractFloat)
    get_active_volume(pd, Val(Symbol(pd.type)), fccd)
end
