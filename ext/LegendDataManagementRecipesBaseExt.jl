# This file is a part of LegendSpecFits.jl, licensed under the MIT License (MIT).

module LegendDataManagementRecipesBaseExt

using RecipesBase
using PropDicts
using Statistics
using TypedTables
using Unitful
using Format
using Measurements: value, uncertainty, weightedmean

@recipe function f(
    chinfo::Table,
    pars::PropDict,
    properties::AbstractVector = [];
    calculate_mean = true,
    verbose = false
)

    # find the unit, add it to the NaN values
    u = Unitful.NoUnits
    for det in chinfo.detector
        if haskey(pars, det)
            mval = reduce(getproperty, properties, init = pars[det])
            if !(mval isa PropDicts.MissingProperty)
                u = unit(mval)
                break
            end
        end
    end
    
    # collect the data
    labels = String[]
    labelcolors = Symbol[]
    vlines = Int[]
    xvalues = Int[]
    yvalues = []
    notworking = Int[]
    for s in sort(unique(chinfo.detstring))
        push!(labels, format("String:{:02d}", s))
        push!(labelcolors, :blue)
        push!(vlines, length(labels))
        for det in sort(chinfo[chinfo.detstring .== s], lt = (a,b) -> a.position < b.position).detector
            push!(labels, string(det))
            push!(xvalues, length(labels))
            existing = false
            if haskey(pars, det)
                mval = reduce(getproperty, properties, init = pars[det])
                existing = (mval isa Number && !iszero(value(mval)))
            end
            if existing
                push!(yvalues, uconvert(u, mval))
                push!(labelcolors, :black)
            else
                verbose && @warn "No entry $(join(string.(properties), '/')) for detector $(det)"
                push!(yvalues, NaN * u)
                push!(notworking, length(labels))
                push!(labelcolors, :red)
            end
        end
    end
    push!(vlines, length(labels) + 1)


    # prepare the plot
    ylabel = length(properties) > 0 ? join(string.(properties), " ") : "Quantity"
    legendfontsize --> 10
    size --> (2500,800)
    legend --> :outerright
    thickness_scaling --> 1.5
    gridalpha := 0.5
    xguide := "Detector"
    yguide --> ylabel
    xlims := (1, length(labels) + 1)
    framestyle := :box
    
    # colored labels (created as annotations below the x-axis)
    left_margin --> (10, :mm)
    bottom_margin --> (8, :mm)
    xticks := (eachindex(labels), vcat(fill(" ",length(labels)-1), "            "))
    xrotation := 90
    yl = let ylims = get(plotattributes, :ylims, :auto)
        if ylims == :auto
            ustrip.(u, (
                0.98 * minimum(filter(!isnan, value.(yvalues) .- uncertainty.(yvalues))),
                1.02 * maximum(filter(!isnan, value.(yvalues) .+ uncertainty.(yvalues)))
            ))
        else
            ylims
        end
    end
    ylims := yl
    @series begin
        let (xticks, xlabels) = (eachindex(labels), labels)
            label := ""
            markeralpha := 0
            y0 = @. zero(xticks) + yl[1] - 0.02*(yl[2] - yl[1])
            # TODO: if text/font definitions get moved from Plots to RecipesBase,
            # replace the following line with something Plots-independent
            series_annotations := [Main.Plots.text(xlabels[i], 8, labelcolors[i], :right, rotation=90) for i in eachindex(xlabels)]
            xticks, y0
        end
    end

    # vertical lines and x-axis labeling
    @series begin
        seriestype := :vline
        linecolor := :black
        linewidth := 2
        label := ""
        vlines
    end

    # plot the data
    @series begin
        seriestype := :scatter
        yerr := uncertainty.(yvalues)
        label := get(plotattributes, :label, ylabel) * if calculate_mean
            mean_value = if any(iszero, uncertainty.(yvalues))
                mean(ustrip.(u, filter(!isnan, yvalues)))
            else
                weightedmean(ustrip.(u, filter(!isnan, yvalues)))
            end
            ": $(round(mean_value, digits=3)*u)"
        else
            ""
        end
        c = get(plotattributes, :seriescolor, :auto)
        seriescolor := c
        markerstrokecolor := c
        xvalues, value.(yvalues)
    end  
end


end # module LegendDataManagementRecipesBaseExt