# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


_table_columnnames(tbl) = keys(Tables.columns(tbl))
_default_table_headermap(tbl) = Dict(k => string(k) for k in _table_columnnames(tbl))

_markdown_cell_content(@nospecialize(x)) = x
_markdown_cell_content(@nospecialize(x::AbstractString)) = String(x)
_markdown_cell_content(@nospecialize(x::Symbol)) = string(x)
_markdown_cell_content(@nospecialize(x::Expr)) = string(x)
_markdown_cell_content(@nospecialize(x::Number)) = _show_plain_compact(x)
_markdown_cell_content(@nospecialize(x::AbstractInterval)) = _show_plain_compact(x)
_markdown_cell_content(@nospecialize(x::Array)) = _show_plain_compact(x)

_show_plain_compact(x) = sprint(show, x; context = :compact=>true)


function _markdown_table(
    tbl;
    headermap::Dict{Symbol,<:AbstractString} = _default_table_headermap(tbl),
    align::AbstractVector{Symbol} = fill(:l, length(Tables.columns(tbl)))
)
    content = Vector{Any}[Any[headermap[k] for k in keys(Tables.columns(tbl))]]
    for r in Tables.rows(tbl)
        push!(content, [_markdown_cell_content(x) for x in values(r)])
    end
    Markdown.Table(content, align)
end



"""
    struct LegendReport

Represents a LEGEND report, e.g. a data processing report.

Example:

```julia
using LegendDataManagement, StructArrays, IntervalSets, Plots

tbl = StructArray(
    col1 = rand(5), col2 = ClosedInterval.(rand(5), rand(5).+1),
    col3 = [rand(3) for i in 1:5], col4 = ProcessStatus.(rand(-1:1, 5)),
    col5 = [:a, :b, :c, :d, :e], col6 = ["a", "b", "c", "d", "e"],
    col7 = [:(a[1]), :(a[2]), :(a[3]), :(a[4]), :(a[5])]
)

report = LegendReport()

push!(report, "# New report")
push!(report, "Table 1:")
push!(report, tbl)
push!(report, "Figure 1:")
push!(report, stephist(randn(1000)))

show(stdout, MIME"text/plain"(), report)
show(stdout, MIME"text/html"(), report)
show(stdout, MIME"text/markdown"(), report)
```
"""
struct LegendReport
    _elements::AbstractVector
end
export LegendReport

LegendReport() = LegendReport(Any[])

function Base.show(io::IO, r::LegendReport)
    for x in r._elements
        show(io, x)
        println(io)
    end
end


function Base.show(io::IO, ::MIME"text/plain", r::LegendReport)
    for x in r._elements
        _show_report_element_plain(io, x)
        println(io)
    end
end

function _show_report_element_plain(io::IO, x)
    show(io, MIME("text/plain"), x)
end


function Base.show(io::IO, ::MIME"text/html", r::LegendReport)
    for x in r._elements
        _show_report_element_html(io, x)
        println(io)
    end
end

function _show_report_element_html(io::IO, x)
    if showable(MIME("text/html"), x)
        show(io, MIME("text/html"), x)
    else
        _show_report_element_plain(io, x)
    end
end


function Base.show(io::IO, ::MIME"text/markdown", r::LegendReport)
    for x in r._elements
        _show_report_element_markdown(io, x)
        #println(io)
    end
end

function _show_report_element_markdown(io::IO, x)
    if showable(MIME("text/markdown"), x)
        show(io, MIME("text/markdown"), x)
    else
        _show_report_element_html(io, x)
    end
end


Base.show(io::IO, ::MIME"juliavscode/html", r::LegendReport) = show(io, MIME("text/html"), r)


Base.push!(report::LegendReport, @nospecialize(content)) = _report_push!(report, content)


function _report_push!(report::LegendReport, @nospecialize(content))
    push!(report._elements, content)
    return report
end

function _report_push!(report::LegendReport, content::Markdown.MD)
    if isempty(report._elements) || !(report._elements[end] isa Markdown.MD)
        push!(report._elements, content)
    else
        append!(report._elements[end].content, content.content)
    end
    return report
end

function _report_push!(report::LegendReport, @nospecialize(markdown_str::AbstractString))
    _report_push!(report, Markdown.parse(markdown_str))
end

function _report_push!(report::LegendReport, @nospecialize(tbl::AbstractVector{<:NamedTuple}))
    _report_push!(report, Markdown.MD(_markdown_table(tbl)))
end
