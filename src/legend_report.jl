# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


"""
    struct LegendReport

Represents a LEGEND report.

Don't instantiate directly, use [`lreport()`](@ref)
"""
struct LegendReport
    _contents::AbstractVector
end


LegendReport() = LegendReport(Any[])

function Base.show(@nospecialize(io::IO), rpt::LegendReport)
    for x in rpt._contents
        show(io, x)
        println(io)
    end
end


function Base.show(@nospecialize(io::IO), mime::MIME"text/plain", rpt::LegendReport)
    r_conv = lreport_for_show!(lreport(), mime, rpt)
    for x in r_conv._contents
        _show_report_element_plain(io, x)
        println(io)
    end
end

function _show_report_element_plain(@nospecialize(io::IO), @nospecialize(x))
    show(io, MIME("text/plain"), x)
end


function Base.show(@nospecialize(io::IO), mime::MIME"text/html", rpt::LegendReport)
    r_conv = lreport_for_show!(lreport(), mime, rpt)
    for x in r_conv._contents
        _show_report_element_html(io, x)
        println(io)
    end
end

function _show_report_element_html(@nospecialize(io::IO), @nospecialize(x))
    if showable(MIME("text/html"), x)
        show(io, MIME("text/html"), x)
    else
        _show_report_element_plain(io, x)
    end
end


function Base.show(@nospecialize(io::IO), mime::MIME"text/markdown", rpt::LegendReport)
    r_conv = lreport_for_show!(lreport(), mime, rpt)
    for x in r_conv._contents
        _show_report_element_markdown(io, x)
        #println(io)
    end
end

function _show_report_element_markdown(@nospecialize(io::IO), @nospecialize(x))
    if showable(MIME("text/markdown"), x)
        show(io, MIME("text/markdown"), x)
    else
        _show_report_element_html(io, x)
    end
end


function Base.show(@nospecialize(io::IO), ::MIME"juliavscode/html", rpt::LegendReport)
    show(io, MIME("text/html"), rpt)
end



"""
    lreport()
    lreport(contents...)

Generate a LEGEND report, e.g. a data processing report.

Use [`lreport!(rpt, contents...)`](@ref) to add more content to a report.

Example:

```julia
using LegendDataManagement, StructArrays, IntervalSets, Plots

tbl = StructArray(
    col1 = rand(5), col2 = ClosedInterval.(rand(5), rand(5).+1),
    col3 = [rand(3) for i in 1:5], col4 = ProcessStatus.(rand(-1:1, 5)),
    col5 = [:a, :b, :c, :d, :e], col6 = ["a", "b", "c", "d", "e"],
    col7 = [:(a[1]), :(a[2]), :(a[3]), :(a[4]), :(a[5])]
)

rpt = lreport(
    "# New report",
    "Table 1:", tbl
)
lreport!(rpt, "Figure 1:", stephist(randn(10^3)))
lreport!(rpt, "Figure 2:", histogram2d(randn(10^4), randn(10^4), format = :png))

show(stdout, MIME"text/plain"(), rpt)
show(stdout, MIME"text/html"(), rpt)
show(stdout, MIME"text/markdown"(), rpt)

writelreport("report.txt", rpt)
writelreport("report.html", rpt)
writelreport("report.md", rpt)
```

See [`LegendDataManagement.lreport_for_show!`](@ref) for how to specialize the
behavior of `show` for specific report content types.
"""
function lreport end
export lreport


lreport() = LegendReport(Any[])
function lreport(contents...)
    rpt = lreport()
    for content in contents
        lreport!(rpt, content)
    end
    return rpt
end


"""
    lreport!(rpt::LegendReport, contents...)

Add more content to report `rpt`. See [`lreport`](@ref) for an example.
"""
function lreport! end
export lreport!


function lreport!(rpt::LegendReport, @nospecialize(content))
    push!(rpt._contents, content)
    return rpt
end

function lreport!(rpt::LegendReport, @nospecialize(contents...))
    for content in contents
        lreport!(rpt, content)
    end
    return rpt
end


function lreport!(rpt::LegendReport, content::Markdown.MD)
    # Need to make a copy here to prevent recursive self-modification during
    # show-transformation:
    content_content_copy = copy(content.content)

    if isempty(rpt._contents) || !(rpt._contents[end] isa Markdown.MD)
        push!(rpt._contents, Markdown.MD(content_content_copy))
    else
        append!(rpt._contents[end].content, content_content_copy)
    end
    return rpt
end

function lreport!(rpt::LegendReport, @nospecialize(markdown_str::AbstractString))
    lreport!(rpt, Markdown.parse(markdown_str))
end

lreport!(rpt::LegendReport, @nospecialize(number::AbstractFloat)) = lreport!(rpt, string(round(number, digits=3)))
lreport!(rpt::LegendReport, @nospecialize(number::Quantity{<:Real})) = lreport!(rpt, string(round(unit(number), number, digits=3)))


"""
    LegendDataManagement.lreport_for_show!(rpt::LegendReport, mime::MIME, content)

Add the contents of `content` to `rpt` in a way that is optimized for being
displayed (e.g. via `show`) with the given `mime` type.

`show(output, mime, rpt)` first transforms `rpt` by converting all contents of
`rpt` using `lreport_for_show!(rpt::LegendReport, mime, content)`.

Defaults to `lreport!(rpt, content)`, except for tables
(`Tables.istable(content) == true`), which are converted to Markdown tables
by default for uniform appearance.

`lreport_for_show!` is not inteded to be called by users, but to be
specialized for specific types of content `content`. Content types not already
supported will primarily require specialization of

```julia
lreport_for_show!(rpt::LegendReport, ::MIME"text/markdown", content::SomeType)
```

In some cases it may be desireable to specialize `lreport_for_show!` for
MIME types like `MIME"text/html"` and `MIME"text/plain"` as well.
"""
function lreport_for_show! end

function lreport_for_show!(rpt::LegendReport, mime::MIME, content::LegendReport)
    for c in content._contents
        lreport_for_show!(rpt, mime, c)
    end
    return rpt
end

function lreport_for_show!(rpt::LegendReport, ::MIME, @nospecialize(content))
    if Tables.istable(content)
        lreport!(rpt, Markdown.MD(_markdown_table(content)))
    else
        lreport!(rpt, content)
    end
end

_table_columnnames(tbl) = keys(Tables.columns(tbl))
_default_table_headermap(tbl) = Dict(k => string(k) for k in _table_columnnames(tbl))

_markdown_cell_content(@nospecialize(content)) = content
_markdown_cell_content(@nospecialize(content::AbstractString)) = String(content)
_markdown_cell_content(@nospecialize(content::Symbol)) = string(content)
_markdown_cell_content(@nospecialize(content::Expr)) = string(content)
_markdown_cell_content(@nospecialize(content::Number)) = _show_plain_compact(content)
_markdown_cell_content(@nospecialize(content::AbstractInterval)) = _show_plain_compact(content)
_markdown_cell_content(@nospecialize(content::Array)) = _show_plain_compact(content)


_show_plain_compact(@nospecialize(content)) = sprint(show, content; context = :compact=>true)

function _markdown_table(
    tbl;
    headermap::Dict{Symbol,<:AbstractString} = _default_table_headermap(tbl),
    align::AbstractVector{Symbol} = fill(:l, length(Tables.columns(tbl)))
)
    content = Vector{Any}[Any[headermap[k] for k in keys(Tables.columns(tbl))]]
    for rpt in Tables.rows(tbl)
        push!(content, [_markdown_cell_content(content) for content in values(rpt)])
    end
    Markdown.Table(content, align)
end


"""
    writelreport(filename::AbstractString, rpt::LegendReport)
    writelreport(filename::AbstractString, mime::MIME, rpt::LegendReport)

Write lreport `rpt` to file `filename`.
"""
function writelreport end
export writelreport

function writelreport(@nospecialize(filename::AbstractString), @nospecialize(mime::MIME), rpt::LegendReport)
    open(filename, "w") do io
        show(io, mime, rpt)
    end
end

function writelreport(@nospecialize(filename::AbstractString), rpt::LegendReport)
    _, ext = splitext(filename)
    mime = mime_from_extension(ext)
    writelreport(filename, mime, rpt)
end
