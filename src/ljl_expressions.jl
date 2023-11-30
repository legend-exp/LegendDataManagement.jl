# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


# ToDo: Pull numerical values out of expressions and turn them into
# parameters, to reduce the amount of generated code.


"""
    const LJlExprLike = Union{Expr, Symbol, Integer, AbstractFloat}

Anything that can represent a parsed LEGEND Julia expression.
"""
const LJlExprLike = Union{Expr, Symbol, Integer, AbstractFloat}
export LJlExprLike


const _cached_jlparsed = LRU{String, LJlExprLike}(maxsize = 10^4)

"""
    parse_ljlexpr(expr_string::AbstractString)::LJlExprLike

Parse an LEGEND Julia expression and return a Julia syntax tree.
"""
function parse_ljlexpr(@nospecialize(expr_string::AbstractString))
    get!(_cached_jlparsed, expr_string) do
        raw_expr = Meta.parse(expr_string)
        return process_ljlexpr(raw_expr)
    end
end
export parse_ljlexpr


const jl_expr_allowed_heads = (:call, :||, :&&)

const jl_expr_allowed_funcs = (
    :!,
    :(==), :<, :>, :>=, :<=, :!=,
    :+, :-, :*, :/,
    :abs,
    :isnan, :isinf
)

const _cached_procjlexpr = LRU{Tuple{LJlExprLike,Any},LJlExprLike}(maxsize = 10^4)

"""
    process_ljlexpr(expr::LJlExprLike, f_varsubst = identity)::LJlExprLike

Verify that `expr` is a valid LEGEND Julia expression and return it,
with modifications if necessary.

Optionally substitute variables in `expr` using `f_varsubst`.
"""
function process_ljlexpr(@nospecialize(expr::LJlExprLike), f_varsubst = identity)
    get!(_cached_procjlexpr, (expr, f_varsubst)) do
        _process_ljlexpr_impl(expr, f_varsubst)::LJlExprLike
    end
end
export process_ljlexpr


function _process_ljlexpr_impl(x, @nospecialize(f_varsubst))
    throw(ArgumentError("Invalid component of type $(typeof(nameof(x))) in LEGEND Julia expression."))
end

_process_ljlexpr_impl(x::Real, @nospecialize(f_varsubst)) = x
_process_ljlexpr_impl(sym::Symbol, f_varsubst) = f_varsubst(sym)

function _process_ljlexpr_impl(@nospecialize(expr::Expr), @nospecialize(f_varsubst))
    _process_inner = Base.Fix2(_process_ljlexpr_impl, f_varsubst)
    if expr.head in jl_expr_allowed_heads
        if expr.head == :call
            funcname = expr.args[begin]
            funcargs = expr.args[2:end]
            if funcname in jl_expr_allowed_funcs
                return Expr(expr.head, funcname, map(_process_inner, funcargs)...)
            else
                throw(ArgumentError("Invalid function name $(funcname) in LEGEND Julia expression."))
            end
        else
            return Expr(expr.head, map(_process_inner, expr.args)...)
        end
    else
        throw(ArgumentError("Invalid head $(expr.head) in LEGEND Julia expression."))
    end
end


_pf_varsym(sym::Symbol) = Expr(:$, sym)

const _cached_ljl_propfunc = Dict{Union{LJlExprLike}, PropertyFunction}()
const _cached_ljl_propfunc_lock = ReentrantLock()

"""
    ljl_propfunc(expr::LJlExprLike)
    ljl_propfunc(expr_string::AbstractString)

Compiles a `PropertyFunctions.PropertyFunction` from a LEGEND Julia
expression.

See also [`parse_lpyexpr`](@ref).
"""
function ljl_propfunc end
export ljl_propfunc

function ljl_propfunc(@nospecialize(expr::LJlExprLike))
    lock(_cached_ljl_propfunc_lock) do
        get!(_cached_ljl_propfunc, expr) do
            pf_body = process_ljlexpr(expr, _pf_varsym)
            return eval(:(@pf $pf_body))
        end
    end
end

function ljl_propfunc(@nospecialize(expr_string::AbstractString))
    ljl_propfunc(parse_ljlexpr(expr_string))
end

"""
    ljl_propfunc(expr_map::AbstractDict{Symbol,<:LJlExprLike})
    ljl_propfunc(expr_map::AbstractDict{Symbol,<:AbstractString})
    ljl_propfunc(expr_map::PropDict)

Compiles a map between output field-names and LEGEND Julia expressions to
a single `PropertyFunctions.PropertyFunction`.

The generated function will return `NamedTuple`s with the same property names
as the keys of `expr_map`.
"""
function ljl_propfunc(@nospecialize(expr_map::AbstractDict{Symbol,<:LJlExprLike}))
    nt_entries = [:($label = $(process_ljlexpr(expr, _pf_varsym))) for (label, expr) in expr_map]
    sort!(nt_entries, by = x -> x.args[1])
    pf_body = :(())
    append!(pf_body.args, nt_entries)
    return eval(:(@pf $pf_body))
end

function ljl_propfunc(@nospecialize(expr_map::AbstractDict{Symbol,<:AbstractString}))
    ljl_propfunc(Dict([(k, parse_ljlexpr(v)) for (k, v) in expr_map]))
end

ljl_propfunc(@nospecialize(expr_map::PropDict)) = ljl_propfunc(Dict{Symbol,String}(expr_map))
