# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const _cached_pyparsed = LRU{String, LJlExprLike}(maxsize = 10^4)

"""
    parse_lpyexpr(expr_string::AbstractString)::LJlExprLike

Parses an expression compatible with the LEGEND Python software and returns a
Julia syntax tree.

Supported are numbers, variable names and calls of `~` (not), `!=`, `+`, `-`,
`*`, `/`, `<`, `>`, `>=`, `<=` and `abs`, as well as `|` and `&`, which are
converted to `||` and `&&`. An `ArgumentError` is thrown for other syntax. The
result is a valid LEGEND Julia expression (see [`parse_ljlexpr`](@ref)).
The parsed expressions are cached.

## Examples
```julia
using LegendDataManagement

parse_lpyexpr("~(is_saturated) & (e_cusp > 100.0)")
```

See also [`lpy_propfunc`](@ref).
"""
function parse_lpyexpr(@nospecialize(expr_string::AbstractString))
    get!(_cached_pyparsed, expr_string) do
        raw_expr = Meta.parse(expr_string)
        return _lpyexpr_to_ljlexpr(raw_expr)
    end
end
export parse_lpyexpr


const pygama_julia_funcmap = IdDict{Symbol, Symbol}(
    :~ => :!,
    :!= => :!=,
    :+ => :+,
    :- => :-,
    :* => :*,
    :/ => :/,
    :< => :<,
    :> => :>,
    :>= => :>=,
    :<= => :<=,
    :abs => :abs,
)


const _cached_lpyexpr2jl = LRU{LJlExprLike,LJlExprLike}(maxsize = 10^4)

function _lpyexpr_to_ljlexpr(@nospecialize(lpy_expr::LJlExprLike))
    get!(_cached_lpyexpr2jl, lpy_expr) do
        jl_expr = _lpyexpr_to_ljlexpr_impl(lpy_expr)
        process_ljlexpr(jl_expr)
    end
end

_lpyexpr_to_ljlexpr_impl(x::Integer) = x
_lpyexpr_to_ljlexpr_impl(x::AbstractFloat) = x

_lpyexpr_to_ljlexpr_impl(sym::Symbol) = sym

function _lpyexpr_to_ljlexpr_impl(expr::Expr)
    head = expr.head
    if head == :call
        pg_funcname = expr.args[begin]
        funcargs = expr.args[2:end]
        if pg_funcname == :|
            return Expr(:||, map(_lpyexpr_to_ljlexpr_impl, funcargs)...)
        elseif pg_funcname == :&
            return Expr(:&&, map(_lpyexpr_to_ljlexpr_impl, funcargs)...)
        elseif haskey(pygama_julia_funcmap, pg_funcname)
            jl_funcname = pygama_julia_funcmap[pg_funcname]
            return Expr(:call, jl_funcname, map(_lpyexpr_to_ljlexpr_impl, funcargs)...)
        else
            throw(ArgumentError("Unknown or invalid function name in pygama expression: $(pg_funcname)"))
        end
    else
        throw(ArgumentError("Unknown or invalid pygama expression head: $(head)"))
    end
end


"""
    lpy_propfunc(expr_string::AbstractString)::PropertyFunctions.PropertyFunction

Generates a `PropertyFunctions.PropertyFunction` from a LEGEND Python
expression, with the syntax described in [`parse_lpyexpr`](@ref).

See also [`parse_lpyexpr`](@ref) and [`ljl_propfunc`](@ref).
"""
function lpy_propfunc(@nospecialize(expr_string::AbstractString))
    ljl_propfunc(parse_lpyexpr(expr_string))
end
export lpy_propfunc
