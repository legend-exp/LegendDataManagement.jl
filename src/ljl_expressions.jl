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
        return process_ljlexpr(raw_expr, _ljl_expr_unitmap)
    end
end
export parse_ljlexpr


const ljl_expr_allowed_heads = Symbol[:., :ref, :call, :macrocall, :||, :&&, :comparison, :if]

const ljl_expr_allowed_funcs = Set{Symbol}([
    :!,
    :(==), :<, :>, :>=, :<=, :!=,
    :isapprox, :≈,
    :in, :∈, :..,
    :+, :-, :*, :/, :div, :rem, :mod,
    :|, :&, :xor,
    :^, :sqrt,
    :one, :zero, :identity,
    :abs, :abs2, :normalize, :norm,
    :exp, :exp2, :exp10, :log, :log2, :log10,
    :sin, :cos, :tan, :asin, :acos, :atan,
    :min, :max,
    :isnan, :isinf, :isfinite,
    :all, :any, :broadcast,
    :sum, :prod, :minimum, :maximum, :mean,
    :get, :getproperty, :getindex, :first, :last,
    :haskey, :isempty, :length, :size,
    :(:), :Symbol, :String, :Int, :Float64, :Bool,
    :string, :parse,
    :value, :uncertainty, :stdscore, :weightedmean, :±, 
    :DetectorId, :ChannelId
])

const _ljlexpr_units = IdDict{Symbol,Expr}([
    :s => :(u"s"),
    :ms => :(u"ms"),
    :μs => :(u"μs"),
    :us => :(u"μs"),
    :ns => :(u"ns"),
    :MeV => :(u"MeV"),
    :keV => :(u"keV"),
    :eV => :(u"eV"),
    :e => :(u"e_au"),
])
_ljl_expr_unitmap(sym::Symbol) = get(_ljlexpr_units, sym, sym)


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
    throw(ArgumentError("Invalid component of type $(nameof(typeof(x))) in LEGEND Julia expression."))
end

_process_ljlexpr_impl(x::Real, @nospecialize(f_varsubst)) = x
_process_ljlexpr_impl(x::LineNumberNode, @nospecialize(f_varsubst)) = x
_process_ljlexpr_impl(x::QuoteNode, @nospecialize(f_varsubst)) = x
_process_ljlexpr_impl(sym::Symbol, f_varsubst) = f_varsubst(sym)

function _process_ljlexpr_impl(@nospecialize(expr::Expr), @nospecialize(f_varsubst))
    _process_inner = Base.Fix2(_process_ljlexpr_impl, f_varsubst)
    if expr.head in ljl_expr_allowed_heads
        if expr.head == :.
            if length(expr.args) == 1
                arg1 = only(expr.args)
                if arg1 isa Symbol
                    # Standalone dot-operator syntax:
                    expr
                else
                    throw(ArgumentError("LEGEND Julia expressions don't support `$expr`"))
                end
            elseif length(expr.args) == 2
                arg1 = expr.args[begin]
                arg2 = expr.args[begin+1]
                if arg2 isa Union{Symbol,QuoteNode}
                    # Property access
                    return Expr(expr.head, _process_ljlexpr_impl(arg1, f_varsubst), arg2)
                elseif arg2 isa Expr && arg2.head == :tuple
                    # Broadcast syntax
                    return Expr(expr.head, arg1, Expr(:tuple, map(_process_inner, arg2.args)...))
                else
                    throw(ArgumentError("LEGEND Julia expressions don't support `$expr`"))
                end
            else
                throw(ArgumentError("LEGEND Julia expressions don't support `$expr`"))
            end
        elseif expr.head == :call
            funcname = expr.args[begin]
            funcname_str = string(funcname)
            # Handle constructs like `a .+ b`:
            base_funcname = funcname_str[begin] == '.' ? Symbol(funcname_str[begin+1:end]) : funcname
            funcargs = expr.args[begin+1:end]
            if base_funcname in ljl_expr_allowed_funcs
                return Expr(expr.head, funcname, map(_process_inner, funcargs)...)
            else
                throw(ArgumentError("Function \"$(funcname)\" not allowed in LEGEND Julia expression."))
            end
        elseif expr.head == :macrocall
            macro_name = expr.args[begin]
            macro_args = expr.args[begin+1:end]
            if macro_name == Symbol("@u_str")
                return Expr(expr.head, macro_name, macro_args...)
            else
                throw(ArgumentError("Macro \"$(macro_name)\" not allowed in LEGEND Julia expression."))
            end
        else
            return Expr(expr.head, map(_process_inner, expr.args)...)
        end
    else
        @info "EXPR:" expr
        throw(ArgumentError("Invalid head $(expr.head) in LEGEND Julia expression."))
    end
end


const _jlexpr_namespace = UUID("cdf3a628-300d-4c5f-ac08-f586248318e9")

_expr_hash(expr) = uuid5(_jlexpr_namespace, string(expr))

const _argexpr_dict = IdDict{UUID, @NamedTuple{args::Vector{Symbol}, body}}()


struct _ExprFunction{hash} <:Function end

@generated function (f::_ExprFunction{fhash})(__exprf_args__...) where fhash
    argnames, body = _argexpr_dict[fhash]
    argtuple = Expr(:tuple, argnames...)
    quote      
        $argtuple = __exprf_args__
        $body
    end
end

function _propfrom_from_expr(pf_body)
    props, args, args_body = props2varsyms(pf_body)
    args_body_hash = _expr_hash(args_body)
    get!(_argexpr_dict, args_body_hash, (args = args, body = args_body))

    sel_prop_func = _ExprFunction{args_body_hash}()
    PropertyFunction{(props...,)}(sel_prop_func)
end


const _ljlexpr_numbers = IdDict([
    :NaN => :NaN,
    :Inf => :Inf,
    :missing => :missing,
    :nothing => :nothing
])

_pf_varsym(sym::Symbol) = get(_ljlexpr_numbers, sym, Expr(:$, sym))

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
            return _propfrom_from_expr(pf_body)
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
    return _propfrom_from_expr(pf_body)
end

function ljl_propfunc(@nospecialize(expr_map::AbstractDict{Symbol,<:AbstractString}))
    ljl_propfunc(Dict([(k, parse_ljlexpr(string(v))) for (k, v) in expr_map]))
end

ljl_propfunc(@nospecialize(expr_map::PropDict)) = ljl_propfunc(Dict{Symbol,String}(expr_map))
