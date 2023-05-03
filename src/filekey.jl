# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


export FileKey

struct FileKey
    setup::Symbol
    period::Int
    run::Int
    category::Symbol
    time::Int64
end

FileKey(
    setup::Union{Symbol,AbstractString},
    period::Integer,
    run::Integer,
    category::Union{Symbol,AbstractString},
    time::Union{Integer,AbstractString},
) = FileKey(Symbol(setup), Int(period), Int(run), Symbol(category), timestamp2unix(time))

#l200-p02-r006-cal-20221226T200846Z
const filekey_expr = r"^([A-Za-z_][A-Za-z_0-9]*)-p([0-9]{2})-r([0-9]{3})-([A-Za-z_]+)-([0-9]{8}T[0-9]{6}Z)$"
const filekey_relaxed_expr = r"^([A-Za-z_][A-Za-z_0-9]*)-p([0-9]{2})-r([0-9]{3})-([A-Za-z_]+)-([0-9]{8}T[0-9]{6}Z)(-.*)?$"

is_filekey_string(s::AbstractString) = occursin(filekey_expr, s)


const timestamp_format = dateformat"yyyymmddTHHMMSSZ"
const timestamp_expr = r"^([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})Z$"

timestamp2datetime(t::AbstractString) = DateTime(t, timestamp_format)
timestamp2unix(t::AbstractString) = Int64(Dates.datetime2unix(timestamp2datetime(t)))
timestamp2unix(t::Integer) = Int64(t)

unix2timestamp(t::Integer) = Dates.format(Dates.unix2datetime(t), timestamp_format)

is_timestamp_string(s::AbstractString) = occursin(timestamp_expr, s)

function timestamp_from_string(s::AbstractString)
    if is_timestamp_string(s)
        timestamp2datetime(s)
    elseif is_filekey_string(s)
        Dates.unix2datetime(FileKey(s).time)
    else
        throw(ArgumentError("String \"$s\" doesn't seem to be or contain a LEGEND-compatible timestamp"))
    end
end



import Base.==
==(a::FileKey, b::FileKey) = a.setup == b.setup && a.run == b.run && a.time == b.time && a.category == b.category


filekey_period_str(key::FileKey) = "p$(lpad(string(key.period), 2, string(0)))"
filekey_run_str(key::FileKey) = "r$(lpad(string(key.run), 3, string(0)))"

function FileKey(s::AbstractString)
    m = match(filekey_relaxed_expr, basename(s))
    if (m == nothing)
        throw(ArgumentError("String \"$s\" does not represent a valid file key or a compatible filename"))
    else
        x = (m::RegexMatch).captures
        FileKey(
            Symbol(x[1]),
            parse(Int, x[2]),
            parse(Int, x[3]),
            Symbol(x[4]),
            timestamp2unix(x[5])
        )
    end
end

Base.convert(::Type{FileKey}, s::AbstractString) = FileKey(s)


function Base.print(io::IO, key::FileKey)
    print(io, key.setup)
    print(io, "-", filekey_period_str(key))
    print(io, "-", filekey_run_str(key))
    print(io, "-", key.category)
    print(io, "-", unix2timestamp(key.time))
end


Base.show(io::IO, key::FileKey) = print(io, "FileKey(\"$(string(key))\")")


Dates.DateTime(key::FileKey) = Dates.unix2datetime(key.time)
