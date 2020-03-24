# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).


export FileKey

struct FileKey
    setup::Symbol
    run::Int
    time::Int64
    category::Symbol
end


const filekey_expr = r"^([A-Za-z_]+)-run([0-9]{4})-([0-9]{8}T[0-9]{6}Z)-([A-Za-z_]+)$"
const filekey_wildcard_expr = r"^(%|[A-Za-z_]+)-(%|run[0-9]{4})-(%|[0-9]{8}T[0-9]{6}Z)-(%|[A-Za-z_]+)$"

const timestamp_format = dateformat"yyyymmddTHHMMSSZ"

timestamp2unix(t::String) = Int64(Dates.datetime2unix(DateTime(t, timestamp_format)))

unix2timestamp(t::Integer) = Dates.format(Dates.unix2datetime(t), timestamp_format)


import Base.==
==(a::FileKey, b::FileKey) = a.setup == b.setup && a.run == b.run && a.time == b.time && a.category == b.category


filekey_run_str(key::FileKey) = "run$(lpad(string(key.run), 4, string(0)))"

function FileKey(s::AbstractString)
    m = match(filekey_expr, s)
    if (m == nothing)
        throw(ArgumentError("String \"$s\" does not represent a valid file key"))
    else
        x = (m::RegexMatch).captures
        FileKey(
            Symbol(x[1]),
            parse(Int, x[2]),
            Int64(Dates.datetime2unix(DateTime(x[3], timestamp_format))),
            Symbol(x[4])
        )
    end
end

Base.convert(::Type{FileKey}, s::AbstractString) = FileKey(s)


function Base.print(io::IO, key::FileKey)
    print(io, key.setup)
    print(io, "-", filekey_run_str(key))
    print(io, "-", unix2timestamp(key.time))
    print(io, "-", key.category)
end


Base.show(io::IO, key::FileKey) = print(io, "FileKey(\"$(string(key))\")")


function Base.occursin(key::FileKey, pattern::AbstractString)
    mk = match(filekey_wildcard_expr, string(key))
    mp = match(filekey_wildcard_expr, pattern)
    
    if (mp == nothing)
        throw(ArgumentError("Not a valid file key pattern: \"$pattern\""))
    else
        all((a == b || b == "%" for (a,b) in zip(mk.captures, mp.captures)))
    end
end
