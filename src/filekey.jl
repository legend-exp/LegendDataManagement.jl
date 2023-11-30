# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

"""
    abstract type DataSelector

Abstract type for data selectors like
[`ExpSetup`](@ref), [`DataTier`](@ref), [`DataPeriod`](@ref),
[`DataRun`](@ref), [`DataCategory`](@ref), [`Timestamp`](@ref) and
[`FileKey`](@ref).
"""
abstract type DataSelector end


"""
    struct ExpSetup <: DataSelector

Represents a LEGEND experimental setup like "l200".

Example:

```julia
setup = ExpSetup(:l200)
setup.label == :l200
string(setup) == "l200"
ExpSetup("l200") == setup
```
"""
struct ExpSetup <: DataSelector
    label::Symbol
end
export ExpSetup

@inline ExpSetup(setup::ExpSetup) = setup

Base.:(==)(a::ExpSetup, b::ExpSetup) = a.label == b.label
Base.isless(a::ExpSetup, b::ExpSetup) = isless(a.label, b.label)

const _setup_expr = r"^([a-z][a-z0-9]*)$"

_can_convert_to(::Type{ExpSetup}, s::AbstractString) = !isnothing(match(_setup_expr, s))

function ExpSetup(s::AbstractString)
    _can_convert_to(ExpSetup, s) || throw(ArgumentError("String \"$s\" does not look like a valid file LEGEND setup name"))
    length(s) < 3 && throw(ArgumentError("String \"$s\" is too short to be a valid LEGEND setup name"))
    length(s) > 8 && throw(ArgumentError("String \"$s\" is too long to be a valid LEGEND setup name"))
    ExpSetup(Symbol(s))
end

Base.convert(::Type{ExpSetup}, s::Symbol) = ExpSetup(s)
Base.convert(::Type{ExpSetup}, s::AbstractString) = ExpSetup(s)

# ToDo: Improve implementation
Base.print(io::IO, category::ExpSetup) = print(io, category.label)


"""
    ExpSetupLike = Union{ExpSetup, Symbol, AbstractString}

Anything that can represent a setup label, like `ExpSetup(:l200)`, `:l200` or
`"l200"`.
"""
const ExpSetupLike = Union{ExpSetup, Symbol, AbstractString}
export ExpSetupLike



"""
    struct DataTier <: DataSelector

Represents a LEGEND data tier like "raw, "dsp", etc.

Example:

```julia
tier = DataTier(:raw)
tier.label == :raw
string(tier) == "raw"
DataTier("raw") == tier
```
"""
struct DataTier <: DataSelector
    label::Symbol
end
export DataTier

@inline DataTier(tier::DataTier) = tier

Base.:(==)(a::DataTier, b::DataTier) = a.label == b.label
Base.isless(a::DataTier, b::DataTier) = isless(a.label, b.label)

const tier_expr = r"^([a-z]+)$"

_can_convert_to(::Type{DataTier}, s::AbstractString) = !isnothing(match(tier_expr, s))

function DataTier(s::AbstractString)
    _can_convert_to(DataTier, s) || throw(ArgumentError("String \"$s\" does not look like a valid file LEGEND data tier"))
    length(s) < 3 && throw(ArgumentError("String \"$s\" is too short to be a valid LEGEND data tier"))
    length(s) > 6 && throw(ArgumentError("String \"$s\" is too long to be a valid LEGEND data tier"))
    DataTier(Symbol(s))
end

Base.convert(::Type{DataTier}, s::AbstractString) = DataTier(s)
Base.convert(::Type{DataTier}, s::Symbol) = DataTier(s)

# ToDo: Improve implementation
Base.print(io::IO, tier::DataTier) = print(io, tier.label)


"""
    DataTierLike = Union{DataTier, Symbol, AbstractString}

Anything that can represent a data tier, like `DataTier(:raw)`, `:raw` or
`"raw"`.
"""
const DataTierLike = Union{DataTier, Symbol, AbstractString}
export DataTierLike



"""
    struct DataPeriod <: DataSelector

Represents a LEGEND data-taking period.

Example:

```julia
period = DataPeriod(2)
period.no == 2
string(period) == "p02"
DataPeriod("p02") == period
```
"""
struct DataPeriod <: DataSelector
    no::Int
end
export DataPeriod

@inline DataPeriod(period::DataPeriod) = period

Base.:(==)(a::DataPeriod, b::DataPeriod) = a.no == b.no
Base.isless(a::DataPeriod, b::DataPeriod) = isless(a.no, b.no)

# ToDo: Improve implementation
Base.print(io::IO, period::DataPeriod) = print(io, "p$(lpad(string(period.no), 2, string(0)))")

const period_expr = r"^p([0-9]{2})$"

_can_convert_to(::Type{DataPeriod}, s::AbstractString) = !isnothing(match(period_expr, s))

function DataPeriod(s::AbstractString)
    m = match(period_expr, s)
    if (m == nothing)
        throw(ArgumentError("String \"$s\" does not look like a valid file LEGEND data-run name"))
    else
        DataPeriod(parse(Int, (m::RegexMatch).captures[1]))
    end
end

Base.convert(::Type{DataPeriod}, s::AbstractString) = DataPeriod(s)


"""
    DataPeriodLike = Union{DataPeriod, Integer, AbstractString}

Anything that can represent a data period, like `DataPeriod(2)` or "p02".
"""
const DataPeriodLike = Union{DataPeriod, AbstractString}
export DataPeriodLike



"""
    struct DataRun <: DataSelector

Represents a LEGEND data-taking run.

Example:

```julia
r = DataRun(6)
r.no == 6
string(r) == "r006"
DataRun("r006") == r
```
"""
struct DataRun <: DataSelector
    no::Int
end
export DataRun

@inline DataRun(r::DataRun) = r

Base.:(==)(a::DataRun, b::DataRun) = a.no == b.no
Base.isless(a::DataRun, b::DataRun) = isless(a.no, b.no)

# ToDo: Improve implementation
Base.print(io::IO, run::DataRun) = print(io, "r$(lpad(string(run.no), 3, string(0)))")

const run_expr = r"^r([0-9]{3})$"

_can_convert_to(::Type{DataRun}, s::AbstractString) = !isnothing(match(run_expr, s))

function DataRun(s::AbstractString)
    m = match(run_expr, s)
    if (m == nothing)
        throw(ArgumentError("String \"$s\" does not look like a valid file LEGEND data-run name"))
    else
        DataRun(parse(Int, (m::RegexMatch).captures[1]))
    end
end

Base.convert(::Type{DataRun}, s::AbstractString) = DataRun(s)

"""
    DataRunLike = Union{DataRun, Integer, AbstractString}

Anything that can represent a data run, like `DataRun(6)` or "r006".
"""
DataRunLike = Union{DataRun, AbstractString}
export DataRunLike


"""
    struct DataCategory <: DataSelector

Represents a LEGEND data category (related to a DAQ/measuring mode) like
"cal" or "phy".

Example:

```julia
category = DataCategory(:cal)
category.label == :cal
string(category) == "cal"
DataCategory("cal") == category
```
"""
struct DataCategory <: DataSelector
    label::Symbol
end
export DataCategory

@inline DataCategory(category::DataCategory) = category

Base.:(==)(a::DataCategory, b::DataCategory) = a.label == b.label
Base.isless(a::DataCategory, b::DataCategory) = isless(a.label, b.label)

const category_expr = r"^([a-z]+)$"

_can_convert_to(::Type{DataCategory}, s::AbstractString) = !isnothing(match(category_expr, s))

function DataCategory(s::AbstractString)
    _can_convert_to(DataCategory, s) || throw(ArgumentError("String \"$s\" does not look like a valid file LEGEND data category"))
    length(s) < 3 && throw(ArgumentError("String \"$s\" is too short to be a valid LEGEND data category"))
    length(s) > 6 && throw(ArgumentError("String \"$s\" is too long to be a valid LEGEND data category"))
    DataCategory(Symbol(s))
end

Base.convert(::Type{DataCategory}, s::AbstractString) = DataCategory(s)
Base.convert(::Type{DataCategory}, s::Symbol) = DataCategory(s)

# ToDo: Improve implementation
Base.print(io::IO, category::DataCategory) = print(io, category.label)


"""
    DataCategoryLike = Union{DataCategory, Symbol, AbstractString}

Anything that can represent a data category, like `DataCategory(:cal)`,
`:cal` or `"cal"`.
"""
const DataCategoryLike = Union{DataCategory, Symbol, AbstractString}
export DataCategoryLike



"""
    struct Timestamp <: DataSelector

Represents a LEGEND timestamp.

Example:

```julia
timestamp = Timestamp("20221226T200846Z")
timestamp.unixtime == 1672085326
string(timestamp) == "20221226T200846Z"
````
"""
struct Timestamp <: DataSelector
    unixtime::Int
end
export Timestamp

@inline Timestamp(timestamp::Timestamp) = timestamp

Dates.DateTime(timestamp::Timestamp) = Dates.unix2datetime(timestamp.unixtime)
Timestamp(datetime::Dates.DateTime) = Timestamp(round(Int, Dates.datetime2unix(datetime)))

_can_convert_to(::Type{Timestamp}, s::AbstractString) = _is_timestamp_string(s) || _is_filekey_string(s)

function Timestamp(s::AbstractString)
    if _is_timestamp_string(s)
        Timestamp(DateTime(s, _timestamp_format))
    elseif _is_filekey_string(s)
        Timestamp(FileKey(s))
    else
        throw(ArgumentError("String \"$s\" doesn't seem to be or contain a LEGEND-compatible timestamp"))
    end
end

Base.convert(::Type{Timestamp}, s::AbstractString) = Timestamp(s)
Base.convert(::Type{Timestamp}, datetime::DateTime) = Timestamp(datetime)


Base.:(==)(a::Timestamp, b::Timestamp) = a.unixtime == b.unixtime
Base.isless(a::Timestamp, b::Timestamp) = isless(a.unixtime, b.unixtime)

Base.print(io::IO, timestamp::Timestamp) = print(io, Dates.format(DateTime(timestamp), _timestamp_format))


const _timestamp_format = dateformat"yyyymmddTHHMMSSZ"
const _timestamp_expr = r"^([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})Z$"

# ToDo: Remove _timestamp2datetime and _timestamp2unix
_timestamp2datetime(t::AbstractString) = DateTime(Timestamp(t))
_timestamp2unix(t::AbstractString) = Timestamp(t).unixtime
_timestamp2unix(t::Integer) = Int64(t)

_is_timestamp_string(s::AbstractString) = occursin(_timestamp_expr, s)

# ToDo: Remove _timestamp_from_string
_timestamp_from_string(s::AbstractString) = DateTime(Timestamp(s))


"""
    TimestampLike = Union{Timestamp, AbstractString, Integer}

Anything that can represent a timestamp, like `Timestamp("20221226T200846Z")`
or "20221226T200846Z".
"""
const TimestampLike = Union{Timestamp, AbstractString, Integer}
export TimestampLike



"""
    struct FileKey <: DataSelector

Represents a LEGEND file key.

Example:

```julia
filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
```

See also [`read_filekeys`](@ref) and [`write_filekeys`](@ref).
"""
struct FileKey <: DataSelector
    setup::ExpSetup
    period::DataPeriod
    run::DataRun
    category::DataCategory
    time::Timestamp
end
export FileKey

#=FileKey(
    setup::Union{Symbol,AbstractString},
    period::Integer,
    run::Integer,
    category::Union{Symbol,AbstractString},
    time::Union{Integer,AbstractString},
) = FileKey(Symbol(setup), Int(period), Int(run), Symbol(category), _timestamp2unix(time))=#


#Base.:(==)(a::FileKey, b::FileKey) = a.setup == b.setup && a.run == b.run && a.time == b.time && a.category == b.category

function Base.isless(a::FileKey, b::FileKey)
    isless(a.setup, b.setup) || isequal(a.setup, b.setup) && (
        isless(a.period, b.period) || isequal(a.period, b.period) && (
            isless(a.run, b.run) || isequal(a.run, b.run) && (
                isless(a.category, b.category) || isequal(a.category, b.category) && (
                    isless(a.time, b.time)
                )
            )
        )
    )
end


#l200-p02-r006-cal-20221226T200846Zp([0-9]{2})
const _filekey_expr = r"^([a-z][a-z0-9]*)-p([0-9]{2})-r([0-9]{3})-([a-z]+)-([0-9]{8}T[0-9]{6}Z)$"
const _filekey_relaxed_expr = r"^([a-z][a-z0-9]*)-p([0-9]{2})-r([0-9]{3})-([a-z]+)-([0-9]{8}T[0-9]{6}Z)(-.*)?$"

_is_filekey_string(s::AbstractString) = occursin(_filekey_expr, s)

@inline FileKey(filekey::FileKey) = filekey

_can_convert_to(::Type{FileKey}, s::AbstractString) = !isnothing(match(_filekey_relaxed_expr, basename(s)))

function FileKey(s::AbstractString)
    m = match(_filekey_relaxed_expr, basename(s))
    if (m == nothing)
        throw(ArgumentError("String \"$s\" does not represent a valid file key or a compatible filename"))
    else
        x = (m::RegexMatch).captures
        FileKey(
            ExpSetup(Symbol(x[1])),
            DataPeriod(parse(Int, x[2])),
            DataRun(parse(Int, x[3])),
            DataCategory(Symbol(x[4])),
            Timestamp(x[5])
        )
    end
end

Base.convert(::Type{FileKey}, s::AbstractString) = FileKey(s)


function Base.print(io::IO, key::FileKey)
    print(io, key.setup)
    print(io, "-", DataPeriod(key))
    print(io, "-", DataRun(key))
    print(io, "-", DataCategory(key))
    print(io, "-", Timestamp(key))
end

Base.show(io::IO, key::FileKey) = print(io, "FileKey(\"$(string(key))\")")

ExpSetup(key::FileKey) = ExpSetup(key.setup)

DataPeriod(key::FileKey) = DataPeriod(key.period)
filekey_period_str(key::FileKey) = string(DataPeriod(key))

DataRun(key::FileKey) = DataRun(key.run)
filekey_run_str(key::FileKey) = string(DataRun(key))

DataCategory(key::FileKey) = DataCategory(key.category)

Timestamp(key::FileKey) = Timestamp(key.time)
Dates.DateTime(key::FileKey) = DateTime(Timestamp(key))


"""
    FileKeyLike = Union{FileKey, AbstractString}

Anything that can represent a file key, like
`FileKey("l200-p02-r006-cal-20221226T200846Z")` or
`"l200-p02-r006-cal-20221226T200846Z"`.
"""
const FileKeyLike = Union{FileKey, AbstractString}
export FileKeyLike


"""
    read_filekeys(filename::AbstractString)::AbstractVector{FileKey}

Reads a list of [`FileKey`](@ref) from a text file, one file key per line.

Ignores empty lines. `#` may be used to start a comment in the file.
"""
function read_filekeys(filename::AbstractString)
    lines = filter(!isempty, [strip(first(split(l, '#'))) for l in readlines(filename)])
    filtered_lines = strip.(filter(l -> !isempty(l) && !startswith(l, "#"), lines))
    return FileKey.(filtered_lines)
end
export read_filekeys


"""
    write_filekeys(filename::AbstractString, filekeys::AbstractVector{<:FileKey})

Writes a list of [`FileKey`](@ref) to a text file, one file key per line.
"""
function write_filekeys(filename::AbstractString, filekeys::AbstractVector{<:FileKey})
    open(filename, "w") do io
        for key in filekeys
            print(io, key, "\n")
        end
    end
end
export write_filekeys



"""
    struct ChannelId <: DataSelector

Represents a LEGEND data channel.

Example:

```julia
ch = ChannelId(1083204)
# ch = ChannelId(98) # with old channel numbering
ch.no == 1083204
string(ch) == "ch1083204"
ChannelId("ch1083204") == ch
```
"""
struct ChannelId <: DataSelector
    no::Int
end
export ChannelId

@inline ChannelId(ch::ChannelId) = ch

Base.:(==)(a::ChannelId, b::ChannelId) = a.no == b.no
Base.isless(a::ChannelId, b::ChannelId) = isless(a.no, b.no)

function Base.print(io::IO, ch::ChannelId)
    if ch.no < 1000
        @printf(io, "ch%03d", ch.no)
    else
        @printf(io, "ch%03d", ch.no)
    end
end

const ch_expr = r"^ch([0-9]{3}|[0-9]{7})$"

_can_convert_to(::Type{ChannelId}, s::AbstractString) = !isnothing(match(ch_expr, s))

function ChannelId(s::AbstractString)
    m = match(ch_expr, s)
    if (m == nothing)
        throw(ArgumentError("String \"$s\" does not look like a valid file LEGEND data channel name"))
    else
        ChannelId(parse(Int, (m::RegexMatch).captures[1]))
    end
end

Base.convert(::Type{ChannelId}, s::AbstractString) = ChannelId(s)


"""
    ChannelIdLike = Union{ChannelId, Integer, AbstractString}

Anything that can represent a data channel, like `ChannelId(1083204)` or
"ch1083204".
"""
ChannelIdLike = Union{ChannelId, AbstractString}
export ChannelIdLike



"""
    struct DetectorId <: DataSelector

Represents a LEGEND detector id id.

Example:

```julia
detector = DetectorId(:V99000A)
detector.label == :V99000A
string(detector) == "V99000A"
DetectorId("V99000A") == detector
```
"""
struct DetectorId <: DataSelector
    label::Symbol
end
export DetectorId

@inline DetectorId(detector::DetectorId) = detector

Base.:(==)(a::DetectorId, b::DetectorId) = a.label == b.label
Base.isless(a::DetectorId, b::DetectorId) = isless(a.label, b.label)

const detectorid_expr = r"^([A-Z][A-Z0-9]+)$"

_can_convert_to(::Type{DetectorId}, s::AbstractString) = !isnothing(match(detectorid_expr, s))

function DetectorId(s::AbstractString)
    _can_convert_to(DetectorId, s) || throw(ArgumentError("String \"$s\" does not look like a valid file LEGEND detector id"))
    length(s) < 4 && throw(ArgumentError("String \"$s\" is too short to be a valid LEGEND detector id"))
    length(s) > 7 && throw(ArgumentError("String \"$s\" is too long to be a valid LEGEND detector id"))
    DetectorId(Symbol(s))
end

Base.convert(::Type{DetectorId}, s::AbstractString) = DetectorId(s)
Base.convert(::Type{DetectorId}, s::Symbol) = DetectorId(s)

# ToDo: Improve implementation
Base.print(io::IO, detector::DetectorId) = print(io, detector.label)


"""
    DetectorIdLike = Union{DetectorId, Symbol, AbstractString}

Anything that can represent a detector id.
"""
const DetectorIdLike = Union{DetectorId, Symbol, AbstractString}
export DetectorIdLike
