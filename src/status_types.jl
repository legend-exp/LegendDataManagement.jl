# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

"""
    @enum ProcessStatus

May be `succeeded`, `pending`, or `failed`.
"""
@enum ProcessStatus process_succeeded=1 process_failed=0 process_pending=-1 
export ProcessStatus, process_succeeded, process_failed, process_pending


function Base.show(io::IO, ::MIME"text/plain", status::ProcessStatus)
    if status == process_succeeded
        print(io, "Success")
    elseif status == process_failed
        print(io, "Failure")
    elseif status == process_pending
        print(io, "Pending")
    else
        @assert false
    end
end


# Markdown.plaininline uses MIME"text/plain" by default, so need to specialize:
Markdown.plaininline(io::IO, x::ProcessStatus) = show(io, MIME"text/markdown"(), x)

function Base.show(io::IO, ::MIME"text/markdown", status::ProcessStatus)
    if status == process_succeeded
        print(io, """<span style="color:green">Success</span>""")
        #print(io, "\$\${\\color{green}Success}\$\$")
    elseif status == process_failed
        print(io, """<span style="color:red">Failure</span>""")
        #print(io, "\$\${\\color{red}Failure}\$\$")
    elseif status == process_pending
        print(io, """<span style="color:yellow">Pending</span>""")
        #print(io, "\$\${\\color{yellow}Pending}\$\$")
    else
        @assert false
    end
end


function Base.show(io::IO, ::MIME"text/html", status::ProcessStatus)
    if status == process_succeeded
        print(io, """<span style="color:green">Success</span>""")
    elseif status == process_failed
        print(io, """<span style="color:red">Failure</span>""")
    elseif status == process_pending
        print(io, """<span style="color:yellow">Pending</span>""")
    else
        @assert false
    end
end
