# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


const _always_everywhere_code::Expr = quote
    import LegendDataManagement
end

"""
    always_everywhere(expr)

Runs `expr` on all current Julia processes, but also all future Julia
processes added via [`legend_addprocs`](@ref)).

Similar to `Distributed.everywhere`, but also stores `expr` so that
`legend_addprocs` can execute it automatically on new worker processes.
"""
macro always_everywhere(expr)
    return quote
        expr = $(esc(Expr(:quote, expr)))
        push!(_always_everywhere_code.args, expr)
        _run_on_procs(expr, Distributed.procs())
    end
end
export @always_everywhere


function _run_on_procs(expr, procs::AbstractVector{<:Integer})
    mod_expr = Expr(:toplevel, :(task_local_storage()[:SOURCE_PATH] = $(get(task_local_storage(), :SOURCE_PATH, nothing))), expr)
    Distributed.remotecall_eval(Main, procs, mod_expr)
end

function _run_always_everywhere_code(@nospecialize(procs::AbstractVector{<:Integer}))
    _run_on_procs(_always_everywhere_code, procs)
end


"""
    pinthreads_default()

Use default thread-pinning strategy.
"""
function legend_pinthreads()
    if Distributed.myid() == 1
        let n_juliathreads = Base.Threads.nthreads()
            if n_juliathreads > 1
                LinearAlgebra.BLAS.set_num_threads(n_juliathreads)
            end
        end
    else
        let available_cpus = ThreadPinning.affinitymask2cpuids(ThreadPinning.get_affinity_mask())
            ThreadPinning.pinthreads(:affinitymask)
            LinearAlgebra.BLAS.set_num_threads(length(available_cpus))
        end
    end
end
export legend_pinthreads


"""
    LegendDataManagement.legend_distributed_pinthreads(procs::AbstractVector{<:Integer})

Use default thread-pinning strategy on processes `procs`.
"""
function legend_distributed_pinthreads(@nospecialize(procs::AbstractVector{<:Integer}))
    if 1 in procs
        legend_pinthreads()
    end

    workerprocs = filter(!isequal(1), procs)
    if !isempty(workerprocs)
        Distributed.remotecall_eval(Main, workerprocs,
            quote
                import LegendDataManagement
                LegendDataManagement.legend_pinthreads()
            end
        )
    end
end


"""
    LegendDataManagement.shutdown_workers_atexit()

Ensure worker processes are shut down when Julia exits.
"""
function shutdown_workers_atexit()
    atexit(() -> Distributed.rmprocs(filter!(!isequal(1), Distributed.workers()), waitfor = 1))
end


"""
    LegendDataManagement.worker_resources

Get the distributed Julia process resources currently available.
"""
function worker_resources()
    resources_ft = Distributed.remotecall.(LegendDataManagement._current_process_resources, Distributed.workers())
    resources = fetch.(resources_ft)
    sorted_resources = sort(resources, by = x -> x.workerid)
    StructArray(sorted_resources)
end

function _current_process_resources()
    return (
        workerid = Distributed.myid(),
        hostname = Base.gethostname(),
        nthreads = Base.Threads.nthreads(),
        blas_nthreads = LinearAlgebra.BLAS.get_num_threads(),
        cpuids = ThreadPinning.getcpuids()
    )
end


"""
    legend_addprocs()
    legend_addprocs(nprocs::Integer)

Add Julia worker processes for LEGEND data processing.

Automatically chooses between

* Adding processes on the current host
* Adding processes via SLURM (when in an `salloc` or `sbatch`) environment

Ensures that all workers processes use the same Julia project environment as
the current process. Requires that file systems paths are consistenst
across compute hosts.

Use [`@always_everywhere`](@ref) to run initialization code on all current
processes and all future processes added via `legend_addprocs`:

```julia
using Distributed, LegendDataManagement

@always_everywhere begin
    using SomePackage
    import SomeOtherPackage

    get_global_value() = 42
end

# ... some code ...

legend_addprocs()

# `get_global_value` is available even though workers were added later:
remotecall_fetch(get_global_value, last(workers()))
```

See also [`LegendDataManagement.worker_resources()`](@ref) and
[`LegendDataManagement.shutdown_workers_atexit()`](@ref).
"""
function legend_addprocs end
export legend_addprocs

legend_addprocs(; kwargs...) = _default_addprocs()(; kwargs...)
legend_addprocs(@nospecialize(nprocs::Integer); kwargs...) = _default_addprocs()(Int(nprocs); kwargs...)
legend_addprocs(remote_procs::Vector{<:Tuple}; kwargs...) = _addprocs_localhost(remote_procs; kwargs...)

function _default_addprocs()
    if haskey(ENV, "SLURM_JOB_ID") && !haskey(ENV, "SLURM_STEP_ID")
        # In salloc- or sbatch-spawned environment, but not within srun:
        return _addprocs_slurm
    else
        return _addprocs_localhost
    end
end


_addprocs_localhost(; kwargs...) = _addprocs_localhost(1; kwargs...)

function _addprocs_localhost(
    nprocs; 
    job_file_loc::AbstractString = joinpath(homedir(), "slurm-julia-output"),
    retry_delays::AbstractVector{<:Real} = [1, 1, 2, 2, 4, 5, 5, 10, 10, 10, 10, 20, 20, 20],
    env_args::Vector{Pair{String, String}}=Pair{String, String}[]
)
    @info "Adding $nprocs Julia processes on current host"

    # Maybe wait for shared/distributed file system to get in sync?
    # sleep(5)

    julia_project = dirname(Pkg.project().path)
    worker_nthreads = if hakey(ENV, "JULIA_NUM_THREADS")
            parse(Int, ENV["JULIA_NUM_THREADS"])
        else
            Base.Threads.nthreads()
        end
    heapsize_hint = if haskey(ENV, "JULIA_HEAP_SIZE_HINT")
            "--heap-size-hint=$(ENV["JULIA_HEAP_SIZE_HINT"])"
        else
            ""
        end

    new_workers = Distributed.addprocs(
        nprocs,
        exeflags = `--project=$julia_project --threads=$worker_nthreads $heapsize_hint`,
        topology = :master_worker, env = env_args
    )

    @info "Configuring $nprocs new Julia worker processes"

    _run_always_everywhere_code(new_workers)

    # Sanity check:
    worker_ids = Distributed.remotecall_fetch.(Ref(Distributed.myid), Distributed.workers())
    @assert length(worker_ids) == Distributed.nworkers()

    @info "Added $(length(new_workers)) Julia worker processes on current host"
end


function _addprocs_slurm(; kwargs...)
    slurm_ntasks = parse(Int, ENV["SLURM_NTASKS"])
    slurm_ntasks > 1 || throw(ErrorException("Invalid nprocs=$slurm_ntasks inferred from SLURM environment"))
    _addprocs_slurm(slurm_ntasks; kwargs...)
end

function _addprocs_slurm(
    nprocs::Int;
    job_file_loc::AbstractString = joinpath(homedir(), "slurm-julia-output"),
    retry_delays::AbstractVector{<:Real} = [1, 1, 2, 2, 4, 5, 5, 10, 10, 10, 10, 20, 20, 20],
    env_args::Vector{Pair{String, String}}=Pair{String, String}[]
)
    @info "Adding $nprocs Julia processes via SLURM"

    julia_project = dirname(Pkg.project().path)
    slurm_ntasks = nprocs
    slurm_nthreads = parse(Int, ENV["SLURM_CPUS_PER_TASK"])
    slurm_mem_per_cpu = _parse_bytes(ENV["SLURM_MEM_PER_CPU"])
    slurm_mem_per_task = slurm_nthreads * slurm_mem_per_cpu

    cluster_manager = LegendDataManagement.SlurmManager(slurm_ntasks, retry_delays)
    worker_timeout = round(Int, max(sum(cluster_manager.retry_delays), 60))
    ENV["JULIA_WORKER_TIMEOUT"] = "$worker_timeout"
    
    mkpath(job_file_loc)
    new_workers = Distributed.addprocs(
        cluster_manager, job_file_loc = job_file_loc,
        exeflags = `--project=$julia_project --threads=$slurm_nthreads --heap-size-hint=$(slurm_mem_per_task÷2)`,
        cpus_per_task = "$slurm_nthreads", mem_per_cpu="$slurm_mem_per_cpu", unbuffered="",
        mem_bind = "local", cpu_bind="cores", env=env_args
    )

    @info "Configuring $nprocs new Julia worker processes"

    _run_always_everywhere_code(new_workers)
    legend_distributed_pinthreads(new_workers)

    @info "Added $(length(new_workers)) Julia worker processes via SLURM"
end


function _parse_bytes(bytes_str::String; round_to::String="MB")
    # assume string is in convert_to unit when it doesn't have a unit
    if !isnothing(tryparse(Int, bytes_str))
        return parse(Int, bytes_str)
    end
    # otherwise convert to conert_to unit
    k = 1024
    sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    bytes = parse(Int, match(r"(\d+)", bytes_str).match)
    bytes_unit = match(r"([A-Za-z]+)", bytes_str).match
    i = findfirst(occursin.(sizes, bytes_unit)) - findfirst(occursin.(sizes, round_to))
    return convert(Int, round(bytes * k^i, digits=0))
end