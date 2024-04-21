# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const _g_processops_lock = ReentrantLock()

const _g_always_everywhere_code::Expr = quote
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
        try
            lock(_g_processops_lock)
            expr = $(esc(Expr(:quote, expr)))
            push!(_g_always_everywhere_code.args, expr)
            _run_on_procs(expr, Distributed.procs())
        finally
            unlock(_g_processops_lock)
        end
    end
end
export @always_everywhere


function _run_on_procs(expr, procs::AbstractVector{<:Integer})
    mod_expr = Expr(:toplevel, :(task_local_storage()[:SOURCE_PATH] = $(get(task_local_storage(), :SOURCE_PATH, nothing))), expr)
    Distributed.remotecall_eval(Main, procs, mod_expr)
end

function _run_always_everywhere_code(@nospecialize(procs::AbstractVector{<:Integer}); pre_always::Expr = :())
    code = quote
        $pre_always
        $_g_always_everywhere_code
    end

    _run_on_procs(code, procs)
end


"""
    pinthreads_default()

Use default thread-pinning strategy.
"""
function legend_pinthreads()
    if Distributed.myid() == 1
        let n_juliathreads = nthreads()
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
        nthreads = nthreads(),
        blas_nthreads = LinearAlgebra.BLAS.get_num_threads(),
        cpuids = ThreadPinning.getcpuids()
    )
end


"""
    abstract type LegendDataManagement.AddProcsMode

Abstract supertype for worker process addition modes.

Subtypes must implement:

* [`LegendDataManagement.legend_addprocs(mode::SomeAddProcsMode)`](@ref)

and may want to specialize:

* [`LegendDataManagement.worker_init_code(mode::SomeAddProcsMode)`](@ref)
"""
abstract type AddProcsMode end


"""
    LegendDataManagement.worker_init_code(::AddProcsMode)::Expr

Get a Julia code expression to run on new worker processes even before
running [`@always_everywhere`](@ref) code on them.
"""
function worker_init_code end
worker_init_code(::AddProcsMode) = :()



"""
    legend_addprocs()
    legend_addprocs(mode::LegendDataManagement.AddProcsMode)
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

# In salloc- or sbatch-spawned environment, but not within srun:
_in_slurm_alloc() = haskey(ENV, "SLURM_JOB_ID") && !haskey(ENV, "SLURM_STEP_ID")

@noinline function legend_addprocs()
    if _in_slurm_alloc()
        return legend_addprocs(SlurmRun())
    else
        return legend_addprocs(AddProcsLocalhost())
    end
end

@noinline function legend_addprocs(nprocs::Integer)
    if _in_slurm_alloc()
        return legend_addprocs(SlurmRun(slurm_flags = `--ntasks=$nprocs`))
    else
        return legend_addprocs(AddProcsLocalhost())
    end
end



"""
    LegendDataManagement.AddProcsLocalhost(;
        nprocs::Integer = 1
    )

Mode to add `nprocs` worker processes on the current host.
"""
@kwdef struct AddProcsLocalhost <: AddProcsMode
    nprocs::Int = 1
end


function legend_addprocs(mode::AddProcsLocalhost)
    n_workers = mode.nprocs
    try
        lock(_g_processops_lock)

        @info "Adding $n_workers Julia processes on current host"

        # Maybe wait for shared/distributed file system to get in sync?
        # sleep(5)

        julia_project = dirname(Pkg.project().path)
        worker_nthreads = nthreads()

        new_workers = Distributed.addprocs(
            n_workers,
            exeflags = `--project=$julia_project --threads=$worker_nthreads`
        )

        @info "Configuring $n_workers new Julia worker processes"

        _run_always_everywhere_code(new_workers, pre_always = worker_init_code(mode))

        # Sanity check:
        worker_ids = Distributed.remotecall_fetch.(Ref(Distributed.myid), Distributed.workers())
        @assert length(worker_ids) == Distributed.nworkers()

        @info "Added $(length(new_workers)) Julia worker processes on current host"
    finally
        unlock(_g_processops_lock)
    end
end



"""
    LegendDataManagement.default_elastic_manager()
    LegendDataManagement.default_elastic_manager(manager::ClusterManagers.ElasticManager)

Get or set the default elastic cluster manager.
"""
function default_elastic_manager end

_g_elastic_manager::Union{Nothing, ClusterManagers.ElasticManager} = nothing

function default_elastic_manager()
    global _g_elastic_manager
    if isnothing(_g_elastic_manager)
        _g_elastic_manager = ClusterManagers.ElasticManager(addr=:auto, port=0, topology=:master_worker)
    end
    return _g_elastic_manager
end
    
function default_elastic_manager(manager::ClusterManagers.ElasticManager)
    global _g_elastic_manager
    _g_elastic_manager = manager
    return _g_elastic_manager
end



"""
    abstract type LegendDataManagement.ElasticAddProcsMode <: LegendDataManagement.AddProcsMode

Abstract supertype for worker process addition modes that use the
elastic cluster manager.

Subtypes must implement:

* [`LegendDataManagement.worker_start_command(mode::SomeElasticAddProcsMode, manager::ClusterManagers.ElasticManager)`](@ref)
* [`LegendDataManagement.start_elastic_workers(mode::SomeElasticAddProcsMode, manager::ClusterManagers.ElasticManager)`](@ref)

and may want to specialize:

* [`LegendDataManagement.elastic_addprocs_timeout(mode::SomeElasticAddProcsMode)`](@ref)
"""
abstract type ElasticAddProcsMode <: AddProcsMode end

"""
    LegendDataManagement.worker_start_command(
        mode::ElasticAddProcsMode,
        manager::ClusterManagers.ElasticManager = LegendDataManagement.default_elastic_manager()
    )::Tuple{Cmd,Integer}

Return the system command to start worker processes as well as the number of
workers to start.
"""
function worker_start_command end
worker_start_command(mode::ElasticAddProcsMode) = worker_start_command(mode, default_elastic_manager())


function _elastic_worker_startjl(manager::ClusterManagers.ElasticManager)
    cookie = Distributed.cluster_cookie()
    socket_name = manager.sockname
    address = string(socket_name[1])
    port = convert(Int, socket_name[2])
    """using ClusterManagers; ClusterManagers.elastic_worker("$cookie", "$address", $port)"""
end

const _default_addprocs_params = Distributed.default_addprocs_params()

_default_julia_cmd() = `$(_default_addprocs_params[:exename]) $(_default_addprocs_params[:exeflags])`
_default_julia_flags() = ``
_default_julia_project() = Pkg.project().path


"""
    LegendDataManagement.elastic_localworker_startcmd(
        manager::Distributed.ClusterManager;
        julia_cmd::Cmd = _default_julia_cmd(),
        julia_flags::Cmd = _default_julia_flags(),
        julia_project::AbstractString = _default_julia_project()
    )::Cmd

Return the system command required to start a Julia worker process, that will
connect to `manager`, on the current host.
"""
function elastic_localworker_startcmd(
    manager::Distributed.ClusterManager;
    julia_cmd::Cmd = _default_julia_cmd(),
    julia_flags::Cmd = _default_julia_flags(),
    julia_project::AbstractString = _default_julia_project()
)
    julia_code = _elastic_worker_startjl(manager)

    `$julia_cmd --project=$julia_project $julia_flags -e $julia_code`
end



"""
    LegendDataManagement.elastic_addprocs_timeout(mode::ElasticAddProcsMode)

Get the timeout in seconds for waiting for worker processes to connect.
"""
function elastic_addprocs_timeout end

elastic_addprocs_timeout(mode::ElasticAddProcsMode) = 60


"""
    LegendDataManagement.start_elastic_workers(mode::ElasticAddProcsMode, manager::ClusterManagers.ElasticManager)::Int

Spawn worker processes as specified by `mode` and return the number of
expected additional workers.
"""
function start_elastic_workers end


function legend_addprocs(mode::ElasticAddProcsMode)
    try
        lock(_g_processops_lock)

        manager = default_elastic_manager()

        old_procs = Distributed.procs()
        n_previous = length(old_procs)
        n_to_add = start_elastic_workers(mode, manager)

        @info "Waiting for $n_to_add workers to connect..."
    
        sleep(1)

        # ToDo: Add timeout and either prevent workers from connecting after
        # or somehow make sure that init and @always everywhere code is still
        # run on them before user code is executed on them.

        timeout = elastic_addprocs_timeout(mode)

        t_start = time()
        t_waited = zero(t_start)
        n_added_last = 0
        while true
            t_waited = time() - t_start
            if t_waited > timeout
                @error "Timeout after waiting for workers to connect for $t_waited seconds"
                break
            end
            n_added = Distributed.nprocs() - n_previous
            if n_added > n_added_last
                @info "$n_added of $n_to_add additional workers have connected"
            end
            if n_added == n_to_add
                break
            elseif n_added > n_to_add
                @warn "More workers connected than expected: $n_added > $n_to_add"
                break
            end

            n_added_last = n_added
            sleep(1)
        end

        new_workers = setdiff(Distributed.workers(), old_procs)
        n_new = length(new_workers)

        @info "Initializing $n_new new Julia worker processes"
        _run_always_everywhere_code(new_workers, pre_always = worker_init_code(mode))

        @info "Added $n_new new Julia worker processes"

        if n_new != n_to_add
            throw(ErrorException("Tried to add $n_to_add new workers, but added $n_new"))
        end
    finally
        unlock(_g_processops_lock)
    end
end


"""
    LegendDataManagement.ExternalProcs(;
        nprocs::Integer = ...
    )

Add worker processes by starting them externally.

Will log (via `@info`) a worker start command and then wait for the workers to
connect. The user is responsible for starting the specified number of workers
externally using that start command.

Example:

```julia
mode = ExternalProcs(nprocs = 4)
legend_addprocs(mode)
```

The user now has to start 4 Julia worker processes externally using the logged
start command. This start command can also be retrieved via
[`worker_start_command(mode)](@ref).
"""
@kwdef struct ExternalProcs <: ElasticAddProcsMode
    nprocs::Int = 1
end


function worker_start_command(mode::ExternalProcs, manager::ClusterManagers.ElasticManager)
    worker_nthreads = nthreads()
    julia_flags = `$(_default_julia_flags()) --threads=$worker_nthreads`
    elastic_localworker_startcmd(manager, julia_flags = julia_flags), mode.nprocs
end

function start_elastic_workers(mode::ExternalProcs, manager::ClusterManagers.ElasticManager)
    start_cmd, n_workers = worker_start_command(mode, manager)
    @info "To add Julia worker processes, run ($n_workers times in parallel, I'll wait for them): $start_cmd"
    return n_workers
end
