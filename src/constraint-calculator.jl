
@enum PARTITION_METHOD greedy degree annealing optimal

# This function takes in a table and outputs an approximate
function _degree_based_partition(data::Vector{Vector{UInt}}, order::Int)
    attribute_set = collect(1:length(data[1]))
    Xs = Set([X for X in combinations(attribute_set, order)])
    degrees = counter(Vector{UInt})
    for tuple in data
        for X in Xs
            inc!(degrees, tuple[X])
        end
    end

    partitions = Dict(X=>[] for X in Xs)
    for tuple in data
        min_degree = Inf
        min_X = nothing
        for X in Xs
            if min_degree > degrees[tuple[X]]
                min_degree = degrees[tuple[X]]
                min_X = X
            end
        end
        push!(partitions[min_X], tuple)
    end

    partition_dcs = Dict()
    max_gdc = -1
    for (X, partition) in partitions
        partition_degrees = counter(Vector{UInt})
        for tuple in partition
            inc!(partition_degrees, tuple[X])
        end
        partition_dc = maximum(values(partition_degrees); init=0)
        partition_dcs[X] = partition_dc
        max_gdc = max(partition_dc, max_gdc)
    end

    regular_dcs = Dict()
    for X in Xs
        degrees = counter(Vector{UInt})
        for tuple in data
            inc!(degrees, tuple[X])
        end
        regular_dcs[X] = maximum(values(degrees))
    end

    return (max_gdc=max_gdc, partition_dcs=partition_dcs, regular_dcs=regular_dcs, partitions=partitions)
end

function _annealing_based_partition(data::Vector{Vector{UInt}}, order::Int)
    attribute_set = collect(1:length(data[1]))
    Xs = Set([X for X in combinations(attribute_set, order)])
    value_to_tuples = Dict{Vector{UInt}, Set{Int}}()
    for i in 1:length(data)
        tuple = data[i]
        for X in Xs
            value_to_tuples[tuple[X]] = push!(get(value_to_tuples, tuple[X], Set()), i)
        end
    end

    value_queue = PriorityQueue{Any, Int}()
    for (value, tuples) in value_to_tuples
        value_queue[value] = length(tuples)
    end

    value_order = Dict()
    cur_pos = 1
    while ! isempty(value_queue)
        min_val = dequeue!(value_queue)
        for i in value_to_tuples[min_val]
            tuple = data[i]
            for X in Xs
                neighbor_val = tuple[X]
                tuple[X] == min_val && continue
                delete!(value_to_tuples[neighbor_val], i)
                value_to_tuples[tuple[X]] = push!(get(value_to_tuples, tuple[X], []), i)
                if neighbor_val in keys(value_queue)
                    value_queue[neighbor_val] = length(value_to_tuples[neighbor_val])
                end
            end
        end
        value_order[min_val] = cur_pos
        cur_pos += 1
    end

    partitions = Dict(X=>[] for X in Xs)
    for tuple in data
        min_order = Inf
        min_X = nothing
        for X in Xs
            if min_order > value_order[tuple[X]]
                min_order = value_order[tuple[X]]
                min_X = X
            end
        end
        push!(partitions[min_X], tuple)
    end

    partition_dcs = Dict()
    max_gdc = -1
    for (X, partition) in partitions
        partition_degrees = counter(Vector{UInt})
        for tuple in partition
            inc!(partition_degrees, tuple[X])
        end
        partition_dc = maximum(values(partition_degrees);init=0)
        partition_dcs[X] = partition_dc
        max_gdc = max(partition_dc, max_gdc)
    end

    regular_dcs = Dict()
    for X in attribute_set
        degrees = counter(UInt)
        for tuple in data
            inc!(degrees, tuple[X])
        end
        regular_dcs[X] = maximum(values(degrees))
    end

    return (max_gdc=max_gdc, partition_dcs=partition_dcs, regular_dcs=regular_dcs, partitions=partitions)
end


function _greedy_partition(data::Vector{Vector{UInt}}, order::Int)
    best_gdc = Inf
    best_partitions = nothing
    best_partition_dcs = nothing
    regular_dcs = nothing
    for i in 1:1
        shuffle!(data)
        attribute_set = collect(1:length(data[1]))
        Xs = Set([X for X in combinations(attribute_set, order)])

        partitions = Dict(X=>[] for X in Xs)
        partition_counters = Dict(X=>counter(Vector{UInt}) for X in Xs)
        for tuple in data
            min_degree = Inf
            min_X = nothing
            for X in Xs
                if min_degree > partition_counters[X][tuple[X]]
                    min_degree = partition_counters[X][tuple[X]]
                    min_X = X
                end
            end
            inc!(partition_counters[min_X], tuple[min_X])
            push!(partitions[min_X], tuple)
        end

        partition_dcs = Dict()
        max_gdc = -1
        for (X, partition) in partitions
            partition_degrees = counter(Vector{UInt})
            for tuple in partition
                inc!(partition_degrees, tuple[X])
            end
            partition_dc = maximum(values(partition_degrees); init=0)
            partition_dcs[X] = partition_dc
            max_gdc = max(partition_dc, max_gdc)
        end

        regular_dcs = Dict()
        for X in Xs
            degrees = counter(Vector{UInt})
            for tuple in data
                inc!(degrees, tuple[X])
            end
            regular_dcs[X] = maximum(values(degrees))
        end
        if max_gdc < best_gdc
            best_gdc = max_gdc
            best_partition_dcs = partition_dcs
            best_partitions = partitions
        end
    end
    return (max_gdc=best_gdc, partition_dcs=best_partition_dcs, regular_dcs=regular_dcs, partitions=best_partitions)
end

function _process_augmenting_path(tuple, partition_tuple_sets, Xs, overall_min_degree)
    for X in Xs
        if !haskey(partition_tuple_sets[X], tuple[X])
            partition_tuple_sets[X][tuple[X]] = Set()
        end
        if length(partition_tuple_sets[X][tuple[X]]) < overall_min_degree
            push!(partition_tuple_sets[X][tuple[X]], tuple)
            return true
        end
    end

    valid_path_end = nothing
    XY_PAIR = Tuple{Union{Nothing, UInt},Vector{UInt}}
    xy_parent = Dict{XY_PAIR, XY_PAIR}()
    visited_X = Set{Tuple{UInt, UInt}}()
    frontier = Queue{XY_PAIR}()
    enqueue!(frontier, (nothing, tuple))
    xy_parent[(nothing, tuple)] = (nothing, tuple)
    while length(frontier) > 0 && isnothing(valid_path_end)
        cur_X, cur_y = dequeue!(frontier)
        for X in Xs
            # If we've seen it before, skip.
            ((X, cur_y[X]) in visited_X) && continue
            for new_y in partition_tuple_sets[X][cur_y[X]]
                # If we've seen it before, skip.
                if haskey(xy_parent, (X, new_y))
                    continue
                end
                # Check whether new_y is a good stopping place.
                for X2 in Xs
                    if !haskey(partition_tuple_sets[X2], new_y[X2])
                        partition_tuple_sets[X2][new_y[X2]] = Set()
                    end
                    if length(partition_tuple_sets[X2][new_y[X2]]) < overall_min_degree
                        valid_path_end = (X, new_y)
                        push!(partition_tuple_sets[X2][new_y[X2]], new_y)
                        break
                    end
                end
                xy_parent[(X, new_y)] = (cur_X, cur_y)
                (!isnothing(valid_path_end)) && break
                enqueue!(frontier, (X, new_y))
            end
            push!(visited_X, (X, cur_y[X]))
            (!isnothing(valid_path_end)) && break
        end
    end
    if isnothing(valid_path_end)
        return false
    end

    # Follow the BFS tree to find the augmenting path to our new tuple.
    full_path = []
    cur_X, cur_y = valid_path_end
    while !isnothing(cur_X)
        par_X, par_y = xy_parent[(cur_X, cur_y)]
        push!(full_path, (cur_X, par_y) => (cur_X, cur_y))
        cur_X, cur_y = par_X, par_y
    end

    # Do the adds & deletes implied by the path.
    for ((add_X, add_y), (del_X, del_y)) in full_path
        if !isnothing(del_X)
            delete!(partition_tuple_sets[del_X][del_y[del_X]], del_y)
        end
        push!(partition_tuple_sets[add_X][add_y[add_X]], add_y)
    end
    return true
end

function _optimal_partition(data::Vector{Vector{UInt}})
    data = deepcopy(data)
    attribute_set = collect(1:length(data[1]))
    Xs = Set([X[1] for X in combinations(attribute_set, 1)])
    tuples = Set()
    for (i, tuple) in enumerate(data)
        append!(tuple, i)
        @assert tuple ∉ tuples
        push!(tuples, tuple)
    end
    X_TYPE = UInt
    Y_TYPE = Vector{UInt}
    partition_tuple_sets = Dict{X_TYPE, Dict{X_TYPE, Set{Y_TYPE}}}(X=>Dict{X_TYPE, Set{Y_TYPE}}() for X in Xs)
    overall_min_degree = 0
    for tuple in data
        found_path = _process_augmenting_path(tuple, partition_tuple_sets, Xs, overall_min_degree)
        if !found_path
            overall_min_degree += 1
            X = rand(Xs)
            push!(partition_tuple_sets[X][tuple[X]], tuple)
        end
    end

    partitions = Dict(X=>Set{Y_TYPE}() for X in Xs)
    for X in Xs
        partitions[X] = union(values(partition_tuple_sets[X])...)
    end

    partition_dcs = Dict()
    max_gdc = -1
    for (X, partition) in partitions
        partition_degrees = counter(UInt)
        for tuple in partition
            inc!(partition_degrees, tuple[X])
        end
        partition_dc = maximum(values(partition_degrees); init=0)
        partition_dcs[X] = partition_dc
        max_gdc = max(partition_dc, max_gdc)
    end

    regular_dcs = Dict()
    for X in Xs
        degrees = counter(UInt)
        for tuple in data
            inc!(degrees, tuple[X])
        end
        regular_dcs[X] = maximum(values(degrees))
    end
    return (max_gdc=max_gdc, partition_dcs=partition_dcs, regular_dcs=regular_dcs, partitions=partitions)
end

function make_attributes_disjoint(data)
    new_data = []
    for tuple in data
        new_tuple = [hash(tuple[i]) + i for i in eachindex(tuple)]
        push!(new_data, new_tuple)
    end
    return new_data
end

function remove_key_attributes(data::Vector{Vector{UInt}})
    attribute_set = collect(1:length(data[1]))
    regular_dcs = Dict()
    for X in attribute_set
        degrees = counter(Any)
        for tuple in data
            inc!(degrees, tuple[X])
        end
        regular_dcs[X] = maximum(values(degrees))
    end

    non_key_attributes = []
    for (attribute, degree) in regular_dcs
        if degree > 2
            push!(non_key_attributes, attribute)
        end
    end

    new_data = []
    for tuple in data
        push!(new_data, tuple[non_key_attributes])
    end
    return new_data
end


function get_constraint_and_partition(data, order::Int; method::PARTITION_METHOD=degree)
    data::Vector{Vector{UInt}} = make_attributes_disjoint(data)
    data = remove_key_attributes(data)
    if method == degree
        return _degree_based_partition(data, order)
    elseif method == greedy
        return _greedy_partition(data, order)
    elseif method == annealing
            return _annealing_based_partition(data, order)
    elseif method == optimal
            @profile _optimal_partition(data)
            return _optimal_partition(data)
    else
        throw(ErrorException(string(method) * " Not Implemented"))
    end
end
