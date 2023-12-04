
@enum PARTITION_METHOD degree annealing

# This function takes in a table and outputs an approximate
function _degree_based_partition(data::Vector{Vector{String}}, order::Int)
    attribute_set = collect(1:length(data[1]))
    Xs = Set([X for X in combinations(attribute_set, order)])
    degrees = counter(Any)
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
        partition_degrees = counter(Any)
        for tuple in partition
            inc!(partition_degrees, tuple[X])
        end
        partition_dc = maximum(values(partition_degrees))
        partition_dcs[X] = partition_dc
        max_gdc = max(partition_dc, max_gdc)
    end

    regular_dcs = Dict()
    for X in Xs
        degrees = counter(Any)
        for tuple in data
            inc!(degrees, tuple[X])
        end
        regular_dcs[X] = maximum(values(degrees))
    end

    return (max_gdc=max_gdc, partition_dcs=partition_dcs, regular_dcs=regular_dcs, partitions=partitions)
end

function _annealing_based_partition(data::Vector{Vector{String}}, order::Int)
    attribute_set = collect(1:length(data[1]))
    Xs = Set([X for X in combinations(attribute_set, order)])
    degrees = counter(Any)
    value_to_tuples = Dict()
    for i in 1:length(data)
        tuple = data[i]
        for X in Xs
            value_to_tuples[tuple[X]] = push!(get(value_to_tuples, tuple[X], []), i)
            inc!(degrees, tuple[X])
        end
    end

    value_queue = PriorityQueue{Any, Int}()
    for (value, degree) in degrees
        value_queue[value] = degree
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
                filter!(e->e≠i, value_to_tuples[neighbor_val])

                value_to_tuples[tuple[X]] = push!(get(value_to_tuples, tuple[X], []), i)
                dec!(degrees, neighbor_val)
                if neighbor_val in keys(value_queue)
                    value_queue[neighbor_val] = degrees[neighbor_val]
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
        partition_degrees = counter(Any)
        for tuple in partition
            inc!(partition_degrees, tuple[X])
        end
        partition_dc = maximum(values(partition_degrees))
        partition_dcs[X] = partition_dc
        max_gdc = max(partition_dc, max_gdc)
    end

    regular_dcs = Dict()
    for X in attribute_set
        degrees = counter(Any)
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
        new_tuple = [string(tuple[i]) * "_" * string(i) for i in eachindex(tuple)]
        push!(new_data, new_tuple)
    end
    return new_data
end

function get_constraint_and_partition(data, order::Int; method::PARTITION_METHOD=degree)
    data::Vector{Vector{String}} = make_attributes_disjoint(data)
    if method == degree
        return _degree_based_partition(data, order)
    elseif method == annealing
            return _annealing_based_partition(data, order)
    else
        throw(ErrorException(string(method) * " Not Implemented"))
    end
end
