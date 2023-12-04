


function csv_to_vec_of_vec(filename::String)
    table = readdlm(filename, ',', Any, '\n')
    output_data = []
    for row in 1:size(table)[1]
        output_row = []
        for value in table[row, :]
            push!(output_row, value)
        end
        push!(output_data, output_row)
    end
    return output_data
end

function subgraph_to_csv(input_filename::String, output_filename::String)
    table = readdlm(input_filename, ' ', Any, '\n')
    output_data = []
    for i in 1:size(table)[1]
        row = table[i,:]
        if row[1] == "v" || row[1] == "t"
            continue
        end
        output_row = [row[2], row[3]]
        push!(output_data, output_row)
    end
    writedlm(output_filename, output_data, ",")
end
