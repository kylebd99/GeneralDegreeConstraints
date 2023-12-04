include("../src/GeneralDegreeConstraints.jl")

datasets = [
            "data/wordnet.csv",
            "data/yeast.csv",
            "data/aids.csv",
            "data/dblp.csv",
            "data/postLinks.csv",
            "data/comments.csv",
]

for dataset in datasets
    data = csv_to_vec_of_vec(dataset)
    println("Dataset: ", dataset)
    result = get_constraint_and_partition(data, 1; method=annealing)
    println("Max GDC: ", result.max_gdc)
    println("Regular DCs: ", result.regular_dcs)
    println("Partition DCs: ", result.partition_dcs)
    println("Partition sizes: ", [length(x) for x in values(result.partitions)])
end
