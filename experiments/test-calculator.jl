include("../src/GeneralDegreeConstraints.jl")


datasets = ["data/wordnet.csv",
            "data/yeast.csv",
            "data/aids.csv",
            "data/dblp.csv",
            "data/postLinks.csv",
            "data/comments.csv",
            "data/movie_companies.csv",
            "data/movie_keyword.csv",
            "data/movie_info.csv",
            "data/cast_info.csv",
            "data/census13.csv",
            "data/forest10.csv",
            "data/power7.csv",
            "data/dmv11.csv",
            ]
datasets = ["data/wordnet.csv",
"data/yeast.csv",
"data/aids.csv",
"data/dblp.csv",
"data/postLinks.csv",
"data/comments.csv",
"data/movie_companies.csv",
"data/census13.csv",
"data/forest10.csv",
"data/power7.csv",
]

for dataset in datasets
    data = csv_to_vec_of_vec(dataset)
    println("Dataset: ", dataset)
    println("GREEDY:")
    result = get_constraint_and_partition(data, 1; method=greedy)
    println("Max GDC: ", result.max_gdc)
    println("Min DC: ", minimum(values(result.regular_dcs)))
    println("Regular DCs: ", result.regular_dcs)
    println("Partition DCs: ", result.partition_dcs)
    println("Partition sizes: ", [x=>length(p) for (x, p) in result.partitions])
    println("Total Tuples: ", sum([length(p) for (x, p) in result.partitions]))
    println("OPTIMAL:")
    result = get_constraint_and_partition(data, 1; method=optimal)
    println("Max GDC: ", result.max_gdc)
    println("Min DC: ", minimum(values(result.regular_dcs)))
    println("Regular DCs: ", result.regular_dcs)
    println("Partition DCs: ", result.partition_dcs)
    println("Partition sizes: ", [x=>length(p) for (x, p) in result.partitions])
    println("Total Tuples: ", sum([length(p) for (x, p) in result.partitions]))
end
