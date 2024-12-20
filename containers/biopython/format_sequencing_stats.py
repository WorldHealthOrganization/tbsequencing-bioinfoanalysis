import sys, pandas

file_location = sys.argv[1]

sample_id = file_location.rsplit("/", 1)[1]

header_regions_qc = [
    "Median", 
    "Coverage10x",
    "Coverage15x",
    "Coverage20x",
    "Coverage30x"
]

global_stats_header = [
    "SampleId",
    "MedianDepth ",
    "Coverage10x",
    "Coverage15x",
    "Coverage20x",
    "Coverage30x",
    "RawTotalSequences",
    "FilteredSequences",
    "Sequences",
    "IsSorted",
    "FirstFragments",
    "LastFragments",
    "ReadsMapped",
    "ReadsMappedAndPaired",
    "ReadsUnmapped",
    "ReadsProperlyPaired",
    "ReadsPaired",
    "ReadsDuplicated",
    "ReadsMQ0",
    "ReadsQCFailed",
    "NonPrimaryAlignments",
    "SupplementaryAlignments",
    "TotalLength",
    "TotalFirstFragmentLength",
    "TotalLastFragmentLength",
    "BasesMapped",
    "BasesMappedCigar",
    "BasesTrimmed",
    "BasesDuplicated",
    "Mismatches",
    "ErrorRate",
    "AverageLength",
    "AverageFirstFragmentLength",
    "AverageLastFragmentLength",
    "MaximumLength",
    "MaximumFirstFragmentLength",
    "MaximumLastFragmentLength",
    "AverageQuality",
    "InsertSizeAverage",
    "InsertSizeStandardDeviation",
    "InwardOrientedPairs",
    "OutwardOrientedPairs",
    "PairsWithOtherOrientation",
    "PairsOnDifferentChromosomes",
    "PercentageOfProperlyPairedReads"
]

stats = pandas.read_csv(file_location+"_qcstats.txt", sep=",", header=None, names=header_regions_qc)

samtools_stats = []

with open(file_location+"-samtools-stats.txt", "r") as samtools_file:
    for line in [x for x in samtools_file.readlines() if x.startswith("SN")]:
        try:
            samtools_stats.append(int(line.split(":")[1].split("#")[0].strip()))
        except ValueError:
            samtools_stats.append(float(line.split(":")[1].split("#")[0].strip()))
rec = pandas.DataFrame.from_records(
    [
        [sample_id] + list(list(stats[header_regions_qc].itertuples(index=False, name=None))[0]) + samtools_stats
    ]
)

rec[[2, 3, 4, 5]] = rec[[2, 3, 4, 5]].round(4)

rec.columns = global_stats_header

rec.to_csv("global-stats/"+sample_id+"-stats.csv.gz", sep="\t", header=True, index=False, compression="gzip")

header_regions_bed = [
    "Chromosome", 
    "Start",
    "End",
    "Locus",
    "MeanDepth"
]

mean_cov = pandas.read_csv(file_location + ".regions.bed.gz", sep="\t", header=None, names=header_regions_bed, compression="gzip", index_col=3)

depth_locus = pandas.read_csv(file_location + ".thresholds.bed.gz", sep="\t", header=0, compression="gzip").rename(columns={"region": "Locus"}).set_index("Locus")

for i in ["10X", "15X", "20X", "30X"]:
    depth_locus[i] = depth_locus[i]/(depth_locus["end"]-depth_locus["start"])

mean_cov = mean_cov.join(depth_locus).reset_index()

mean_cov["Sample"] = sample_id

mean_cov[["Sample", "Locus", "MeanDepth", "10X", "15X", "20X", "30X"]].to_csv("locus-stats/"+sample_id+"-locusseqstats.csv.gz", sep="\t", header=False, index=False, compression="gzip")