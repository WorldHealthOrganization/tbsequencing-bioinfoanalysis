import hail as hl
import sys
import argparse
import prepare_gene_models
import prepare_datasets
import combine_datasets
import write_results_files

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gtf")
    parser.add_argument("--gene_data")
    parser.add_argument("--variant_results")
    parser.add_argument("--variant_annotation")
    parser.add_argument("--genes", nargs="+")
    args = parser.parse_args()


    hl.init()
    hl.ReferenceGenome("h37Rv", ["NC_000962.3"], {"NC_000962.3":4411532})
    gtf = sys.argv[1]
    gene_data = sys.argv[2]
    variant_results = sys.argv[3]
    variant_annotation = sys.argv[4]
    staging_folder = "staging_folder"
    output_folder = "output"
    prepare_gene_models.prepare_gene_models(args.gtf, staging_folder)
    prepare_datasets.prepare_dataset(args.gene_data, args.variant_results, args.variant_annotation, staging_folder)
    combine_datasets.combine_datasets(staging_folder, "tb").write(staging_folder+"/combined.ht")
    write_results_files.write_data_files(staging_folder +"/combined.ht", output_folder, args.genes)

