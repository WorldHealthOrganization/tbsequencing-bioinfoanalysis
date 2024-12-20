#!/usr/bin/env python3

import csv
import json
from json.encoder import encode_basestring_ascii, _make_iterencode
import multiprocessing
import os
import sys

import hail as hl
from tqdm import tqdm

INFINITY = float("inf")


class ResultEncoder(json.JSONEncoder):
    """
    JSON encoder that supports Hail Structs and limits precision of floats.
    """

    def default(self, o):  # pylint: disable=method-hidden
        if isinstance(o, hl.Struct):
            return dict(o)

        return super().default(o)

    def iterencode(self, o, _one_shot=False):
        def floatstr(o, **kwargs):  # pylint: disable=unused-argument
            if o != o:
                return '"NaN"'
            elif o == INFINITY:
                return '"Infinity"'
            elif o == -INFINITY:
                return '"-Infinity"'

            return "{:.5g}".format(o)

        _iterencode = _make_iterencode(
            {},
            self.default,
            encode_basestring_ascii,
            None,  # indent,
            floatstr,
            ":",  # key_separator,
            ",",  # item_separator,
            False,  #  sort_keys,
            False,  #  skipkeys,
            _one_shot,
        )
        return _iterencode(o, 0)


def split_data(row):
    gene_id = row[0]
    gene = json.loads(row[1])
    all_variants = gene.pop("variants")
    gene_h37Rv = gene.pop("h37Rv")
    if gene_h37Rv:
        gene_h37Rv = {**gene, "reference_genome": "h37Rv", **gene_h37Rv}
        gene_h37Rv = json.dumps({"gene": gene_h37Rv}, cls=ResultEncoder)



    all_variants = {k: json.dumps({"variants": v}, cls=ResultEncoder) for k, v in all_variants.items()}

    return gene_id, gene_h37Rv, all_variants


def write_data_files(table_path, output_directory, genes=None):

    ds = hl.read_table(table_path)

    os.makedirs(output_directory, exist_ok=True)

    with open(f"{output_directory}/metadata.json", "w") as output_file:
        output_file.write(hl.eval(hl.json(ds.globals.meta)))

    gene_search_terms = ds.select(data=hl.json(hl.tuple([ds.gene_id, ds.search_terms])))
    gene_search_terms.key_by().select("data").export(f"{output_directory}/gene_search_terms.json.txt", header=False)
    os.remove(f"{output_directory}/.gene_search_terms.json.txt.crc")

    ds = ds.drop("search_terms")
    os.makedirs(f"{output_directory}/results", exist_ok=True)
    for dataset in ds.globals.meta.datasets.dtype.fields:
        reference_genome = "h37Rv"
        gene_results = ds.filter(hl.is_defined(ds.gene_results[dataset]))
        gene_results = gene_results.select(
            result=hl.tuple(
                [
                    gene_results.gene_id,
                    gene_results.gene_id,
                    gene_results.gene_id,
                    gene_results[reference_genome].chrom,
                    (gene_results[reference_genome].start + gene_results[reference_genome].stop) // 2,
                    gene_results.gene_results[dataset].group_results,
                ]
            )
        )
        gene_results = gene_results.collect()

        gene_results = [r.result for r in gene_results]

        with open(f"{output_directory}/results/{dataset.lower()}.json", "w") as output_file:
            output_file.write(json.dumps({"results": gene_results}, cls=ResultEncoder))

    if genes:
        ds = ds.filter(hl.set(genes).contains(ds.gene_id))

    temp_file_name = "temp.tsv"
    n_rows = ds.count()
    ds.select(data=hl.json(ds.row)).export(f"{output_directory}/{temp_file_name}", header=False)

    csv.field_size_limit(sys.maxsize)
    os.makedirs(f"{output_directory}/genes", exist_ok=True)

    with multiprocessing.get_context("spawn").Pool() as pool:
        with open(f"{output_directory}/{temp_file_name}") as data_file:

            reader = csv.reader(data_file, delimiter="\t")
            for gene_id, gene_h37Rv, all_variants in tqdm(pool.imap(split_data, reader), total=n_rows):
                num = int(gene_id)
                gene_dir = f"{output_directory}/genes/{str(num % 1000).zfill(3)}"
                os.makedirs(gene_dir, exist_ok=True)
                if gene_h37Rv:
                    with open(f"{gene_dir}/{gene_id}_h37Rv.json", "w") as out_file:
                        out_file.write(gene_h37Rv)


                for dataset, dataset_variants in all_variants.items():
                    if dataset_variants:
                        with open(f"{gene_dir}/{gene_id}_{dataset}_variants.json", "w") as out_file:
                            out_file.write(dataset_variants)

    os.remove(f"{output_directory}/{temp_file_name}")
    os.remove(f"{output_directory}/.{temp_file_name}.crc")


