import argparse
import importlib
import os
import sys

import hail as hl

ALLOWED_RESULT_TYPES = {hl.tbool, hl.tfloat32, hl.tfloat64, hl.tint32, hl.tint64, hl.tstr}


def validate_gene_results_table(ds):
    assert ds.key.dtype.fields == ("gene_id",), "Table must be keyed by gene ID"

    assert "group_results" in ds.row_value.dtype.fields, "Table must have a 'group_results' field"

    assert isinstance(ds.group_results.dtype, hl.tdict), "'group_results' must be a dict"

    assert ds.group_results.dtype.key_type == hl.tstr, "'group_results' keys must be strings"

    assert isinstance(ds.group_results.dtype.value_type, hl.tstruct), "'group_results' value must be a struct"

    for typ in ds.group_results.dtype.value_type.types:
        assert (
            typ in ALLOWED_RESULT_TYPES
        ), f"'group_results' fields may only be one of {', '.join(map(str, ALLOWED_RESULT_TYPES))}"


def validate_variant_results_table(ds):
    assert ds.key.dtype.fields == ("locus", "alleles"), "Table must be keyed by locus and alleles"
    assert ds.locus.dtype in (hl.tlocus("GRCh37"), hl.tlocus("GRCh38"), hl.tlocus("h37Rv")), "'locus' must be a locus type"
    assert ds.alleles.dtype == hl.tarray(hl.tstr), "'alleles' must be an array of strings"

    required_fields = {
        "gene_id": hl.tstr,
        "consequence": hl.tstr,
        "hgvsc": hl.tstr,
        "hgvsp": hl.tstr,
    }
    for field, typ in required_fields.items():
        assert field in ds.row_value.dtype.fields, f"Missing required field '{field}'"
        assert ds[field].dtype == typ, f"{field} should be type {typ}"

    assert "group_results" in ds.row_value.dtype.fields, "Table must have a 'group_results' field"
    assert isinstance(ds.group_results.dtype, hl.tdict), "'group_results' must be a dict"
    assert ds.group_results.dtype.key_type == hl.tstr, "'group_results' keys must be strings"
    assert isinstance(ds.group_results.dtype.value_type, hl.tstruct), "'group_results' value must be a struct"

    for typ in ds.group_results.dtype.value_type.types:
        assert (
            typ in ALLOWED_RESULT_TYPES
        ), f"'group_results' fields may only be one of {', '.join(map(str, ALLOWED_RESULT_TYPES))}"

    assert isinstance(ds.info.dtype, hl.tstruct), "'info' must be a struct"
    for typ in ds.info.dtype.types:
        assert (
            typ in ALLOWED_RESULT_TYPES
        ), f"'info' fields may only be one of {', '.join(map(str, ALLOWED_RESULT_TYPES))}"


def prepare_variant_results(variant_results_path, variant_annotation_path):
    variant_results = hl.import_table(
        variant_results_path,
        force_bgz=True,
        min_partitions=100,
        key="ID",
        missing="NA",
        types={
            "ID": hl.tint,
            "ac_case": hl.tint,
            "ac_ctrl": hl.tint,
            "an_case": hl.tint,
            "an_ctrl": hl.tint,
            "af": hl.tfloat32,
            "odds_ratio": hl.tfloat32,
            "analysis_group": hl.tstr,
        },
    )


    variant_results = variant_results.group_by("ID").aggregate(
        group_results=hl.agg.collect(variant_results.row_value)
    )
    variant_results = variant_results.annotate(
        group_results=hl.dict(
            variant_results.group_results.map(
                lambda group_result: (group_result.analysis_group, group_result.drop("analysis_group"))
            )
        )
    )

    variant_annotations = hl.import_table(
        variant_annotation_path,
        force_bgz=True,
        min_partitions=100,
        key="ID",
        missing="NA",
        types={
            "ID": hl.tint,
            "Variant ID": hl.tstr,
            "Consequence": hl.tstr,
            "gene_id": hl.tstr,
            "HGVSc": hl.tstr,
            "HGVSp": hl.tstr,
        },
    )

    variant_annotations = variant_annotations.rename(
        {
            "Consequence": "csq",
            "HGVSc": "hgvsc",
            "HGVSp": "hgvsp",   
        }
    )
    print(variant_annotations.describe())
    variant_annotations = variant_annotations.select(
        "gene_id",
        consequence=variant_annotations.csq,
        variant_id=variant_annotations["Variant ID"],
        hgvsc=variant_annotations.hgvsc.split(":")[-1],
        hgvsp=variant_annotations.hgvsp.split(":")[-1],
        info=hl.struct(
            in_analysis=1,
            comment='',
            cadd=0
        ),
    )

    variants = variant_annotations.annotate(group_results=variant_results[variant_annotations.key].group_results)
   
    print(variants.describe())
    variants = variants.annotate(
        locus=hl.rbind(
            variants["variant_id"].split(":"), lambda p: hl.locus(p[0], hl.int(p[1]), reference_genome="h37Rv")
        ),
        alleles=hl.rbind(variants["variant_id"].split(":"), lambda p: [p[2], p[3]]),
    )
    variants = variants.key_by("locus", "alleles")
    variants = variants.drop("variant_id")
    return variants

def prepare_gene_results(gene_results_path):

    ds = hl.import_table(
        gene_results_path,
        delimiter=",",
        missing="NA",
        quote='"',
        types={
            "gene_id": hl.tstr,
            "analysis_group": hl.tstr,
            "susceptible_genotyped": hl.tint,
            "resistant_genotyped": hl.tint,
        },
    )
    ds = ds.group_by("gene_id").aggregate(group_results=hl.agg.collect(ds.row_value))
    ds = ds.annotate(
        group_results=hl.dict(
            ds.group_results.map(
                lambda group_result: (group_result.analysis_group, group_result.drop("gene_id", "analysis_group"))
            )
        )
    )
    return ds

def prepare_dataset(gene_results, variant_results, variant_annotation, staging_path):

    gene_results = prepare_gene_results(gene_results)
    validate_gene_results_table(gene_results)   
    gene_results.write(os.path.join(staging_path, "tb", "gene_results.ht"), overwrite=True)

    variant_results = prepare_variant_results(variant_results, variant_annotation)
    validate_variant_results_table(variant_results)
    variant_results.write(os.path.join(staging_path, "tb", "variant_results.ht"), overwrite=True)

