import hail as hl
import sys


def get_exons(gencode):
    """
    Filter Gencode table to exons and format fields.
    """
    exons = gencode.filter(hl.set(["exon"]).contains(gencode.feature))
    exons = exons.select(
        feature_type=exons.feature,
        transcript_id=exons.transcript_id.split("\\.")[0],
        chrom=exons.interval.start.contig.replace("^chr", ""),
        strand=exons.strand,
        start=exons.interval.start.position,
        stop=exons.interval.end.position,
    )
    return exons


def get_genes(gencode):
    """
    Filter Gencode table to genes and format fields.
    """
    genes = gencode.filter(gencode.feature == "gene")
    genes = genes.select(
        gene_id=genes.Dbxref_GeneID,
        gene_locus_tag=genes.locus_tag,
        gene_name=genes.gene,
        gene_synonym=hl.array([genes.gene_synonym]),
        chrom=genes.interval.start.contig.replace("^chr", ""),
        strand=genes.strand,
        start=genes.interval.start.position,
        stop=genes.interval.end.position,
    )
    genes = genes.key_by(genes.gene_id).drop("interval")
   
    return genes


def get_transcripts(gencode):
    """
    Filter Gencode table to transcripts and format fields.
    """
    transcripts = gencode.filter(gencode.feature == "transcript")
    transcripts = transcripts.select(
        transcript_id=transcripts.transcript_id.split("\\.")[0],
        gene_id=transcripts.Dbxref_GeneID,
        chrom=transcripts.interval.start.contig.replace("^chr", ""),
        strand=transcripts.strand,
        start=transcripts.interval.start.position,
        stop=transcripts.interval.end.position,
    )
    
    transcripts = transcripts.key_by(transcripts.transcript_id).drop("interval")
    return transcripts


def load_gencode_gene_models(gtf_path, reference_genome):
    gencode = hl.experimental.import_gtf(
        gtf_path, reference_genome=reference_genome, min_partitions=100, skip_invalid_contigs=False
    )
    # Extract genes, transcripts, and exons from the GTF file
    genes = get_genes(gencode)
    transcripts = get_transcripts(gencode)
    exons = get_exons(gencode)
    exons = exons.cache()
    # Annotate transcripts with their exons
    transcript_exons = exons.group_by(exons.transcript_id).aggregate(exons=hl.agg.collect(exons.row_value))
    transcripts = transcripts.annotate(
        exons=transcript_exons[transcripts.transcript_id].exons.map(
            lambda exon: exon.select("feature_type", "start", "stop")
        )
    )

    # Annotate genes with their transcripts
    gene_transcripts = transcripts.key_by()
    gene_transcripts = gene_transcripts.group_by(gene_transcripts.gene_id).aggregate(
        transcripts=hl.agg.collect(gene_transcripts.row_value.drop("gene_id", "chrom"))
    )
    genes = genes.annotate(**gene_transcripts[genes.gene_id])
    genes = genes.cache()

    return genes


def load_canonical_transcripts(canonical_transcripts_path):
    # Canonical transcripts file is a TSV with two columns: gene ID and transcript ID and no header row
    canonical_transcripts = hl.import_table(canonical_transcripts_path, force=True, no_header=True, min_partitions=100)
    canonical_transcripts = canonical_transcripts.rename({"f0": "gene_id", "f1": "transcript_id"})
    canonical_transcripts = canonical_transcripts.key_by("gene_id")
    return canonical_transcripts




def prepare_gene_models_helper(reference_genome, gff_path):

    # Load genes from GTF file
    genes = load_gencode_gene_models(gff_path, reference_genome)
    genes = genes.distinct()
    genes = genes.transmute(gencode_gene_symbol=genes.gene_id)

    return genes




def prepare_gene_models(gtf_file, staging_path):
    h37Rv = prepare_gene_models_helper("h37Rv", gtf_file)
    h37Rv = h37Rv.select(h37Rv=h37Rv.row_value)
    # Annotate genes with information from HGNC
    # Collect all fields that can be used to search by gene symbol
    h37Rv = h37Rv.annotate(
        search_terms=hl.set(
            hl.empty_array(hl.tstr)
            .append(h37Rv.gene_id)
            .append(h37Rv.h37Rv.gene_locus_tag)
            .append(h37Rv.h37Rv.gene_name)
            .extend(hl.or_else(h37Rv.h37Rv.gene_synonym, hl.empty_array(hl.tstr)))
            .append(h37Rv.h37Rv.gencode_gene_symbol)
            .filter(hl.is_defined)
        ),
    )
    h37Rv.write(f"{staging_path}/gene_models.ht", overwrite=True)