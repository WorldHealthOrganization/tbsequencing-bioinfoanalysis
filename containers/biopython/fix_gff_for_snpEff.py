#snpEff has a slight problem when pseudogene types of a GFF lacks the qualifier "gene"
#we go through the original GFF and add this qualifier where it is needed
#We also need to change the type tRNA to rRNA otherwise these genes are interpreted as protein coding by snpEff
import gffutils, sqlite3, argparse, json, io, os
import download_references_ncbi_dataset_api
from BCBio import GFF
from Bio import SeqIO


def main(arguments):
    zip_object = download_references_ncbi_dataset_api.return_genome_zip(arguments.genome_accession_id)

    fasta_entry = download_references_ncbi_dataset_api.extract_file_from_zip_object(zip_object, arguments.genome_accession_id, arguments.genome_accession_id+"_"+arguments.assembly_accession_id+"_genomic.fna")

    with open("sequences.fa", "wb") as fasta_file:
        fasta_file.write(fasta_entry)

    fasta_io = io.StringIO(fasta_entry.decode())

    for rec in SeqIO.parse(fasta_io, "fasta"):
        rec.seq = rec.seq[(len(rec.seq)-400):len(rec.seq)] + rec.seq[0:2000]
        rec.id="NC_000962.3_fake_dnaA"

        with open("sequences-fake.fa", "w") as output_handle:
            SeqIO.write(rec, output_handle, "fasta")


    gff_entry = download_references_ncbi_dataset_api.extract_file_from_zip_object(zip_object, arguments.genome_accession_id, "genomic.gff")

    gff_file = arguments.genome_accession_id+".gff"
    
    with open(gff_file, "wb") as f:
        f.write(gff_entry)

    IDs = []

    gff = GFF.parse(open(gff_file))
    for record in gff:
        for features in record.features:
            IDs.append(features.qualifiers["ID"])
            if IDs.count(features.qualifiers["ID"])>1:
                features.qualifiers["ID"] = [features.id]
            if features.type=="pseudogene":
                if "gene" not in features.qualifiers:
                    features.qualifiers["gene"]=features.qualifiers["locus_tag"]
            #This is necessary for the gnomad browser configuration
            #The GFF is converted to GTF using AGAT and we use the GeneID from NCBI
            #as identifier for each gene.
            #This parses only gene entries of the GFF, not CDS.
            try: 
                GeneID = features.qualifiers["Dbxref"][0].replace("GeneID:","")
                features.qualifiers["Dbxref_GeneID"]=GeneID
            except KeyError:
                pass

    with open(gff_file+"-tmp", "w") as tmp:
        GFF.write([record], tmp)

    try:
        db = gffutils.FeatureDB("dbGff")
        os.remove("dbGff")
    except ValueError :
        pass
    except TypeError:
        os.remove("dbGff")

    db = gffutils.create_db(gff_file + "-tmp", "dbGff", checklines=0)

    #This is a fix for snpEff annotation
    #Without this fix, tRNA genes are flagged as protein coding genes
    db.execute("UPDATE features SET featuretype=replace(featuretype, 'tRNA', 'rRNA')")

    # Correcting the start of gene Rv1129c following Claudio instructions
    # Actually not doing that for the moment
    #    if arguments.genome_accession_id=="GCF_000195955.2" and arguments.assembly_accession_id=="ASM19595v2":
    #        db.execute("UPDATE features SET end=replace(end, 1254534, 1254510)")
    
    with open("genes.gff", "w") as fixed_file:
        for f in db.all_features():
            fixed_file.write(str(f) + '\n')

    try:
        db = gffutils.FeatureDB("dbGff")
        os.remove("dbGff")
    except ValueError :
        pass
    except TypeError:
        os.remove("dbGff")

    db = gffutils.create_db(gff_file + "-tmp", "dbGff", checklines=0)

    db.execute("DELETE FROM features WHERE NOT (start==1 and end==1524)")

    db.execute("UPDATE features SET start=start+400, end=end+400, seqid='NC_000962.3_fake_dnaA'")

    with open("genes-fake.gff", "w") as fixed_file:
        for f in db.all_features():
            fixed_file.write(str(f) + '\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--genome_accession_id", help="Bucket location of CRyPTIC data file", default="GCF_000195955.2")
    parser.add_argument("--assembly_accession_id", help="Bucket location of CRyPTIC data file", default="ASM19595v2")
    args = parser.parse_args()

    main(args)
