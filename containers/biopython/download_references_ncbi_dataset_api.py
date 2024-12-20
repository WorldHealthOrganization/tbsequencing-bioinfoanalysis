#snpEff has a slight problem when pseudogene types of a GFF lacks the qualifier "gene"
#we go through the original GFF and add this qualifier where it is needed
#We also need to change the type tRNA to rRNA otherwise these genes are interpreted as protein coding by snpEff
import gffutils, sqlite3, argparse, json, zipfile, requests, io, os
from BCBio import GFF
from Bio import SeqIO


def return_genome_zip(genome_accession_id):
    req = requests.get("https://api.ncbi.nlm.nih.gov/datasets/v1/genome/accession/"+genome_accession_id+"/download?exclude_sequence=false&include_annotation_type=GENOME_GFF&hydrated=FULLY_HYDRATED", stream=True)
    return(zipfile.ZipFile(io.BytesIO(req.content)))

def extract_file_from_zip_object(zip_object, genome_accession_id, file_id):
    return(zip_object.read("ncbi_dataset/data/"+genome_accession_id+"/"+file_id))

def main(arguments):

    zip_object = return_genome_zip(arguments.genome_accession_id)

    fasta_entry = extract_file_from_zip_object(zip_object, arguments.genome_accession_id, arguments.genome_accession_id+"_"+arguments.assembly_accession_id+"_genomic.fna")

    folder = "." if not arguments.output_folder.strip("/") else arguments.output_folder.strip("/")

    if not os.path.exists(folder):
        os.makedirs(folder)
    with open(folder+"/"+arguments.genome_accession_id+"_"+arguments.assembly_accession_id+".fna", "wb") as fasta_file:
        fasta_file.write(fasta_entry)

    gff_entry = extract_file_from_zip_object(zip_object, arguments.genome_accession_id, "genomic.gff")
    gff_file = folder+"/"+arguments.genome_accession_id+".gff"
    with open(gff_file, "wb") as f:
        f.write(gff_entry)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--genome_accession_id", default="GCF_000195955.2")
    parser.add_argument("--assembly_accession_id", default="ASM19595v2")
    parser.add_argument("--output_folder", default="references/")
    args = parser.parse_args()

    main(args)