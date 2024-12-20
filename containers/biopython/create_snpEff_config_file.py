import argparse

def main(arguments):
    reference = arguments.nucleotide_accession_id
    bacterial_codons = "codon.Bacterial_and_Plant_Plastid: TTT/F, TTC/F, TTA/L, TTG/L+, TCT/S, TCC/S, TCA/S, TCG/S, TAT/Y, TAC/Y, TAA/*, TAG/*, TGT/C, TGC/C, TGA/*, TGG/W, CTT/L, CTC/L, CTA/L, CTG/L+, CCT/P, CCC/P, CCA/P, CCG/P, CAT/H, CAC/H, CAA/Q, CAG/Q, CGT/R, CGC/R, CGA/R, CGG/R, ATT/I+, ATC/I+, ATA/I+, ATG/M+, ACT/T, ACC/T, ACA/T, ACG/T, AAT/N, AAC/N, AAA/K, AAG/K, AGT/S, AGC/S, AGA/R, AGG/R, GTT/V, GTC/V, GTA/V, GTG/V+, GCT/A, GCC/A, GCA/A, GCG/A, GAT/D, GAC/D, GAA/E, GAG/E, GGT/G, GGC/G, GGA/G, GGG/G"
    genome = reference + ".genome: " + reference
    chromosome = reference + ".chromosomes: " + reference
    codonTable = reference + "." + reference + ".codonTable: Bacterial_and_Plant_Plastid"
    text = bacterial_codons + "\n" + genome + "\n" + chromosome + "\n" + codonTable
    with open("snpEff_"+reference+".config", "w") as f:
        f.write(text)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nucleotide_accession_id", help="Bucket location of CRyPTIC data file", default="NC_000962.3")
    args = parser.parse_args()

    main(args)
