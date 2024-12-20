#/bin/bash

sample_id=$(basename $1)
reference_name=$2
rm -f genotype/${sample_id}-genotype.csv*

for genotyper in freebayes gatk bcftools
do
    bcftools norm --fasta-ref $reference_name --multiallelics - $1-${genotyper}.vcf > $1-${genotyper}-normalized.vcf
    bcftools annotate --annotations references/variant_ids.tsv.gz --columns CHROM,POS,ID,REF,ALT $1-${genotyper}-normalized.vcf | \
      bcftools query -f "${genotyper},${sample_id},%CHROM,%POS,%ID,%REF,%ALT,%QUAL,[%AD],[%DP],[%GT]\n" | \
        sed "s/,\.,/,,/g" >> \
          genotype/${sample_id}-genotype.csv
done

gzip genotype/${sample_id}-genotype.csv