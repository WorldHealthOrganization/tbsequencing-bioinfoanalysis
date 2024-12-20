#/bin/bash

sample_id=$1
reference_name=$2

bcftools mpileup --annotate FORMAT/AD,FORMAT/DP --threads 4 -Oz -f references/${reference_name}.fna -o ${sample_id}/${sample_id}.mpileup.gz ${sample_id}/${sample_id}.bam && bcftools call -m --threads 4 -Oz -o ${sample_id}/${sample_id}-bcftools.vcf.gz ${sample_id}/${sample_id}.mpileup.gz && bcftools view --exclude \"GT=\\'0/0\\'|GT=\\'./.\\'\" ${sample_id}/${sample_id}-bcftools.vcf.gz > ${sample_id}/${sample_id}-bcftools.vcf