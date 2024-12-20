#/bin/bash

sample_id=$1
reference_name=$2

samtools view -C --reference references/${reference_name}.fna -o ${sample_id}/${sample_id}.cram ${sample_id}/${sample_id}.bam && samtools index ${sample_id}/${sample_id}.cram