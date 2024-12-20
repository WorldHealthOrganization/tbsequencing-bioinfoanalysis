#/bin/bash

sample_id=$1

bcftools view ${sample_id}/${sample_id}-delly.bcf -f \"PASS\" --include \\'ALT=\"<DEL>\"&GT!=\"0/0\"\\' | bcftools query -f\"${sample_id},%CHROM,%POS,%REF,DEL-[%INFO/END],delly,%QUAL,[%DR],[%RR],[%DV],[%RV],[%GT]\\\\n\" > deletion/${sample_id}-delly.tsv