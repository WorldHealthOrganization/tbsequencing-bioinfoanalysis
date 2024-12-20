#/bin/bash

sample_id=$1

rm -rf ${sample_id}/*.f*q ${sample_id}/*.f*q.gz ${sample_id}/*.bam ${sample_id}/*.bam.bai ${sample_id}/*.sam ${sample_id}/*.mpileup.gz ${sample_id}/*-kraken-full.txt