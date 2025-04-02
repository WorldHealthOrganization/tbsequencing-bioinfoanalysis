# WHO Owner
Owned by the Global Tuberculosis Programme, GTB, Geneva Switzerland. References: Carl-Michael Nathanson.

# Bioinformatic processing

The repository holds definition for three different components of the bioinformatic processing:

1. The infrastructure terraform code
2. Docker image definition used for sequencing data processing
3. PySpark ETL jobs for post processing

You can check our GitHub Actions workflow in this repository for deploying each component.

## Infrastructure
Use terraform as usual to deploy the bioinformatic specific infrastructure. It will include:


Eight Step Function States Machines that together enable the bioinformatic processing of the WGS Illumina data:

* Master, handling together all operations. Runs every day.
* Creation of all the temporary AWS resources necessary to run a batch of bioinformatic analysis
* Downloading of the references from NCBI (reference TB genome) after resources have been created
* Child, which handles the processing of a single sample
* Deletion of temporary AWS resources necessary for batched analysis
* Data insertion, which will insert all newly created data (stored in S3) into our RDS database, after all samples have been processed
* Variant Annotation, which will create and insert into the RDS database the annotation for the newly identified variants

## Master pipeline
1. Checks whether there are new samples to be processed (if not, stops there)
2. Creates all the temporary infrastructure necessary to process the samples
3. Download all necessary reference files from the NCBI Child pipeline

## Resources creation

1. Creates an FSx volume
2. Creates a launch template for EC2 instances so that the newly created FSx volume is mounted at start up
3. Creates a new Batch Computational Environment, which starts EC2 spot instances using the newly created launch template
4. Creates a new Batch Queue which is associated with the newly created Computational Environment 
5. Waits for FSx drive to be ready (around 15 minutes)

## Download references

1. Downloads the reference TB genome from the NCBI
2. Prepares all necessary indexes of the downloaded genome
3. Extracts, compresses, indexes all known theoretical variants from the RDS database

## Sample processing

1. Downloads the raw sequencing data (either from NCBI or from our S3 bucket where contributors upload their data to the tbsequencing portal)
2. Aligns to the reference
3. Performs taxonomy analysis
4. Identifies genetic variants (gatk HaplotypeCaller, bcftools, freebayes)
5. Calculate per gene and global sequencing QC stats
6. Identifies deletion (delly)

## Data insertion

Uses AWS Glue to insert from S3 files to RDS database:

* genotype (including deletion) calls
* per gene sequencing stats (median coverage etc)
* global summary sequencing stats
* taxonomy analysis stats


## Variant annotation

* Creates temporary resources
* Requests from the database new variants only (i.e. unannotated)
* Download references from the NCBI 
* Processes references to create SnpEff configuration files
* Annotates the new variants, transform them for loading into the database
* Normalizes the newly inserted data

## Calculate statistics pipeline

Updates all tbsequencing web views. Runs AWS Glue jobs that assign drug resistance predictions from genotype data for the new samples only


## Docker images
Specific open source bioinformatic tools will be needed for sequencing data analysis. These will need to be pushed in each of their respective AWS ECR that have been created by the main [infrastructure](https://github.com/finddx/tbsequencing-infrastructure) repository.

