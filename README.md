# Bioinformatic processing

The repository holds definition for three different components of the bioinformatic processing:

1. The infrastructure terraform code
2. Docker image definition used for sequencing data processing
3. PySpark ETL jobs for post processing

You can check our GitHub Actions workflow in this repository for deploying each component.

## Infrastructure
Use terraform as usual to deploy the bioinformatic specific infrastructure. It will include:

* Step Function workflows to:
    1. transform raw sequencing data into genotype call and quality control values
    2. insert the calls into the RDS database
    3. annotate the new variants
    4. update the agregated statistics shown on the web site
* Glue jobs necessary for above step 4.
* An eventbridge scheduled rule that will trigger the processing of new raw sequencing data every 3 days. Disabled by default

## Docker images
Specific open source bioinformatic tools will be needed for sequencing data analysis. These will need to be pushed in each of their respective AWS ECR that have been created by the main [infrastructure](https://github.com/finddx/tbsequencing-infrastructure) repository.

