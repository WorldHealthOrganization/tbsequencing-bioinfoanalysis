module "glue" {
  source          = "git::https://github.com/finddx/seq-treat-tbkb-terraform-modules.git//glue?ref=glue-v1.2"
  glue_crawlers   = local.glue_crawlers
  glue_databases  = local.glue_databases
  glue_connection = local.glue_connection
  glue_classifier = local.glue_classifiers
  glue_jobs       = local.glue_jobs
}

locals {
  glue_databases = {
    glue_database = {
      description = "Database holding tables from GenPhenSQL and S3 source files"
    }
  }
}

locals {
  glue_connection = {
    glue_connection = {
      jdbc_connection_url    = "jdbc:postgresql://${data.aws_ssm_parameter.db_host.value}:${data.aws_ssm_parameter.db_port.value}/${data.aws_ssm_parameter.db_name.value}"
      secret_id              = data.aws_ssm_parameter.rds_credentials_secret_arn.value
      availability_zone      = "${local.aws_region}a"
      subnet_id              = var.low_cost_implementation ? data.aws_subnets.public-a[0].ids[0] : data.aws_subnets.private-a[0].ids[0]
      security_group_id_list = [data.aws_security_group.glue.id]
      tags                   = local.tags
    }
  }
}

locals {
  glue_classifiers = {
    genotype = {
      allow_single_column    = false
      disable_value_trimming = false
      quote_symbol           = "\""
      contains_header        = "ABSENT"
      delimiter              = ","
      header = [
        "Genotyper",
        "SampleId",
        "Chrom",
        "Pos",
        "Id",
        "Ref",
        "Alt",
        "Qual",
        "RefAd",
        "AltAd",
        "TotalDP",
        "Value"
      ]
    }
    deletion = {
      allow_single_column    = false
      disable_value_trimming = false
      quote_symbol           = "\""
      contains_header        = "ABSENT"
      delimiter              = ","
      header = [
        "SampleId",
        "Chromosome",
        "Position",
        "AlternativeNucleotide",
        "Deletion",
        "Genotyper",
        "Quality",
        "DR",
        "RR",
        "DV",
        "RV",
        "Value"
      ]
    }
    locus_stats = {
      allow_single_column    = false
      disable_value_trimming = false
      quote_symbol           = "\""
      contains_header        = "ABSENT"
      delimiter              = "\t"
      header = [
        "SampleId",
        "LocusTagName",
        "MeanDepth",
        "Cov10x",
        "Cov15x",
        "Cov20x",
        "Cov30x"
      ]
    }
    taxonomy_assignment = {
      allow_single_column    = false
      disable_value_trimming = false
      quote_symbol           = "\""
      contains_header        = "ABSENT"
      delimiter              = "\t"
      header = [
        "SampleId",
        "NCBITaxonId",
        "Value"
      ]
    }
    global_stats = {
      allow_single_column    = false
      disable_value_trimming = false
      quote_symbol           = "\""
      contains_header        = "PRESENT"
      delimiter              = "\t"
      header = [
        "SampleId",
        "MedianDepth ",
        "Coverage10x",
        "Coverage15x",
        "Coverage20x",
        "Coverage30x",
        "RawTotalSequences",
        "FilteredSequences",
        "Sequences",
        "IsSorted",
        "FirstFragments",
        "LastFragments",
        "ReadsMapped",
        "ReadsMappedAndPaired",
        "ReadsUnmapped",
        "ReadsProperlyPaired",
        "ReadsPaired",
        "ReadsDuplicated",
        "ReadsMQ0",
        "ReadsQCFailed",
        "NonPrimaryAlignments",
        "SupplementaryAlignments",
        "TotalLength",
        "TotalFirstFragmentLength",
        "TotalLastFragmentLength",
        "BasesMapped",
        "BasesMappedCigar",
        "BasesTrimmed",
        "BasesDuplicated",
        "Mismatches",
        "ErrorRate",
        "AverageLength",
        "AverageFirstFragmentLength",
        "AverageLastFragmentLength",
        "MaximumLength",
        "MaximumFirstFragmentLength",
        "MaximumLastFragmentLength",
        "AverageQuality",
        "InsertSizeAverage",
        "InsertSizeStandardDeviation",
        "InwardOrientedPairs",
        "OutwardOrientedPairs",
        "PairsWithOtherOrientation",
        "PairsOnDifferentChromosomes",
        "PercentageOfProperlyPairedReads"
      ]
    }
  }
}

locals {
  glue_crawlers = {
    deletion = {
      database_name = module.glue.glue_database_name["glue_database"]
      role          = aws_iam_role.glue_role.arn
      s3 = {
        s3_path = "s3://${module.s3_for_fsx.bucket_id["fsx-export"]}/deletion"
      }
      classifiers = [module.glue.glue_classifier_name["deletion"]]
      description = "Crawls molecular data files files from S3"
      tags        = local.tags
    }

    genotype = {
      database_name = module.glue.glue_database_name["glue_database"]
      role          = aws_iam_role.glue_role.arn
      s3 = {
        s3_path = "s3://${module.s3_for_fsx.bucket_id["fsx-export"]}/genotype"
      }
      classifiers = [module.glue.glue_classifier_name["genotype"]]
      description = "Crawls processed genotype result files from S3"
      tags        = local.tags
    }

    global_stats = {
      database_name = module.glue.glue_database_name["glue_database"]
      role          = aws_iam_role.glue_role.arn
      s3 = {
        s3_path = "s3://${module.s3_for_fsx.bucket_id["fsx-export"]}/global-stats"
      }
      classifiers = [module.glue.glue_classifier_name["global_stats"]]
      description = "Crawls global sequencing stats result files from S3"
      tags        = local.tags
    }

    locus_stats = {
      database_name = module.glue.glue_database_name["glue_database"]
      role          = aws_iam_role.glue_role.arn
      s3 = {
        s3_path = "s3://${module.s3_for_fsx.bucket_id["fsx-export"]}/locus-stats"
      }
      classifiers = [module.glue.glue_classifier_name["locus_stats"]]
      description = "Crawls per locus sequencing stats result files from S3"
      tags        = local.tags
    }

    taxonomy_assignment = {
      database_name = module.glue.glue_database_name["glue_database"]
      role          = aws_iam_role.glue_role.arn
      s3 = {
        s3_path = "s3://${module.s3_for_fsx.bucket_id["fsx-export"]}/taxonomy-assignment"
      }
      classifiers = [module.glue.glue_classifier_name["taxonomy_assignment"]]
      description = "Crawls taxonomy assignment result files from S3"
      tags        = local.tags
    }
    gen_phen = {
      database_name = module.glue.glue_database_name["glue_database"]
      role          = aws_iam_role.glue_role.arn
      jdbc_target = {
        connection_name = module.glue.glue_connection_name["glue_connection"]
        path            = data.aws_ssm_parameter.db_name.value
      }
      configuration = ""
      description   = "Crawls the genphensql"
      tags          = local.tags
    }
  }
}

locals {
  glue_jobs = {
    genotype = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for loading raw genotypes into genphensql staged table"
      glue_version      = "3.0"
      number_of_workers = "2"
      worker_type       = "G.1X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"   = "job-bookmark-enable",
        "--enable-spark-ui"       = "true",
        "--spark-event-logs-path" = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/genotype/",
        "--glue_db_name"          = module.glue.glue_database_name["glue_database"],
        "--postgres_db_name"      = data.aws_ssm_parameter.db_name.value,
        "--TempDir"               = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/"
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/genotype-known.py"
    }
    new_var_genotype = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for loading genotypes associated with new variants into genphensql"
      glue_version      = "4.0"
      number_of_workers = "2"
      worker_type       = "G.1X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"   = "job-bookmark-enable",
        "--enable-spark-ui"       = "true",
        "--spark-event-logs-path" = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/new_variant_genotype/",
        "--glue_db_name"          = module.glue.glue_database_name["glue_database"],
        "--postgres_db_name"      = data.aws_ssm_parameter.db_name.value,
        "--TempDir"               = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/"
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/genotype-new.py"
    }
    deletion = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for loading deletion identified by delly into genphensql staged table"
      glue_version      = "3.0"
      number_of_workers = "2"
      worker_type       = "G.1X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"        = "job-bookmark-enable",
        "--enable-spark-ui"            = "true",
        "--spark-event-logs-path"      = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/deletion/",
        "--glue_db_name"               = module.glue.glue_database_name["glue_database"],
        "--postgres_db_name"           = data.aws_ssm_parameter.db_name.value,
        "--rds_host"                   = data.aws_ssm_parameter.db_host.value,
        "--rds_port"                   = data.aws_ssm_parameter.db_port.value,
        "--rds_secret_credentials_arn" = data.aws_ssm_parameter.rds_credentials_secret_arn.value,
        "--TempDir"                    = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/deletion.py"
    }
    locus_stats = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for loading sequencing depth and coverage per locus stats into genphensql"
      glue_version      = "3.0"
      number_of_workers = "2"
      worker_type       = "G.1X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"        = "job-bookmark-enable",
        "--enable-spark-ui"            = "true",
        "--spark-event-logs-path"      = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/locusstats/",
        "--glue_db_name"               = module.glue.glue_database_name["glue_database"],
        "--postgres_db_name"           = data.aws_ssm_parameter.db_name.value,
        "--rds_host"                   = data.aws_ssm_parameter.db_host.value,
        "--rds_port"                   = data.aws_ssm_parameter.db_port.value,
        "--rds_secret_credentials_arn" = data.aws_ssm_parameter.rds_credentials_secret_arn.value,
        "--TempDir"                    = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/locus-stats.py"
    }
    taxonomy_assignment = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for loading taxonomy assignments by kraken2 into genphensql"
      glue_version      = "3.0"
      number_of_workers = "2"
      worker_type       = "G.1X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"        = "job-bookmark-enable",
        "--enable-spark-ui"            = "true",
        "--spark-event-logs-path"      = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/taxonomyassignment/",
        "--glue_db_name"               = module.glue.glue_database_name["glue_database"],
        "--postgres_db_name"           = data.aws_ssm_parameter.db_name.value,
        "--rds_host"                   = data.aws_ssm_parameter.db_host.value,
        "--rds_port"                   = data.aws_ssm_parameter.db_port.value,
        "--rds_secret_credentials_arn" = data.aws_ssm_parameter.rds_credentials_secret_arn.value,
        "--TempDir"                    = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/taxonomy-assignment.py"
    }
    global_stats = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for loading sequencing stats by samtools per sample into genphensql"
      glue_version      = "3.0"
      number_of_workers = "2"
      worker_type       = "Standard"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"        = "job-bookmark-enable",
        "--enable-spark-ui"            = "true",
        "--spark-event-logs-path"      = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/globalstats/",
        "--glue_db_name"               = module.glue.glue_database_name["glue_database"],
        "--postgres_db_name"           = data.aws_ssm_parameter.db_name.value,
        "--rds_host"                   = data.aws_ssm_parameter.db_host.value,
        "--rds_port"                   = data.aws_ssm_parameter.db_port.value,
        "--rds_secret_credentials_arn" = data.aws_ssm_parameter.rds_credentials_secret_arn.value,
        "--TempDir"                    = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/global-stats.py"
    }
    del_variants = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job to insert new delly deletion variants"
      glue_version      = "3.0"
      number_of_workers = "5"
      worker_type       = "G.2X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option" = "job-bookmark-disable",
        "--conf"                = "spark.driver.maxResultSize=6g",
        "--glue_db_name"        = module.glue.glue_database_name["glue_database"],
        "--postgres_db_name"    = data.aws_ssm_parameter.db_name.value,
        "--TempDir"             = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/del-variants.py"
    }
    join_genotype = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for loading final genotypes, with joining into genphensql"
      glue_version      = "3.0"
      number_of_workers = "5"
      worker_type       = "G.1X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"        = "job-bookmark-disable",
        "--database_name"              = module.glue.glue_database_name["glue_database"],
        "--enable-spark-ui"            = "true",
        "--write-shuffle-spills-to-s3" = "true",
        "--write-shuffle-files-to-s3"  = "true",
        "--conf"                       = "spark.shuffle.glue.s3ShuffleBucket=S3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/spill",
        "--spark-event-logs-path"      = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/joingenotype",
        "--postgres_db_name"           = data.aws_ssm_parameter.db_name.value,
        "--TempDir"                    = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/join-genotype.py"
    }
    predict_resistance = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for calculating genotypic resistance statistics"
      glue_version      = "3.0"
      number_of_workers = "5"
      worker_type       = "G.2X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"      = "job-bookmark-enable",
        "--glue_database_name"       = module.glue.glue_database_name["glue_database"],
        "--rds_database_name"        = data.aws_ssm_parameter.db_name.value,
        "--rds_glue_connection_name" = module.glue.glue_connection_name["glue_connection"],
        "--enable-spark-ui"          = "true",
        "--spark-event-logs-path"    = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/predictresistance/",
        "--TempDir"                  = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/predict_resistance.py"
    }
    predict_resistance_v2 = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for predicting resistance according to mutation catalogue v2"
      glue_version      = "4.0"
      number_of_workers = "3"
      worker_type       = "G.2X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"      = "job-bookmark-enable",
        "--glue_database_name"       = module.glue.glue_database_name["glue_database"],
        "--postgres_database_name"   = data.aws_ssm_parameter.db_name.value,
        "--rds_glue_connection_name" = module.glue.glue_connection_name["glue_connection"],
        "--enable-spark-ui"          = "true",
        "--spark-event-logs-path"    = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/predictresistancev2/",
        "--extra-py-files"           = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/ETL_tools.zip",
        "--TempDir"                  = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }
      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/predict_resistance_v2.py"
    }
    write_formatted_annotations_per_gene = {
      role_arn          = aws_iam_role.glue_role.arn
      connections       = [module.glue.glue_connection_name["glue_connection"]]
      description       = "Glue job for writing data into formatted_annotations_per_gene table"
      glue_version      = "3.0"
      number_of_workers = "5"
      worker_type       = "G.2X"

      tags = merge(local.tags, {
        Name = local.prefix
      })

      default_arguments = {
        "--job-bookmark-option"      = "job-bookmark-enable",
        "--enable-spark-ui"          = "true",
        "--extra-py-files"           = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/ETL_tools.zip",
        "--spark-event-logs-path"    = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/glue/writefapg/",
        "--glue_db_name"             = module.glue.glue_database_name["glue_database"],
        "--postgres_db_name"         = data.aws_ssm_parameter.db_name.value,
        "--rds_glue_connection_name" = module.glue.glue_connection_name["glue_connection"],
        "--TempDir"                  = "s3://${module.s3_for_fsx.bucket_id["glue-logs-bucket"]}/",
      }

      script_location = "s3://${module.s3_for_fsx.bucket_id["glue-scripts"]}/glue-jobs/write_formatted_annotations_per_gene.py"
    }
  }
}
