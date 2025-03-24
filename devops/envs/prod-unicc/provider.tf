provider "aws" {
  region = local.aws_region
  default_tags {
    tags = local.tags
  }

}

provider "aws" {
  alias  = "useast1"
  region = "us-east-1"
  default_tags {
    tags = local.tags
  }

}
