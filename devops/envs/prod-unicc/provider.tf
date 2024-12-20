provider "aws" {
  region = local.aws_region
}

provider "aws" {
  alias  = "useast1"
  region = "us-east-1"
}
