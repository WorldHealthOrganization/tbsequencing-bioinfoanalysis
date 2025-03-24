terraform {
  backend "s3" {
    encrypt        = true
    bucket         = ""
    key            = ""
    region         = ""
    dynamodb_table = ""
  }
}
