import boto3, argparse, os, re


def copy_reads_from_s3(args):
    s3 = boto3.client('s3')
    filename = args.sample_id + "/" + args.fastq_id
    if re.match(r'[ESD]RR[0-9]+', args.fastq_id):
        fastq_path = "sra/"+args.fastq_id+"/"+args.fastq_id
    else:
        fastq_path = args.fastq_path.strip("s3://")
        filename += "_R" + str(int(args.index) + 1) + ".fastq.gz"
    try:
        os.mkdir(args.sample_id)
    except OSError:
        pass
    s3.download_file(
        Bucket=args.bucket,
        Key=fastq_path,
        Filename=filename,
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fastq_path", help="Bucket location of CRyPTIC data file", default=None)
    parser.add_argument("--fastq_id", help="Bucket location of CRyPTIC data file")
    parser.add_argument("--sample_id", help="Bucket location of CRyPTIC data file")
    parser.add_argument("--index", help="Index", default=None)
    parser.add_argument("--bucket", help="Bucket name", default="sra-pub-run-odp")
    args = parser.parse_args()

    copy_reads_from_s3(args)
