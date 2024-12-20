import base64, os

def lambda_handler(event, context):
    mime_string = b"""MIME-Version: 1.0
        Content-Type: multipart/mixed; boundary="==BOUNDARY=="

        --==BOUNDARY==
        Content-Type: text/cloud-config; charset="us-ascii"

        runcmd:
        - amazon-linux-extras install -y lustre2.10
        - mkdir -p /scratch/working
        - mount -t lustre -o defaults,noatime,flock,_netdev """ + event["FSx"]["Name"].encode() + b""".fsx.""" + os.environ.get("AWS_REGION").encode() + b""".amazonaws.com@tcp:/""" + event["FSx"]["MountName"].encode() + b""" /scratch/working
        --==BOUNDARY==
    """
    formated_mime = base64.b64encode(b"\n".join([x.strip(b"\t").strip(b" ") for x in mime_string.split(b"\n")]))
    return(formated_mime.decode("utf-8"))