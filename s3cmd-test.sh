#!/bin/sh
CFG="$(dirname $0)/s3cmd-weedS3.cfg"
if [ ! -e $CFG ]; then
    cat >$CFG <<EOF
[default]
access_key = AAA
bucket_location = US
#cloudfront_host = cloudfront.amazonaws.com
default_mime_type = binary/octet-stream
delete_removed = False
dry_run = False
enable_multipart = True
encoding = UTF-8
encrypt = False
follow_symlinks = False
force = False
get_continue = False
gpg_command = /usr/bin/gpg
gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_passphrase =
guess_mime_type = True
host_base = s3.localhost
host_bucket = %(bucket)s.s3.localhost
human_readable_sizes = False
invalidate_on_cf = False
list_md5 = False
log_target_prefix =
mime_type =
multipart_chunk_size_mb = 15
preserve_attrs = True
progress_meter = True
proxy_host =
proxy_port = 0
recursive = False
recv_chunk = 4096
reduced_redundancy = False
secret_key =
send_chunk = 4096
#simpledb_host = sdb.amazonaws.com
skip_existing = False
socket_timeout = 300
urlencoding_mode = normal
use_https = False
verbosity = DEBUG
#website_endpoint = http://%(bucket)s.s3-website-%(location)s.amazonaws.com/
website_error =
website_index = index.html
EOF
fi
S3CMD="s3cmd --config=$CFG"
set -e
if $S3CMD ls | grep -q proba; then
    $S3CMD ls
else
    $S3CMD mb s3://proba
fi
$S3CMD put $(dirname $0)/LICENSE s3://proba

#  sudo ./s3impl/s3impl -weed=http://localhost:9333 -db=/tmp/weedS3 -http=s3.localhost:80

