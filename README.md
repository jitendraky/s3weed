# S3weed
S3-like proxy for Weed-FS

# Status

*Alpha*
It needs some more work, test cases, and so on, and maybe the interface will change too!

[![Build Status](https://travis-ci.org/tgulacsi/s3weed.png)](https://travis-ci.org/tgulacsi/s3weed)

# Goals

Provide a proxy for Weed-FS (and possibly for other stores) which makes
it usable with a general S3 client.

# Usage

  * The interface definition is github.com/tgulacsi/s3weed/s3intf
  * The server (which uses a given implementation of the interface): github.com/tgulacsi/s3weed/s3srv
  * The beginning of an filesystem-hierarchy-backed implementation: github.com/tgulacsi/s3weed/s3impl/dirS3
  * The beginning of a Weed-FS backed implementation: github.com/tgulacsi/s3weed/s3impl/weedS3
  This does not have any authentication (uses empty password) ATM.

    go build github.com/tgulacsi/s3weed/s3impl
    mkdir /tmp/weedS3
    s3impl -db=/tmp/weedS3 -weed=http://localhost:9333 -http=s3.localhost:80

  Some testing with s3cmd is in s3cmd-test.sh

## Caveeats
I've tested with s3cmd, but that seems to implement only the DNS-named buckets
(you set the bucket name in the server name: testbucket.s3.localhost).
Thus you need to resolve the "bucketname".fqdn to fqdn - add
    127.0.0.1   localhost   s3.localhost    testbucket.s3.localhost
to your /etc/hosts file, for example.

# Contributing

Pull requests are welcomed!
Tests are needed! (See and extend s3impl/impl_test.go)

# Credits
  * [Chris Lu]

# License

BSD 2 clause - see LICENSE for more details
