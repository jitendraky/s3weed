# S3weed
S3-like proxy for Weed-FS

# Status

*Alpha*
It needs some more work, test cases, and so on, and maybe the interface will change too!

[![Build Status](https://secure.travis-ci.org/tgulacsi/s3weed.png)](http://travis-ci.org/tgulacsi/s3weed)

# Goals

Provide a proxy for Weed-FS (and possibly for other stores) which makes
it usable with a general S3 client.

# Usage

  * The interface definition is github.com/tgulacsi/s3weed/s3intf
  * The server (which uses a given implementation of the interface): github.com/tgulacsi/s3weed/s3srv
  * The beginning of an filesystem-hierarchy-backed implementation: github.com/tgulacsi/s3weed/s3impl/dirS3

# Contributing

Pull requests are welcomed!
Tests are needed! (See and extend s3impl/impl_test.go)

# Credits
  * [Chris Lu]

# License

BSD 2 clause - see LICENSE for more details
