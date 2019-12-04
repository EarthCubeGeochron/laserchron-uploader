# laserchron-uploader
Upload data to the LaserChron archive

## Useful s3 tools

`s3cmd` is a command line interface to S3. It can be used for basic management
of the LaserChron data bucket.

To configure `s3cmd` to access the LaserChron archive, run `s3cmd --configure`.
This will show a set of prompts to input credentials and eventually write out
an `.s3cfg` file in your home directory. The `host_base` config
value should be `sfo2.digitaloceanspaces.com`,
and the "host bucket template" should be `%(bucket)s.sfo2.digitaloceanspaces.com`
instead of the default AWS values.

This tool can now be used to manage the `laserchron-data` bucket. Some common patterns
are listed [here](https://www.digitalocean.com/docs/spaces/resources/s3cmd-usage/).
For instance, to list all files in the LaserChron archive, run `s3cmd ls s3://laserchron-data`.
