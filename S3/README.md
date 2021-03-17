# AWS CLI
## S3 to S3
* aws s3 cp s3://tam-results/test.txt s3://research-tmp-test/test_cp.txt
* to copy all object use aws s3 sync s3://DOC-EXAMPLE-BUCKET-SOURCE s3://DOC-EXAMPLE-BUCKET-TARGET
## Local to S3
* aws s3 cp /tmp/foo/ s3://bucket/ --recursive --exclude "*" --include "*.jpg" --include "*.txt" 
## S3 to Local
* aws s3 cp s3://mybucket/test.txt test2.txt
