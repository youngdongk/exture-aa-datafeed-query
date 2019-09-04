#!/bin/bash
set -eu
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin

bucket=mybucket
project=myproject
dataset=adobe_datafeed
table=hit_data

for file in $(ls *.tar.gz); do
    echo "processing ${file}..."
    tar -zxf ${file}
    
    echo "load hit_data.tsv..."
    bq load --field_delimiter='\t' --max_bad_records=100 ${project}:${dataset}.${table} hit_data.tsv
     
    echo "backup ${file}..."
    gsutil cp ${file} gs://${bucket}/

    #clear
    rm ${file}
done

echo "load supporting files..."
bq load --replace --field_delimiter='\t' ${project}:${dataset}.browser browser.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.browser_type browser_type.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.color_depth color_depth.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.connection_type connection_type.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.country country.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.event event.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.javascript_version javascript_version.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.languages languages.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.operating_systems operating_systems.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.referrer_type referrer_type.tsv id:integer,description:string,description2:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.resolution resolution.tsv id:integer,description:string
bq load --replace --field_delimiter='\t' ${project}:${dataset}.search_engines search_engines.tsv id:integer,description:string

#clear
rm *.tsv
