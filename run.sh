#!/bin/bash

#java -jar target/avro2parquet-1.0.jar -avro2parquet -avro ~/Downloads/input.avro
#java -jar target/avro2parquet-1.0.jar -read_avro -num 10000 -avro ~/Downloads/input.avro
#java -jar target/avro2parquet-1.0.jar -read_avro -avro ~/Downloads/input.avro
java -jar target/avro2parquet-1.0.jar -read_parquet -parquet ~/Downloads/input.avro.parquet
