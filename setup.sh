#!/bin/bash
source beam/bin/activate

pip install --quiet apache-beam

mkdir -p data
gsutil cp gs://dataflow-samples/shakespeare/kinglear.txt data/