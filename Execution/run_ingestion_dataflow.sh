cd ~/data-science-on-gcp/04_streaming/simulate

virtualenv data-sci-env -p python3.5

source data-sci-env/bin/activate

pip install timezonefinder pytz
pip install apache-beam[gcp]

cd ~

python biopsy_ingest.py -p smu-msds-7346-summer2019-mld -b raw-biopsy-files -d raw_biopsy_data
python genome_ingest.py -p smu-msds-7346-summer2019-mld -b raw-biopsy-files -d raw_biopsy_data
