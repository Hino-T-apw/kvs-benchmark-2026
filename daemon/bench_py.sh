# Hinotetsu (例)
#./hinotetsud -p 11211 -m 256

# Redis/Memcached
docker compose up -d

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# mixed (set/get半々)
python bench_kvs.py --mode mixed --concurrency 64 --ops 200000 --value-size 256