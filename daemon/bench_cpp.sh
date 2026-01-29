g++ -O3 -std=c++20 bench_kvs.cpp -pthread -o bench_kvs

./bench_kvs --mode mixed --threads 64 --ops 200000 --value-size 256
