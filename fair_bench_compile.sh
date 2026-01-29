g++ -O3 -std=c++17 -o fair_bench fair_bench.cpp \
    -I/usr/local/include \
    -I/usr/local/opt/berkeley-db/include \
    -L/usr/local/lib \
    -L/usr/local/opt/berkeley-db/lib \
    -ltokyocabinet -lkyotocabinet -ltkrzw -ldb -lm

./fair_bench 100000
./fair_bench 1000000