g++ -O3 -std=c++17 -o bench_all bench_all.cpp \
    -I/usr/local/include \
    -L/usr/local/lib \
    -ltokyocabinet -lkyotocabinet -ltkrzw -lm

# コンパイル
g++ -O3 -std=c++17 -o bench_all2 bench_all2.cpp \
    -I/usr/local/include \
    -I/usr/local/opt/berkeley-db/include \
    -L/usr/local/lib \
    -L/usr/local/opt/berkeley-db/lib \
    -ltokyocabinet -lkyotocabinet -ltkrzw -ldb -lm
