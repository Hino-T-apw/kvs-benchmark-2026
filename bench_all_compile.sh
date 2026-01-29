g++ -O3 -std=c++17 -o bench_all bench_all.cpp \
    -I/usr/local/include \
    -L/usr/local/lib \
    -ltokyocabinet -lkyotocabinet -ltkrzw -lm