CC=/opt/homebrew/opt/llvm/bin/clang
rm -rf bin
mkdir bin
$CC -o ./bin/test ztest/test.c -I./ --std=c2x -g -pthread;
$CC -o ./bin/benchmark zbenchmark/benchmark.c -I./ --std=c2x -O3 -pthread;