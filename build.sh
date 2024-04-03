CC=/opt/homebrew/opt/llvm/bin/clang
rm -rf bin
mkdir bin
$CC -o bin/test_test ztest/test_test.c -I./;./bin/test_test
$CC -o bin/utils_test zutils/utils_test.c -I./;./bin/utils_test
$CC -o bin/log_test zlog/log_test.c -I./;./bin/log_test