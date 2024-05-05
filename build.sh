CC=/opt/homebrew/opt/llvm/bin/clang
rm -rf bin
mkdir bin
$CC -o ./bin/test ztest/test.c -I./ --std=c2x -g -pthread;
$CC -o ./bin/svr_cli_test znet/svr_cli_test.c -I./ --std=c2x -pthread -g;