#!/bin/sh

#forse 32bit build under 64bit linux

os=Linux
arch=x86
ccflags="-fPIC -z noexecstack"
ext="so"

libPath=lib/$os/$arch
#[ ! -d libPath ] && echo 'lib path not found: '$libPath && exit

gcc -m32 -Iinclude -Iinclude/system/$os -L$libPath compscan.c $libPath/libcrypto.a -shared $ccflags -o $libPath/compscan.$ext
