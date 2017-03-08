#!/bin/sh

#detect os/arch
case `uname -s` in
   MINGW32*)
      os=Windows
      arch=x86;;
   MINGW64*)
      os=Windows
      arch=x86_64;;
   Linux*)
      os=Linux
      arch=`uname -m`
      [[ $arch =~ (ia|amd)64 ]] && arch=x86_64
      [[ $arch =~ ia?[0-9]+ ]] && arch=x86
      ;;
esac
if [ "$os" == "Windows" ]
then
   ccflags="-D_JNI_IMPLEMENTATION_ -Wl,--kill-at"
   ext="dll"
else
   ccflags="-fPIC -z noexecstack"
   ext="so"
fi

libPath=lib/$os/$arch
#[ ! -d libPath ] && echo 'lib path not found: '$libPath && exit

gcc -Iinclude -Iinclude/system/$os -L$libPath compscan.c $libPath/libcrypto.a -shared $ccflags -o $libPath/compscan.$ext
