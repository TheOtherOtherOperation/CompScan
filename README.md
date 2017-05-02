# CompScan

CompScan is a tool for analyzing the compressibility of a datastore.

A typical looks like this:
    
    java -cp <classpath> net.deepstorage.compscan.CompScan <arguments>

where <classpath> consists of paths to lz4.jar and CompScan.jar and separated by system-specific separator

Alternately, if running from a JAR:
   
   java -cp <path to lz4.jar> -jar CompScan.jar <arguments>

The <arguments> part is described below.
The lz4.jar can be found in the 'lib' directory.

#Examples.

Windows:
   
D:\CompScan>java -cp lib/lz4-1.3.jar;CompScan.jar net.deepstorage.compscan.CompScan --mode BIG 1200m C:\inputDir C:\outputDir 4096 16384 LZ4
D:\CompScan>java -cp lib/lz4-1.3.jar -jar CompScan.jar --mode BIG 1200m C:\inputDir C:\outputDir 4096 16384 LZ4

Linux:
~/CompScan>java -cp lib/lz4-1.3.jar:CompScan.jar net.deepstorage.compscan.CompScan --mode BIG 1200m /etc/inputDir /etc/outputDir 4096 16384 LZ4
~/CompScan>java -cp lib/lz4-1.3.jar.jar -jar CompScan.jar --mode BIG 1200m /etc/inputDir /etc/outputDir 4096 16384 LZ4
   
Here:
    datastore location: C:\inputDir
    output location: C:\outputDir
    block size: 4096
    superblock size: 16384
    compression scheme: LZ4
    mode: separate files bigger than 1200 megabytes

## Arguments
```
Usage: CompScan [-h] [--help] [--pause] [--threads <number>] [--mapType (java|direct|fs)] [--mapDir <path>] [--mapOptions] [--mode (NORMAL|BIG <size>|VMDK)] [--overwrite] [--rate MB_PER_SEC] [--buffer-size BUFFER_SIZE] pathIn pathOut blockSize superblockSize format[:<options>]
Positional Arguments
    pathIn            path to the dataset
    pathOut           where to save the output
    blockSize         bytes per block
    superblockSize    bytes per superblock (must be an even multiple of block size)
    formatString      compression format to use, 
                      currently includes None,LZ4,GZIP,LZW
    format options    for LZ4: compression level 0 - 17, default 9
                      for GZIP: compression level 1 - 9, default 6
Optional Arguments
   -h, --help        print this help message
   --verbose         enable verbose console feedback (should only be used for debugging)
   --usage           enable printing of estimated memory usage (requires wide console)
   --mode            scan mode, one of [NORMAL, BIG, VMDK]:
                     * NORMAL - default, all file tree is processed as a single stream
                     * BIG <size spec> - process files independently, only those
                       bigger then spec, where <size spec> = <number>[k|m|g]
                       meaning size in bytes, e.g: 1000, 100k, 100m, 100g
                     * VMDK - process files independently, only those
                       with extensions [txt, vhd, vhdx, vmdk]
   --overwrite       whether overwriting the output file is allowed
   --rate MB_PER_SEC maximum MB/sec we're allowed to read
   --buffer-size BUFFER_SIZE size of the internal read buffer
   --hashes          print the hash table before exiting; the hashes are never saved to disk
   --threads         number of processing threads.
                     Optimal value: number of CPU cores/hyperthreads.
                     Default: autodetected
   --pause           make pause (e.g. to attach external profiler)
   --mapType         select the implementation of the hash-counting map:
                           * java - plain java.util.HashMap
                           * direct - custom off-heap map over native RAM
                           * fs - custom off-heap map over memory-mapped files
                           Default: fs:<user home>/.compscan/map
   --mapDir          home directory for fs-type map
   --mapOptions      print advanced map options and exit

Advanced map options:
   --mapMemoryMax    upper bound for map data size. Holds only for direct and fs maps. 
                     Upon reaching this limit the maps will stop working.
                     Estimation: <expected scan size>*100/<block size>
                     Default: 128TiB
   --mapMemoryChunk  map memory allocation chunks. Holds only for direct and fs maps.
                     Too small chunks may cause speed degradation. Too large chunks may cause allocation problems.
                     Optimal value:
                     <expected map memory size (see above)>/1000
                     Default: 128MiB
   --mapListSize     map's internal parameter, controls the tradeoff between speed and memory consumtion
                     (both are inversely proportional to it)
                     Optimal range: 8..20
                     Default: 9
```



## Memory Considerations

The program stores approximately 100 bytes of data per unique hash, in addition to some comparatively  small amount (< 20MB) of internal state. If run on a large data store, it is very possible for the hash map to overrun the default heap memory (around 250MB in 32-bit Java). Besides that, too many objects in java heap space make garbage collection much harder, causing significant performance degradation. 
To avoid such problems the new type of map implementation was introduced, which keeps data out of java heap, in native memory, which in turn may be either plain RAM of memory-mapped files.
To switch between the map implementations use the --mapType option with following values:
 --mapType java     - standard java HashMap. Retained only for comparison.
 --mapType direct   - off-heap map over plain RAM. 
 --mapType fs       - toff-heap map over memory-mapped files. The default type.

The fs-type map is operated over multiple temp files created in its home directory. The default directory is <user home>/.compscan/map and can be specified with --mapDir option. The files are normally deleted on application exit, but may be left undeleted upon abnormal termination, so it is recommended to periodically clean this directory.

Both off-heap map types have upper size limit 128TiB, which still may be increased using --mapMemoryMax option.

To see all map-related options use the --mapOption switch.

When using the java map type it is recommended to specify the appropriate heap size using JVM option -Xmx<size>

All size-related options must be of the form NU, where N is an integer and U is a unit specifier: k (kilobytes), m (megabytes), g (gigabytes), p(petabytes).



Example:
```
java -Xmx8g -jar CompScan.jar --mapType java ... (This will start the JVM in 64-bit mode with both the maximum heap size set to 8GB.)
java -jar CompScan.jar --mapType direct ...     (use the off-heap map over plain RAM)
java -jar CompScan.jar --mapType fs --mapDir /tmp/compscan ... (use the off-heap map over mapped files in the /tmp/compscan)
```


## Adding new compression formats

The program allows for the easy addition of new compression formats. When the "format" CLI argument is read, the Java Reflection API is used to search for a matching class name in the net.deepstorage.compscan.compress package --- that is, if "LZW" is provided as the format argument, Java Reflection is used to search for the corresponding class net.deepstorage.compscan.compress.LZW. If the corresponding class exists and implements the interface CompressionInterface (net/deepstorage/compscan/CompressionInterface.java), then that class is used to perform the compression phase.

Any experienced Java programmer should know how to place the new class in the appropriate package and implement the necessary interface.

Example:
Suppose we want to create a Zip compression format.
1. Create the Zip.java file at "net/deepstorage/compscan/compress/Zip.java"
2. The first non-comment, non-whitepsace line must be "package net.deepstorage.compscan.compress;"
3. Import CompressionInterface: "import net.deepstorage.compscan.CompressionInterface;"
4. Create the class definition: "public class Zip implements CompressionInterface { ... }"
5. CompressionInterface requires you to override and implement the compress() method: "public byte[] compress(byte[] data, int blockSize) { ... }". This method expects to receive a data buffer (data) of exactly one superblock in size and should return a (smaller) buffer containing the compressed data.

Completing these steps successfully will cause the new compression class to be detected the next time the project is compiled. You may then access it by specifying "Zip" (case-sensitive) as the format argument on the command line.
