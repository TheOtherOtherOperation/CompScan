#include <jni.h>
#include <windows.h>
#include <openssl/sha.h>

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved)  {
   return JNI_VERSION_1_6;
}

JNIEXPORT void JNICALL Java_net_deepstorage_compscan_SHA1Encoder_encode(
   JNIEnv *env, jclass SHA1Encoder, jbyteArray in, jbyteArray out
){
   jbyte* inPtr=(*env)->GetByteArrayElements(env, in, NULL);
   jbyte* outPtr=(*env)->GetByteArrayElements(env, out, NULL);
   int size=(*env)->GetArrayLength(env, in);
   SHA1(inPtr, size, outPtr);
   (*env)->ReleaseByteArrayElements(env, in, inPtr, JNI_ABORT);
   (*env)->ReleaseByteArrayElements(env, out, outPtr, 0);
}


