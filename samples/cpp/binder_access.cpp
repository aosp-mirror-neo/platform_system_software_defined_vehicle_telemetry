// Copyright 2024 Google LLC

#include <android/binder_auto_utils.h>
#include <android/binder_ibinder_jni.h>
#include <android/binder_manager.h>
#include <log/log.h>

#include <jni.h>

extern "C" JNIEXPORT jobject JNICALL
Java_google_sdv_telemetry_java_Binder_BinderServiceAccessor_getService__Ljava_lang_String_2(
        JNIEnv* env, jclass /**/, jstring str) {
    ALOGI("%s", __func__);

    // Best practice is probably libnativehelper ScopedUtfChars or
    // libbase ScopeGuard (for platform code), but this is with minimal
    // dependencies.
    const char* name = env->GetStringUTFChars(str, nullptr);

    ALOGI("example %s", name);

    jobject jbinder = nullptr;

    // Java does not have vendor variants. It's only safe to pass a service when
    // 'vendor: true' if it is @VintfStability.
    if (AServiceManager_isDeclared(name)) {
        ndk::SpAIBinder binder = ndk::SpAIBinder(AServiceManager_waitForService(name));
        jbinder = AIBinder_toJavaBinder(env, binder.get());
    } else {
        ALOGI("not declared");
    }

    env->ReleaseStringUTFChars(str, name);

    return jbinder;
}
