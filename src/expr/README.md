# Expression Evaluation Sample

## Build Instructions

Add `ast` to `PRODUCT_PACKAGES` in the lunch target's mk file.

* For sdv_core_cf target, add the package to `device/google/sdv/sdv_core_common/sdv_core_common_packages.mk`

Run the following commands

```
$ UNSAFE_DISABLE_HIDDENAPI_FLAGS=true m -j

$ launch_cvd # start emulator

$ adb root

$ adb shell /system/bin/ast
```

Check logs with `adb logcat`

## Test Instructions

`UNSAFE_DISABLE_HIDDENAPI_FLAGS=true atest sdv_ast_unit_tests`
