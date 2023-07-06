# Expression Evaluation Sample

## Build Instructions

Run `UNSAFE_DISABLE_HIDDENAPI_FLAGS=true m -j`

Push executable to device

Run `adb shell /system/bin/ast`

Check logs with `adb logcat`

## Test Instructions

`UNSAFE_DISABLE_HIDDENAPI_FLAGS=true atest sdv_ast_unit_tests`
