package app.risuko.webview_upgrade

import app.risuko.webview_upgrade.reflect.Reflect

/**
 *   Reader for `android.os.SystemProperties` (hidden API)
 *  
 *   For example:
 *   adb shell setprop debug.risuko.wvup.force com.android.chrome   # force a swap
 *   adb shell setprop debug.risuko.wvup.nudge 1                    # force the dialog
 *   adb shell setprop debug.risuko.wvup.minupgrade 999            # raise threshold
 *   adb shell setprop debug.risuko.wvup.minsupported 999
 *
 */

internal object SystemProps {
    fun get(key: String): String = try {
        Reflect.callStatic(Reflect.cls("android.os.SystemProperties"), "get", key) as? String ?: ""
    } catch (_: Throwable) {
        ""
    }

    fun getInt(key: String): Int? = get(key).trim().toIntOrNull()
}
