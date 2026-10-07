package app.risuko.webview_upgrade

import android.app.ActivityManager
import android.app.Application
import android.content.Context
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.Process

/** Single logcat tag for the whole plugin: `adb logcat -s RWebViewUpgrade` */
internal const val LOG_TAG = "RWebViewUpgrade"

private val mainHandler by lazy { Handler(Looper.getMainLooper()) }

internal fun runOnMainThread(block: () -> Unit) {
    if (Looper.myLooper() == Looper.getMainLooper()) block() else mainHandler.post(block)
}

internal fun isMainProcess(context: Context): Boolean {
    val packageName = context.applicationContext.packageName
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
        return packageName == Application.getProcessName()
    }
    val pid = Process.myPid()
    val am = context.getSystemService(Context.ACTIVITY_SERVICE) as? ActivityManager ?: return true
    val processes = am.runningAppProcesses ?: return true
    for (info in processes) {
        if (info.pid == pid) return packageName == info.processName
    }
    return true
}

/** Possible webview provider */
internal val CANDIDATE_PACKAGES = listOf(
    "com.google.android.webview",
    "com.android.webview",
    "com.google.android.webview.beta",
    "com.google.android.webview.dev",
    "com.google.android.webview.canary",
    "com.android.chrome",
    "com.huawei.webview",
)
