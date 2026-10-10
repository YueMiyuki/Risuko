package app.risuko.webview_upgrade

import app.risuko.webview_upgrade.reflect.Reflect


internal object SystemProps {
    fun get(key: String): String = try {
        Reflect.callStatic(Reflect.cls("android.os.SystemProperties"), "get", key) as? String ?: ""
    } catch (_: Throwable) {
        ""
    }

    fun getInt(key: String): Int? = get(key).trim().toIntOrNull()
}
