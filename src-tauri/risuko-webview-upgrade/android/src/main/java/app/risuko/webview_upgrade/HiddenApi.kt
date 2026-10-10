package app.risuko.webview_upgrade

import android.os.Build
import android.util.Log
import java.lang.reflect.Method

internal object HiddenApi {

    @Volatile
    private var done = false

    @Synchronized
    fun exempt() {
        if (done) return
        done = true
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.P) return
        try {
            val getDeclaredMethod: Method = Class::class.java.getDeclaredMethod(
                "getDeclaredMethod",
                String::class.java,
                arrayOf<Class<*>>()::class.java,
            )
            val vmRuntimeClass = Class.forName("dalvik.system.VMRuntime")

            val getRuntime = getDeclaredMethod.invoke(
                vmRuntimeClass, "getRuntime", arrayOfNulls<Class<*>>(0),
            ) as Method
            val setExemptions = getDeclaredMethod.invoke(
                vmRuntimeClass,
                "setHiddenApiExemptions",
                arrayOf<Class<*>>(arrayOf<String>()::class.java),
            ) as Method

            val vmRuntime = getRuntime.invoke(null)
            setExemptions.invoke(vmRuntime, arrayOf("L"))
            Log.i(LOG_TAG, "Hidden API restrictions exempted")
        } catch (t: Throwable) {
            Log.w(LOG_TAG, "Failed to exempt hidden API restrictions: ${t.message}")
        }
    }
}
