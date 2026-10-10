package app.risuko.webview_upgrade.swap

import android.content.Context
import android.content.ContextWrapper
import android.os.IBinder
import android.os.IInterface
import android.util.Log
import app.risuko.webview_upgrade.LOG_TAG
import app.risuko.webview_upgrade.reflect.Reflect


internal object Framework {

    const val SERVICE_WEBVIEW_UPDATE = "webviewupdate"
    const val SERVICE_PACKAGE = "package"

    private val serviceManager get() = Reflect.cls("android.os.ServiceManager")
    private val webViewFactory get() = Reflect.cls("android.webkit.WebViewFactory")
    private val activityThread get() = Reflect.cls("android.app.ActivityThread")

    fun getService(name: String): IBinder? =
        Reflect.callStatic(serviceManager, "getService", name) as? IBinder

    @Suppress("UNCHECKED_CAST")
    fun serviceCache(): MutableMap<String, IBinder> =
        Reflect.getStaticField(serviceManager, "sCache") as MutableMap<String, IBinder>

    fun providerInstance(): Any? = Reflect.getStaticField(webViewFactory, "sProviderInstance")

    fun webViewUpdateAsInterface(binder: IBinder): IInterface =
        Reflect.callStatic(
            Reflect.cls("android.webkit.IWebViewUpdateService\$Stub"),
            "asInterface",
            binder,
        ) as IInterface

    fun setResponsePackageInfo(response: Any, packageInfo: Any?) =
        Reflect.setField(response, "packageInfo", packageInfo)

    fun packageManagerAsInterface(binder: IBinder): IInterface =
        Reflect.callStatic(
            Reflect.cls("android.content.pm.IPackageManager\$Stub"),
            "asInterface",
            binder,
        ) as IInterface

    fun currentActivityThread(): Any? =
        Reflect.callStatic(activityThread, "currentActivityThread")

    fun setActivityThreadPackageManager(iface: IInterface?) {
        Reflect.setStaticField(activityThread, "sPackageManager", iface)
    }

    fun invalidateLoadedApkCache(packageName: String) {
        try {
            val at = currentActivityThread() ?: return
            for (field in arrayOf("mPackages", "mResourcePackages")) {
                try {
                    val map = Reflect.getField(at, field)
                    if (map is MutableMap<*, *>) map.remove(packageName)
                } catch (_: Throwable) {
                }
            }
        } catch (t: Throwable) {
            Log.w(LOG_TAG, "invalidateLoadedApkCache failed: ${t.message}")
        }
    }

    fun flushContextImplPackageManager(context: Context) {
        var base: Context = context.applicationContext ?: return
        while (base is ContextWrapper) base = base.baseContext
        try {
            Reflect.setField(base, "mPackageManager", null)
        } catch (_: Throwable) {
        }
    }
}
