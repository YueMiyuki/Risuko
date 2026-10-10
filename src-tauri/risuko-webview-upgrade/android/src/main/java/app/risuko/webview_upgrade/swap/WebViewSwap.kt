package app.risuko.webview_upgrade.swap

import android.content.Context
import android.content.pm.PackageInfo
import android.content.pm.PackageManager
import android.os.Build
import android.os.Looper
import android.util.Log
import android.webkit.RenderProcessGoneDetail
import android.webkit.WebView
import android.webkit.WebViewClient
import app.risuko.webview_upgrade.LOG_TAG
import java.io.File

internal object WebViewSwap {

    @Volatile
    var replaceWebViewPackageInfo: PackageInfo? = null
        private set

    private var packageManagerHook: PackageManagerHook? = null
    private var activityManagerHook: ActivityManagerHook? = null

    class PreconditionException(message: String) : Exception(message)

    @Synchronized
    fun swapToInstalledPackage(context: Context, packageName: String): PackageInfo? {
        checkPreconditions()

        val packageInfo = context.packageManager.getPackageInfo(
            packageName,
            PackageManager.GET_SHARED_LIBRARY_FILES or PackageManager.GET_META_DATA,
        )
        val appInfo = packageInfo.applicationInfo
            ?: throw PreconditionException("no applicationInfo for $packageName")
        val apkPath: String? = appInfo.sourceDir
        val libsPath: String? = deriveNativeLibsRoot(appInfo)

        if (packageManagerHook == null && apkPath != null && libsPath != null) {
            packageManagerHook = PackageManagerHook(context, packageName, apkPath, libsPath)
        }
        val updateServiceHook = WebViewUpdateServiceHook(context, packageName)
        if (activityManagerHook == null) {
            activityManagerHook = ActivityManagerHook(context, packageName, apkPath, libsPath)
        }

        var success = false
        try {
            packageManagerHook?.hook()
            updateServiceHook.hook()
            activityManagerHook?.hook()

            lockProviderBinding(context)
            replaceWebViewPackageInfo = currentWebViewPackage()
            success = true
            Log.i(LOG_TAG, "swap succeeded -> $packageName (${replaceWebViewPackageInfo?.versionName})")
            return replaceWebViewPackageInfo
        } finally {
            runCatching { updateServiceHook.restore() }
            if (!success) {
                runCatching { packageManagerHook?.restore() }
                packageManagerHook = null
                runCatching { activityManagerHook?.restore() }
                activityManagerHook = null
            }
        }
    }

    private fun checkPreconditions() {
        if (Looper.myLooper() != Looper.getMainLooper()) {
            throw PreconditionException("swap must run on the main thread")
        }
        if (Framework.providerInstance() != null) {
            throw PreconditionException("WebView provider already bound; swap is too late")
        }
    }

    private fun lockProviderBinding(context: Context) {
        val webView = WebView(context)
        webView.webViewClient = object : WebViewClient() {
            override fun onRenderProcessGone(view: WebView, detail: RenderProcessGoneDetail): Boolean {
                Log.w(LOG_TAG, "throwaway WebView render process gone, didCrash=${detail.didCrash()}")
                return true
            }
        }
    }

    private fun currentWebViewPackage(): PackageInfo? =
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            runCatching { WebView.getCurrentWebViewPackage() }.getOrNull()
        } else {
            null
        }

    private fun deriveNativeLibsRoot(appInfo: android.content.pm.ApplicationInfo): String? {
        runCatching {
            val f = android.content.pm.ApplicationInfo::class.java.getDeclaredField("nativeLibraryRootDir")
            f.isAccessible = true
            (f.get(appInfo) as? String)?.takeIf { it.isNotEmpty() }
        }.getOrNull()?.let { return it }
        return appInfo.nativeLibraryDir?.takeIf { it.isNotEmpty() }
            ?.let { File(it).parentFile?.absolutePath }
    }
}
