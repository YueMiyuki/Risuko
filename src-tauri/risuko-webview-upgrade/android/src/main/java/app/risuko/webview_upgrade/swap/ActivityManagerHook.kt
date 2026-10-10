package app.risuko.webview_upgrade.swap

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.util.Log
import app.risuko.webview_upgrade.LOG_TAG
import app.risuko.webview_upgrade.reflect.Reflect
import app.risuko.webview_upgrade.sandbox.SandboxExtras
import java.util.regex.Pattern

internal class ActivityManagerHook(
    private val context: Context,
    private val targetWebViewPackage: String,
    private val webViewApkPath: String?,
    private val webViewLibsPath: String?,
) {
    private val hostPackageName: String = context.packageName
    private val maxStubServices: Int = resolveMaxStubServices(context)

    private var singleton: Any? = null
    private var originalAm: Any? = null
    private var hooked = false

    fun hook() {
        if (hooked) return
        val gDefault = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Reflect.getStaticField(Reflect.cls("android.app.ActivityManager"), "IActivityManagerSingleton")
        } else {
            Reflect.getStaticField(Reflect.cls("android.app.ActivityManagerNative"), "gDefault")
        } ?: run {
            Log.e(LOG_TAG, "ActivityManager singleton is null; cannot hook")
            return
        }
        singleton = gDefault
        val original = Reflect.getField(gDefault, "mInstance") ?: run {
            Log.e(LOG_TAG, "IActivityManager instance is null; cannot hook")
            return
        }
        originalAm = original

        val interceptors = mapOf(
            "bindIsolatedService" to Reflect.Interceptor { args, original2 -> dispatchBind(args, original2) },
            "bindServiceInstance" to Reflect.Interceptor { args, original2 -> dispatchBind(args, original2) },
        )
        val proxyAm = Reflect.newInterfaceProxy(original, interceptors)
        Reflect.setField(gDefault, "mInstance", proxyAm)
        hooked = true
        Log.i(LOG_TAG, "ActivityManagerHook active (host=$hostPackageName, target=$targetWebViewPackage)")
    }

    fun restore() {
        val s = singleton
        val o = originalAm
        if (hooked && s != null && o != null) {
            runCatching { Reflect.setField(s, "mInstance", o) }
            hooked = false
        }
    }

    private fun dispatchBind(args: Array<Any?>, original: () -> Any?): Any? {
        val redirected = args.filterIsInstance<Intent>().firstOrNull { redirectSandboxServiceIfNeeded(it) }
        return if (redirected != null) callBindServiceOnRealAM(args, original) else original()
    }

    private fun redirectSandboxServiceIfNeeded(intent: Intent): Boolean {
        val component = intent.component ?: return false

        if (component.packageName == hostPackageName &&
            component.className?.startsWith(STUB_SERVICE_CLASS_PREFIX) == true
        ) {
            ensureSandboxIntentExtras(intent)
            return true
        }

        if (component.packageName != targetWebViewPackage) return false
        val className = component.className ?: return false
        if (!className.contains(SANDBOX_SERVICE_NAME)) return false

        var sandboxIndex = 0
        val matcher = SANDBOX_SERVICE_INDEX_PATTERN.matcher(className)
        if (matcher.find()) {
            sandboxIndex = matcher.group(1)?.toIntOrNull() ?: 0
        }
        val stubIndex = if (maxStubServices > 0) sandboxIndex % maxStubServices else 0
        val stubComponent = ComponentName(hostPackageName, STUB_SERVICE_CLASS_PREFIX + stubIndex)

        val extras = intent.extras ?: Bundle()
        extras.putString(SandboxExtras.WEBVIEW_APK_PATH, webViewApkPath)
        extras.putString(SandboxExtras.WEBVIEW_LIBS_PATH, webViewLibsPath)
        extras.putString(SandboxExtras.WEBVIEW_PACKAGE_NAME, targetWebViewPackage)
        extras.putString(SandboxExtras.ORIGINAL_SERVICE_CLASS, className)
        putSharedLibs(extras)
        intent.putExtras(extras)

        Log.i(LOG_TAG, "sandbox redirect: ${component.flattenToShortString()} -> ${stubComponent.flattenToShortString()}")
        intent.component = stubComponent
        return true
    }

    private fun ensureSandboxIntentExtras(intent: Intent) {
        val extras = intent.extras ?: Bundle().also { intent.putExtras(it) }
        webViewApkPath?.let { extras.putString(SandboxExtras.WEBVIEW_APK_PATH, it) }
        webViewLibsPath?.let { extras.putString(SandboxExtras.WEBVIEW_LIBS_PATH, it) }
        extras.putString(SandboxExtras.WEBVIEW_PACKAGE_NAME, targetWebViewPackage)
        putSharedLibs(extras)
        intent.putExtras(extras)
    }

    private fun putSharedLibs(extras: Bundle) {
        try {
            val sharedLibs = WebViewSwap.replaceWebViewPackageInfo
                ?.applicationInfo?.sharedLibraryFiles
            if (sharedLibs != null) extras.putStringArray(SandboxExtras.WEBVIEW_SHARED_LIBS, sharedLibs)
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "failed to read WebView sharedLibs: ${t.message}")
        }
    }

    private fun callBindServiceOnRealAM(originalArgs: Array<Any?>, fallback: () -> Any?): Any? {
        val am = originalAm ?: return fallback()
        return try {
            val bindServiceMethod = BindServiceArgsAdapter.findBestBindServiceMethod(am, originalArgs)
            if (bindServiceMethod == null) {
                Log.e(LOG_TAG, "no bindService overload found on AMS")
                return fallback()
            }
            val adapted = BindServiceArgsAdapter.adapt(originalArgs, bindServiceMethod, hostPackageName)
            bindServiceMethod.invoke(am, *adapted)
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "forwarding bindService failed", t)
            fallback()
        }
    }

    companion object {
        private const val SANDBOX_SERVICE_NAME = "SandboxedProcessService"
        private val SANDBOX_SERVICE_INDEX_PATTERN = Pattern.compile("$SANDBOX_SERVICE_NAME(\\d+)$")
        private const val STUB_SERVICE_CLASS_PREFIX =
            "app.risuko.webview_upgrade.sandbox.Stub$SANDBOX_SERVICE_NAME"
        private const val FALLBACK_STUB_COUNT = 5

        private fun resolveMaxStubServices(context: Context): Int = try {
            val pi = context.packageManager.getPackageInfo(
                context.packageName, PackageManager.GET_SERVICES,
            )
            val services = pi.services
            if (services == null) {
                FALLBACK_STUB_COUNT
            } else {
                val num = Pattern.compile("StubSandboxedProcessService(\\d+)$")
                var maxIndex = -1
                for (s in services) {
                    val name = s.name ?: continue
                    val m = num.matcher(name)
                    if (m.find()) maxIndex = maxOf(maxIndex, m.group(1)?.toIntOrNull() ?: -1)
                }
                if (maxIndex >= 0) maxIndex + 1 else FALLBACK_STUB_COUNT
            }
        } catch (_: Throwable) {
            FALLBACK_STUB_COUNT
        }
    }
}
