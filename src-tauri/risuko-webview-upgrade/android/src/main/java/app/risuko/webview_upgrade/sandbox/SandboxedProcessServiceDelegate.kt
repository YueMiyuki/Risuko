package app.risuko.webview_upgrade.sandbox

import android.app.Service
import android.content.Context
import android.content.ContextWrapper
import android.content.Intent
import android.content.res.AssetManager
import android.content.res.Resources
import android.os.Build
import android.os.IBinder
import android.os.Process
import android.util.Log
import app.risuko.webview_upgrade.LOG_TAG
import app.risuko.webview_upgrade.swap.Framework
import app.risuko.webview_upgrade.swap.PackageManagerHook
import dalvik.system.PathClassLoader
import java.io.File
import java.lang.ref.WeakReference
import java.lang.reflect.Array as ReflectArray

internal class SandboxedProcessServiceDelegate {

    private var hostService: Service? = null
    private var appContext: Context? = null

    private var realService: Service? = null
    private var onBindMethod: java.lang.reflect.Method? = null
    private var onDestroyMethod: java.lang.reflect.Method? = null
    private var onRebindMethod: java.lang.reflect.Method? = null
    private var onUnbindMethod: java.lang.reflect.Method? = null

    private var dexInjected = false
    private var ready = false

    fun onCreate(hostService: Service, appContext: Context) {
        this.hostService = hostService
        this.appContext = appContext
    }

    fun onBind(intent: Intent): IBinder? {
        ensureReady(intent)
        val real = realService
        val bind = onBindMethod
        if (!ready || real == null || bind == null) {
            Log.e(LOG_TAG, "sandbox onBind: real service not ready")
            return null
        }
        return try {
            bind.invoke(real, intent) as? IBinder
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "sandbox onBind delegate failed", t)
            null
        }
    }

    fun onDestroy() {
        if (!ready) return
        val real = realService ?: return
        runCatching { onDestroyMethod?.invoke(real) }
    }

    fun onRebind(intent: Intent) {
        if (!ready) return
        val real = realService ?: return
        runCatching { onRebindMethod?.invoke(real, intent) }
    }

    fun onUnbind(intent: Intent): Boolean {
        if (!ready) return false
        val real = realService ?: return false
        return runCatching { onUnbindMethod?.invoke(real, intent) as? Boolean ?: false }.getOrDefault(false)
    }

    @Synchronized
    private fun ensureReady(intent: Intent) {
        if (ready) return
        val app = appContext ?: return

        val apkPath = intent.getStringExtra(SandboxExtras.WEBVIEW_APK_PATH)
        if (apkPath == null) {
            Log.e(LOG_TAG, "sandbox: missing APK path extra")
            return
        }
        var libsPath = intent.getStringExtra(SandboxExtras.WEBVIEW_LIBS_PATH)
        val sharedLibs = intent.getStringArrayExtra(SandboxExtras.WEBVIEW_SHARED_LIBS)
        var realClassName = intent.getStringExtra(SandboxExtras.ORIGINAL_SERVICE_CLASS)
        if (realClassName.isNullOrEmpty()) {
            realClassName = "org.chromium.content.app.SandboxedProcessService0"
        }

        var webViewPkg = intent.getStringExtra(SandboxExtras.WEBVIEW_PACKAGE_NAME)
        if (webViewPkg.isNullOrEmpty()) {
            webViewPkg = intent.getStringExtra(SandboxExtras.CHROMIUM_BROWSER_PACKAGE)
        }
        if (webViewPkg.isNullOrEmpty()) {
            Log.e(LOG_TAG, "sandbox: missing WebView package name")
            return
        }

        if (libsPath == null) {
            libsPath = deriveLibsRootFromInstalledPkg(webViewPkg)
        }
        val nativeLibDir = resolveNativeLibraryDir(libsPath)

        installSandboxPmsHookOnce(app, webViewPkg, apkPath, libsPath)
        patchApplicationInfo(apkPath, nativeLibDir)

        val webViewCtx = tryCreateWebViewPackageContext(webViewPkg)

        if (webViewCtx != null && installChromiumDelegatingClassLoader(webViewCtx.classLoader)) {
            if (!dexInjected) dexInjected = injectDex(apkPath, nativeLibDir, sharedLibs, false)
            if (!dexInjected) {
                Log.e(LOG_TAG, "dex injection failed (delegating CL path)")
                return
            }
            val attachCtx = SandboxedWebViewServiceContext(webViewCtx, app)
            ready = initRealService(realClassName, attachCtx, useAppClassLoader = false)
            if (ready) return
            ready = initRealService(realClassName, attachCtx, useAppClassLoader = true)
            if (ready) return
        }

        if (!dexInjected) dexInjected = injectDex(apkPath, nativeLibDir, sharedLibs, true)
        if (!dexInjected) {
            Log.e(LOG_TAG, "dex injection failed")
            return
        }
        if (webViewCtx != null) {
            val attachCtx = SandboxedWebViewServiceContext(webViewCtx, app)
            ready = initRealService(realClassName, attachCtx, useAppClassLoader = true)
            if (ready) return
        }
        val base = hostService?.baseContext ?: return
        ready = initRealService(realClassName, base, useAppClassLoader = false)
    }

    private fun tryCreateWebViewPackageContext(webViewPkg: String): Context? {
        Framework.invalidateLoadedApkCache(webViewPkg)
        return try {
            appContext?.createPackageContext(
                webViewPkg,
                Context.CONTEXT_INCLUDE_CODE or Context.CONTEXT_IGNORE_SECURITY,
            )
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "createPackageContext failed: $webViewPkg", t)
            null
        }
    }

    private fun deriveLibsRootFromInstalledPkg(pkgName: String): String? {
        return try {
            val ai = appContext?.packageManager?.getApplicationInfo(pkgName, 0) ?: return null
            runCatching {
                val f = android.content.pm.ApplicationInfo::class.java.getDeclaredField("nativeLibraryRootDir")
                f.isAccessible = true
                (f.get(ai) as? String)?.takeIf { it.isNotEmpty() }
            }.getOrNull()?.let { return it }
            ai.nativeLibraryDir?.takeIf { it.isNotEmpty() }?.let { File(it).parentFile?.absolutePath }
        } catch (t: Throwable) {
            Log.w(LOG_TAG, "deriveLibsRootFromInstalledPkg failed: ${t.message}")
            null
        }
    }

    private fun resolveNativeLibraryDir(libsRoot: String?): String? {
        if (libsRoot == null) return null
        val dir = File(libsRoot)
        if (!dir.isDirectory) return libsRoot
        val names = dir.list()
        if (names != null && names.any { it.endsWith(".so") }) return dir.absolutePath
        val is64 = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) Process.is64Bit() else false
        val abis = (if (is64) Build.SUPPORTED_64_BIT_ABIS else Build.SUPPORTED_32_BIT_ABIS).clone()
        abis.sort()
        if (names != null) {
            for (name in names) {
                if (java.util.Arrays.binarySearch(abis, name) >= 0) return File(dir, name).absolutePath
            }
        }
        return dir.absolutePath
    }

    private fun patchApplicationInfo(apkPath: String, nativeLibraryDir: String?) {
        try {
            val appInfo = appContext?.applicationInfo ?: return
            appInfo.sourceDir = apkPath
            appInfo.publicSourceDir = apkPath
            if (nativeLibraryDir != null) appInfo.nativeLibraryDir = nativeLibraryDir
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "patchApplicationInfo failed", t)
        }
    }

    private fun installChromiumDelegatingClassLoader(webViewClassLoader: ClassLoader): Boolean {
        return try {
            val app = appContext ?: return false
            val atClass = Class.forName("android.app.ActivityThread")
            val at = atClass.getMethod("currentActivityThread").invoke(null)
            val mPackagesField = atClass.getDeclaredField("mPackages").apply { isAccessible = true }
            @Suppress("UNCHECKED_CAST")
            val mPackages = mPackagesField.get(at) as? Map<String, WeakReference<*>>
            val pkg = app.packageName
            val ref = mPackages?.get(pkg) ?: run {
                Log.e(LOG_TAG, "delegating CL: no host LoadedApk for $pkg")
                return false
            }
            val loadedApk = ref.get() ?: run {
                Log.e(LOG_TAG, "delegating CL: LoadedApk ref empty")
                return false
            }
            val loadedApkClass = Class.forName("android.app.LoadedApk")
            val mClassLoaderField = loadedApkClass.getDeclaredField("mClassLoader").apply { isAccessible = true }
            var appCl = mClassLoaderField.get(loadedApk) as? ClassLoader
            if (appCl == null) {
                val getCl = loadedApkClass.getDeclaredMethod("getClassLoader").apply { isAccessible = true }
                appCl = getCl.invoke(loadedApk) as? ClassLoader
            }
            if (appCl is ChromiumDelegatingClassLoader) return true
            if (appCl == null) return false
            val composite = ChromiumDelegatingClassLoader(appCl, webViewClassLoader)
            mClassLoaderField.set(loadedApk, composite)
            patchContextImplClassLoaderField(app, composite)
            hostService?.let { patchContextImplClassLoaderField(it, composite) }
            true
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "installChromiumDelegatingClassLoader failed", t)
            false
        }
    }

    private fun patchContextImplClassLoaderField(ctx: Context, cl: ClassLoader) {
        var base: Context = ctx
        while (base is ContextWrapper) base = base.baseContext
        val implClass = Class.forName("android.app.ContextImpl")
        if (!implClass.isInstance(base)) return
        val f = implClass.getDeclaredField("mClassLoader").apply { isAccessible = true }
        f.set(base, cl)
    }

    private fun injectDex(
        apkPath: String,
        nativeLibDir: String?,
        sharedLibs: Array<String>?,
        mergeWebViewApkIntoHost: Boolean,
    ): Boolean {
        return try {
            if (!mergeWebViewApkIntoHost && (sharedLibs == null || sharedLibs.isEmpty())) {
                return true
            }
            val currentCl = appContext?.classLoader ?: return false
            val dexPathListClass = Class.forName("dalvik.system.DexPathList")
            val dexElementsField = dexPathListClass.getDeclaredField("dexElements").apply { isAccessible = true }
            val nativeLibPathElementsField = runCatching {
                dexPathListClass.getDeclaredField("nativeLibraryPathElements").apply { isAccessible = true }
            }.getOrNull()
            val pathListField = Class.forName("dalvik.system.BaseDexClassLoader")
                .getDeclaredField("pathList").apply { isAccessible = true }

            var webviewDexElements = arrayOfNulls<Any>(0)
            var webviewNativeElements = arrayOfNulls<Any>(0)
            if (mergeWebViewApkIntoHost) {
                val webviewLoader = PathClassLoader(apkPath, nativeLibDir, currentCl.parent)
                val webviewPathList = pathListField.get(webviewLoader)
                webviewDexElements = dexElementsField.get(webviewPathList) as Array<Any?>
                if (nativeLibPathElementsField != null) {
                    (nativeLibPathElementsField.get(webviewPathList) as? Array<Any?>)?.let { webviewNativeElements = it }
                }
            }

            var sharedDexElements = arrayOfNulls<Any>(0)
            var sharedNativeElements = arrayOfNulls<Any>(0)
            if (sharedLibs != null && sharedLibs.isNotEmpty()) {
                val sharedPath = sharedLibs.joinToString(File.pathSeparator)
                val sharedLoader = PathClassLoader(sharedPath, null, currentCl.parent)
                val sharedPathList = pathListField.get(sharedLoader)
                (dexElementsField.get(sharedPathList) as? Array<Any?>)?.let { sharedDexElements = it }
                if (nativeLibPathElementsField != null) {
                    (nativeLibPathElementsField.get(sharedPathList) as? Array<Any?>)?.let { sharedNativeElements = it }
                }
            }

            val currentPathList = pathListField.get(currentCl)
            val oldDexElements = dexElementsField.get(currentPathList) as? Array<Any?>
            val oldLen = oldDexElements?.size ?: 0
            val webLen = webviewDexElements.size
            val sharedLen = sharedDexElements.size
            if (webLen == 0 && sharedLen == 0) {
                Log.e(LOG_TAG, "no dex elements extracted")
                return false
            }
            val componentType = (oldDexElements ?: webviewDexElements).javaClass.componentType!!
            @Suppress("UNCHECKED_CAST")
            val combined = ReflectArray.newInstance(componentType, webLen + sharedLen + oldLen) as Array<Any?>
            if (webLen > 0) System.arraycopy(webviewDexElements, 0, combined, 0, webLen)
            if (sharedLen > 0) System.arraycopy(sharedDexElements, 0, combined, webLen, sharedLen)
            if (oldLen > 0) System.arraycopy(oldDexElements!!, 0, combined, webLen + sharedLen, oldLen)
            dexElementsField.set(currentPathList, combined)

            if (nativeLibPathElementsField != null) {
                val oldNative = nativeLibPathElementsField.get(currentPathList) as? Array<Any?>
                val oldNativeLen = oldNative?.size ?: 0
                val webNativeLen = webviewNativeElements.size
                val sharedNativeLen = sharedNativeElements.size
                if (webNativeLen > 0 || sharedNativeLen > 0) {
                    val nativeComponentType = when {
                        webNativeLen > 0 -> webviewNativeElements.javaClass.componentType!!
                        oldNativeLen > 0 -> oldNative!!.javaClass.componentType!!
                        else -> sharedNativeElements.javaClass.componentType!!
                    }
                    @Suppress("UNCHECKED_CAST")
                    val combinedNative = ReflectArray.newInstance(
                        nativeComponentType, webNativeLen + sharedNativeLen + oldNativeLen,
                    ) as Array<Any?>
                    if (webNativeLen > 0) System.arraycopy(webviewNativeElements, 0, combinedNative, 0, webNativeLen)
                    if (sharedNativeLen > 0) System.arraycopy(sharedNativeElements, 0, combinedNative, webNativeLen, sharedNativeLen)
                    if (oldNativeLen > 0) System.arraycopy(oldNative!!, 0, combinedNative, webNativeLen + sharedNativeLen, oldNativeLen)
                    nativeLibPathElementsField.set(currentPathList, combinedNative)
                }
            }
            true
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "dex injection failed", t)
            false
        }
    }

    private fun initRealService(
        realClassName: String,
        contextForAttach: Context,
        useAppClassLoader: Boolean,
    ): Boolean {
        return try {
            val cl = if (useAppClassLoader && appContext != null) {
                appContext!!.classLoader
            } else {
                contextForAttach.classLoader
            }
            val realClass = cl.loadClass(realClassName)
            val real = realClass.newInstance() as Service
            realService = real

            val attachBaseContext = ContextWrapper::class.java
                .getDeclaredMethod("attachBaseContext", Context::class.java).apply { isAccessible = true }
            attachBaseContext.invoke(real, contextForAttach)

            onBindMethod = realClass.getMethod("onBind", Intent::class.java).apply { isAccessible = true }
            onDestroyMethod = runCatching { realClass.getMethod("onDestroy").apply { isAccessible = true } }.getOrNull()
            onRebindMethod = runCatching {
                realClass.getMethod("onRebind", Intent::class.java).apply { isAccessible = true }
            }.getOrNull()
            onUnbindMethod = runCatching {
                realClass.getMethod("onUnbind", Intent::class.java).apply { isAccessible = true }
            }.getOrNull()

            realClass.getMethod("onCreate").apply { isAccessible = true }.invoke(real)
            Log.i(LOG_TAG, "real sandbox service onCreate ok: $realClassName")
            true
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "real sandbox service init failed: ${t.message}")
            false
        }
    }

    private class ChromiumDelegatingClassLoader(
        private val appClassLoader: ClassLoader,
        private val chromiumClassLoader: ClassLoader,
    ) : ClassLoader(appClassLoader.parent) {
        override fun loadClass(name: String, resolve: Boolean): Class<*> {
            synchronized(this) {
                var c = findLoadedClass(name)
                if (c == null) {
                    c = if (name.startsWith("org.chromium.")) {
                        chromiumClassLoader.loadClass(name)
                    } else {
                        appClassLoader.loadClass(name)
                    }
                }
                if (resolve) resolveClass(c)
                return c
            }
        }
    }

    private class SandboxedWebViewServiceContext(
        webViewPackageContext: Context,
        hostAppContext: Context,
    ) : ContextWrapper(webViewPackageContext) {
        private val appContextProxy: Context =
            SandboxedApplicationContext(hostAppContext, webViewPackageContext)

        override fun getApplicationContext(): Context = appContextProxy
    }

    private class SandboxedApplicationContext(
        hostAppContext: Context,
        private val webViewContext: Context,
    ) : ContextWrapper(hostAppContext) {
        private val realHostApplicationContext: Context =
            hostAppContext.applicationContext ?: hostAppContext

        override fun getAssets(): AssetManager = webViewContext.assets
        override fun getResources(): Resources = webViewContext.resources
        override fun getApplicationContext(): Context = realHostApplicationContext
    }

    companion object {
        private val pmsLock = Any()

        @Volatile
        private var sandboxPmsInstalled = false

        private fun installSandboxPmsHookOnce(
            appContext: Context,
            webViewPkg: String,
            apkPath: String,
            libsPath: String?,
        ) {
            synchronized(pmsLock) {
                if (sandboxPmsInstalled) return
                if (libsPath == null) {
                    Log.i(LOG_TAG, "sandbox: libsPath null, skipping PMS hook (system handles installed pkg)")
                    return
                }
                PackageManagerHook(appContext, webViewPkg, apkPath, libsPath).hook()
                sandboxPmsInstalled = true
                Log.i(LOG_TAG, "sandbox PMS hook installed for $webViewPkg")
            }
        }
    }
}
