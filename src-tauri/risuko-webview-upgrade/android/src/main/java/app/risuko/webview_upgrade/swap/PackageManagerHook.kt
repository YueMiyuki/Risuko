package app.risuko.webview_upgrade.swap

import android.content.ComponentName
import android.content.Context
import android.content.pm.ApplicationInfo
import android.content.pm.PackageInfo
import android.content.pm.PackageManager
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.os.IInterface
import android.os.Process
import android.util.Log
import app.risuko.webview_upgrade.LOG_TAG
import app.risuko.webview_upgrade.reflect.Reflect
import java.io.File

internal class PackageManagerHook(
    private val context: Context,
    private val webViewPackageName: String,
    private val apkPath: String,
    private val libsPath: String,
) : BinderHook() {

    private lateinit var binderCache: MutableMap<String, IBinder>
    private var cachedWebViewAppInfo: ApplicationInfo? = null
    private var realPackageManagerInterface: IInterface? = null

    private val interceptors: Map<String, Reflect.Interceptor> = mapOf(
        "getPackageInfo" to Reflect.Interceptor { args, original ->
            val pkg = args.getOrNull(0) as? String ?: return@Interceptor original()
            if (pkg != webViewPackageName) return@Interceptor original()
            getWebViewPackageInfo(intFlag(args))
                ?: throw RuntimeException("apkPath is not valid $apkPath")
        },
        "getApplicationInfo" to Reflect.Interceptor { args, original ->
            val pkg = args.getOrNull(0) as? String ?: return@Interceptor original()
            if (pkg != webViewPackageName) return@Interceptor original()
            Framework.invalidateLoadedApkCache(webViewPackageName)
            getWebViewApplicationInfo(intFlag(args))
                ?: throw RuntimeException("apkPath is not valid $apkPath")
        },
        "getComponentEnabledSetting" to Reflect.Interceptor { args, original ->
            val cn = args.getOrNull(0) as? ComponentName ?: return@Interceptor original()
            if (cn.packageName == webViewPackageName) {
                PackageManager.COMPONENT_ENABLED_STATE_DISABLED
            } else {
                original()
            }
        },
        "getServiceInfo" to Reflect.Interceptor { args, original ->
            val cn = args.getOrNull(0) as? ComponentName ?: return@Interceptor original()
            handleGetServiceInfo(cn) ?: original()
        },
        "getInstallerPackageName" to Reflect.Interceptor { args, original ->
            val pkg = args.getOrNull(0) as? String
            if (pkg == webViewPackageName) "com.android.vending" else original()
        },
        "asBinder" to Reflect.Interceptor { _, original ->
            proxyBinder ?: original()
        },
    )

    private fun intFlag(args: Array<Any?>): Int = (args.getOrNull(1) as? Number)?.toInt() ?: 0

    private fun getWebViewApplicationInfo(flags: Int): ApplicationInfo? =
        getWebViewPackageInfo(flags)?.applicationInfo

    private fun getWebViewPackageInfo(flags: Int): PackageInfo? {
        var f = flags
        var pi = context.packageManager.getPackageArchiveInfo(apkPath, f)
        if (pi == null) {
            @Suppress("DEPRECATION")
            f = f and PackageManager.GET_SIGNATURES.inv()
            pi = context.packageManager.getPackageArchiveInfo(apkPath, f)
        }
        if (pi == null) return null
        fillPackageInfo(pi)
        return pi
    }

    private fun fillPackageInfo(packageInfo: PackageInfo) {
        val is64Bit = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) Process.is64Bit() else false
        val preferredAbis =
            (if (is64Bit) Build.SUPPORTED_64_BIT_ABIS else Build.SUPPORTED_32_BIT_ABIS).clone()

        val libsDir = File(libsPath)
        if (!libsDir.exists()) throw RuntimeException("libsDir not exist $libsPath")
        if (libsDir.list() == null) throw RuntimeException("abi dir not exist in $libsPath")

        val resolved = resolveNativeLibraryDir(libsDir, is64Bit)
            ?: throw NullPointerException(
                "unable to find supported abis ${preferredAbis.contentToString()} in dir $libsPath"
            )
        val (cpuAbi, nativeLibraryDir) = resolved

        val ai = packageInfo.applicationInfo ?: return
        runCatching { Reflect.setField(ai, "primaryCpuAbi", cpuAbi) }
        runCatching { Reflect.setField(ai, "nativeLibraryRootDir", libsPath) }
        ai.nativeLibraryDir = nativeLibraryDir
        ai.sourceDir = apkPath
        ai.publicSourceDir = apkPath
        ai.flags = ai.flags or ApplicationInfo.FLAG_INSTALLED or ApplicationInfo.FLAG_HAS_CODE
        cachedWebViewAppInfo = ai
    }

    private fun handleGetServiceInfo(component: ComponentName): ServiceInfo? {
        val className = component.className ?: return null
        if (component.packageName != webViewPackageName ||
            !className.contains("SandboxedProcessService")
        ) {
            return null
        }
        return ServiceInfo().apply {
            name = className
            packageName = component.packageName
            exported = true
            enabled = true
            applicationInfo = cachedWebViewAppInfo ?: getWebViewApplicationInfo(0)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                flags = flags or ServiceInfo.FLAG_ISOLATED_PROCESS or ServiceInfo.FLAG_EXTERNAL_SERVICE
            }
        }
    }

    private fun resolveNativeLibraryDir(libsDir: File, is64Bit: Boolean): Pair<String, String>? {
        val preferredAbis =
            (if (is64Bit) Build.SUPPORTED_64_BIT_ABIS else Build.SUPPORTED_32_BIT_ABIS).clone()
        val sortedAbis = preferredAbis.clone().also { it.sort() }
        val list = libsDir.list() ?: return null

        if (list.any { it.endsWith(".so") }) {
            val cpuAbi = preferredAbis.firstOrNull() ?: return null
            Log.w(LOG_TAG, "native .so in lib root, cpuAbi=$cpuAbi")
            return cpuAbi to libsDir.absolutePath
        }
        for (name in list) {
            if (sortedAbis.binarySearchExists(name)) {
                val d = File(libsDir, name)
                if (d.isDirectory) return name to d.absolutePath
            }
        }
        for (canonical in preferredAbis) {
            val d = File(libsDir, canonical)
            if (d.isDirectory) return canonical to d.absolutePath
            shortDirNameFor(canonical)?.let { short ->
                val d2 = File(libsDir, short)
                if (d2.isDirectory) return canonical to d2.absolutePath
            }
        }
        for (name in list) {
            val sub = File(libsDir, name)
            if (!sub.isDirectory) continue
            val subList = sub.list() ?: continue
            if (subList.any { it.endsWith(".so") }) {
                val cpuAbi = canonicalAbiFromDiskName(name, preferredAbis)
                    ?: name.takeIf { sortedAbis.binarySearchExists(it) }
                    ?: preferredAbis.firstOrNull() ?: name
                return cpuAbi to sub.absolutePath
            }
        }
        return null
    }

    private fun Array<String>.binarySearchExists(name: String): Boolean =
        java.util.Arrays.binarySearch(this, name) >= 0

    private fun shortDirNameFor(canonical: String): String? = when (canonical) {
        "arm64-v8a" -> "arm64"
        "armeabi-v7a", "armeabi" -> "arm"
        else -> null
    }

    private fun canonicalAbiFromDiskName(diskName: String, preferredAbis: Array<String>): String? {
        if (diskName == "arm64") {
            preferredAbis.firstOrNull { it == "arm64-v8a" }?.let { return it }
            preferredAbis.firstOrNull { it.startsWith("arm64") }?.let { return it }
        }
        if (diskName == "arm") {
            preferredAbis.firstOrNull { it == "armeabi-v7a" || it == "armeabi" }?.let { return it }
        }
        return null
    }

    override fun onTargetBinderObtain(): IBinder =
        Framework.getService(Framework.SERVICE_PACKAGE)
            ?: throw IllegalStateException("package service unavailable")

    override fun onProxyBinderCreate(binder: IBinder): ProxyBinder {
        val realInterface: IInterface = Framework.packageManagerAsInterface(binder)
        realPackageManagerInterface = realInterface
        val proxyInterface = Reflect.newInterfaceProxy(realInterface, interceptors) as IInterface
        binderCache = Framework.serviceCache()
        return ProxyBinder(realInterface.asBinder(), proxyInterface)
    }

    override fun onTargetBinderRestore(binder: IBinder) {
        val realInterface = realPackageManagerInterface
            ?: Framework.packageManagerAsInterface(binder)
        binderCache[Framework.SERVICE_PACKAGE] = binder
        Framework.setActivityThreadPackageManager(realInterface)
        Framework.flushContextImplPackageManager(context)
    }

    override fun onProxyBinderReplace(binder: ProxyBinder) {
        binderCache[Framework.SERVICE_PACKAGE] = binder
        Framework.setActivityThreadPackageManager(binder.currentProxyInterface)
        Framework.flushContextImplPackageManager(context)
    }
}
