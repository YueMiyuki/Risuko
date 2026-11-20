package app.risuko.webview_upgrade

import android.content.Context
import android.content.pm.ApplicationInfo
import android.content.pm.PackageInfo
import android.content.pm.PackageManager
import android.os.Build
import android.util.Log
import java.security.MessageDigest

internal object PackageTrust {

    private val PINNED_SHA256 = mapOf(
        "com.google.android.webview" to
            "6FAF3C4140407473400934D117815A21AF1CFEFC5C0BEE61C858BC3D72BA6FE5",
        "com.google.android.webview.beta" to
            "639FC7F1C168CB8A0DF3F66F604F282967F696E91ED28612F8D8511A3748D2AA",
        "com.google.android.webview.dev" to
            "A7E383AB8A6F4F0C6AD609503C17E40D9588D2FDCB45112F59F65273135F1D9B",
        "com.google.android.webview.canary" to
            "DA2E1B4AB9926BC7B081D4E9E57E3C9EC77EE51F5F0F767EE84C131C574E20EE",
    )

    @Suppress("PackageManagerGetSignatures")
    fun verifiedPackageInfo(context: Context, packageName: String): PackageInfo? = try {
        val packageInfo = context.packageManager.getPackageInfo(packageName, infoFlags())
        if (isSystemPackage(packageInfo) || hasPinnedSigner(packageInfo)) packageInfo else null
    } catch (t: Throwable) {
        Log.w(LOG_TAG, "trust check failed for $packageName", t)
        null
    }

    @Suppress("DEPRECATION")
    private fun infoFlags(): Int {
        val flags = PackageManager.GET_SHARED_LIBRARY_FILES or PackageManager.GET_META_DATA
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            flags or PackageManager.GET_SIGNING_CERTIFICATES
        } else {
            flags or PackageManager.GET_SIGNATURES
        }
    }

    private fun isSystemPackage(packageInfo: PackageInfo): Boolean {
        val flags = packageInfo.applicationInfo?.flags ?: return false
        val system = ApplicationInfo.FLAG_SYSTEM or ApplicationInfo.FLAG_UPDATED_SYSTEM_APP
        return flags and system != 0
    }

    @Suppress("DEPRECATION")
    private fun hasPinnedSigner(packageInfo: PackageInfo): Boolean {
        val pin = PINNED_SHA256[packageInfo.packageName] ?: return false
        val certificates = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            packageInfo.signingInfo?.signingCertificateHistory
        } else {
            packageInfo.signatures?.takeIf { it.size == 1 }
        } ?: return false
        return certificates.any { sha256Hex(it.toByteArray()) == pin }
    }

    private fun sha256Hex(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes)
            .joinToString("") { "%02X".format(it) }
}
