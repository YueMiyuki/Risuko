package app.risuko.webview_upgrade

import android.content.Context
import android.content.pm.ApplicationInfo
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

    fun isTrusted(context: Context, packageName: String): Boolean = try {
        isSystemPackage(context, packageName) || hasPinnedSigner(context, packageName)
    } catch (t: Throwable) {
        Log.w(LOG_TAG, "trust check failed for $packageName", t)
        false
    }

    private fun isSystemPackage(context: Context, packageName: String): Boolean {
        val flags = context.packageManager.getApplicationInfo(packageName, 0).flags
        val system = ApplicationInfo.FLAG_SYSTEM or ApplicationInfo.FLAG_UPDATED_SYSTEM_APP
        return flags and system != 0
    }

    private fun hasPinnedSigner(context: Context, packageName: String): Boolean {
        val pin = PINNED_SHA256[packageName] ?: return false
        val pm = context.packageManager
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            return pm.hasSigningCertificate(
                packageName,
                hexToBytes(pin),
                PackageManager.CERT_INPUT_SHA256,
            )
        }
        @Suppress("DEPRECATION", "PackageManagerGetSignatures")
        val signatures = pm.getPackageInfo(packageName, PackageManager.GET_SIGNATURES).signatures
            ?: return false
        if (signatures.size != 1) return false
        return sha256Hex(signatures[0].toByteArray()) == pin
    }

    private fun sha256Hex(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes)
            .joinToString("") { "%02X".format(it) }

    private fun hexToBytes(hex: String): ByteArray =
        ByteArray(hex.length / 2) { i ->
            hex.substring(i * 2, i * 2 + 2).toInt(16).toByte()
        }
}
