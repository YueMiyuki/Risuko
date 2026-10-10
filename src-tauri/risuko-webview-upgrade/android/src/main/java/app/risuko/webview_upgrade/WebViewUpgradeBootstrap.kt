package app.risuko.webview_upgrade

import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import android.util.Log
import android.webkit.WebView
import app.risuko.webview_upgrade.swap.WebViewSwap

internal object WebViewUpgradeBootstrap {

    private const val PROP_FORCE = "debug.risuko.wvup.force"
    private const val PROP_NUDGE = "debug.risuko.wvup.nudge"
    private const val PROP_MIN_UPGRADE = "debug.risuko.wvup.minupgrade"
    private const val PROP_MIN_SUPPORTED = "debug.risuko.wvup.minsupported"

    fun run(context: Context) {
        if (!isMainProcess(context)) {
            return
        }

        try {
            val current = currentWebView(context)
            val systemMajor = current?.third ?: -1

            HiddenApi.exempt()

            val minUpgrade = SystemProps.getInt(PROP_MIN_UPGRADE) ?: WebViewUpgradeConfig.MIN_UPGRADE_MAJOR
            val minSupported = SystemProps.getInt(PROP_MIN_SUPPORTED) ?: WebViewUpgradeConfig.MIN_SUPPORTED_MAJOR

            val forcePkg = SystemProps.get(PROP_FORCE)
            if (forcePkg.isNotEmpty()) {
                Log.w(LOG_TAG, "debug force-swap to $forcePkg")
                trySwap(context, forcePkg)
                return
            }
            if (SystemProps.get(PROP_NUDGE) == "1") {
                Log.w(LOG_TAG, "debug force-nudge")
                OutdatedWebViewNotice.schedule(context, bestCandidatePackage(context))
                return
            }

            if (systemMajor >= minUpgrade) {
                Log.i(
                    LOG_TAG,
                    "system WebView: ${current?.first ?: "<none>"} v${current?.second ?: "?"} " +
                        "(major=$systemMajor); recent enough, skipping",
                )
                return
            }

            val candidate = bestCandidate(context, systemMajor)
            if (candidate != null) {
                Log.i(LOG_TAG, "candidate kernel: ${candidate.first} major=${candidate.second}")
                if (trySwap(context, candidate.first)) {
                    if (candidate.second in 0 until minSupported) {
                        OutdatedWebViewNotice.schedule(context, candidate.first)
                    }
                    return
                }
            } else {
                Log.i(LOG_TAG, "no newer installed WebView candidate found")
            }

            if (systemMajor in 0 until minSupported) {
                OutdatedWebViewNotice.schedule(context, candidate?.first ?: bestCandidatePackage(context))
            }
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "bootstrap failed", t)
        }
    }

    private fun trySwap(context: Context, packageName: String): Boolean {
        return try {
            WebViewSwap.swapToInstalledPackage(context, packageName)
            true
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "swap to $packageName failed; degrading", t)
            false
        }
    }

    private fun currentWebView(context: Context): Triple<String, String?, Int>? {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val pi = runCatching { WebView.getCurrentWebViewPackage() }.getOrNull()
            if (pi != null) return Triple(pi.packageName, pi.versionName, majorOf(pi.versionName))
        }
        for (pkg in CANDIDATE_PACKAGES) {
            val major = installedMajor(context, pkg) ?: continue
            return Triple(pkg, installedVersionName(context, pkg), major)
        }
        return null
    }

    private fun bestCandidate(context: Context, systemMajor: Int): Pair<String, Int>? {
        var best: Pair<String, Int>? = null
        for (pkg in CANDIDATE_PACKAGES) {
            val major = installedMajor(context, pkg) ?: continue
            if (major <= systemMajor) continue
            if (best == null || major > best.second) best = pkg to major
        }
        return best
    }

    private fun bestCandidatePackage(context: Context): String? {
        var best: Pair<String, Int>? = null
        for (pkg in CANDIDATE_PACKAGES) {
            val major = installedMajor(context, pkg) ?: continue
            if (best == null || major > best.second) best = pkg to major
        }
        return best?.first
    }

    private fun installedMajor(context: Context, pkg: String): Int? =
        majorOf(installedVersionName(context, pkg)).takeIf { it >= 0 }

    private fun installedVersionName(context: Context, pkg: String): String? = try {
        context.packageManager.getPackageInfo(pkg, 0).versionName
    } catch (_: PackageManager.NameNotFoundException) {
        null
    } catch (_: Throwable) {
        null
    }

    private fun majorOf(versionName: String?): Int {
        if (versionName.isNullOrEmpty()) return -1
        val firstDot = versionName.indexOf('.')
        val head = if (firstDot >= 0) versionName.substring(0, firstDot) else versionName
        return head.trim().toIntOrNull() ?: -1
    }
}
