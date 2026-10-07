package app.risuko.webview_upgrade

import android.app.Activity
import android.app.AlertDialog
import android.app.Application
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.util.Log

/**
 * Native fallback shown when the system WebView is too old to render the UI and
 * no newer kernel could be swapped in
 */
internal object OutdatedWebViewNotice {

    @Volatile
    private var scheduled = false

    @Synchronized
    fun schedule(context: Context, recommendedPackage: String?) {
        if (scheduled) return
        val app = context.applicationContext as? Application ?: run {
            Log.w(LOG_TAG, "cannot schedule notice: app context is not an Application")
            return
        }
        scheduled = true
        app.registerActivityLifecycleCallbacks(object : Application.ActivityLifecycleCallbacks {
            override fun onActivityResumed(activity: Activity) {
                if (activity.isFinishing) return
                app.unregisterActivityLifecycleCallbacks(this)
                showDialog(activity, recommendedPackage)
            }

            override fun onActivityCreated(activity: Activity, savedInstanceState: Bundle?) {}
            override fun onActivityStarted(activity: Activity) {}
            override fun onActivityPaused(activity: Activity) {}
            override fun onActivityStopped(activity: Activity) {}
            override fun onActivitySaveInstanceState(activity: Activity, outState: Bundle) {}
            override fun onActivityDestroyed(activity: Activity) {}
        })
    }

    private fun showDialog(activity: Activity, recommendedPackage: String?) {
        if (activity.isFinishing) return
        try {
            val message = activity.getString(R.string.webview_upgrade_outdated_message)
            val updateLabel = activity.getString(R.string.webview_upgrade_update_button)
            val dismissLabel = activity.getString(R.string.webview_upgrade_dismiss_button)
            val target = recommendedPackage ?: "com.google.android.webview"
            AlertDialog.Builder(activity)
                .setCancelable(true)
                .setMessage(message)
                .setPositiveButton(updateLabel) { _, _ -> openStore(activity, target) }
                .setNegativeButton(dismissLabel, null)
                .show()
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "failed to show outdated-WebView notice", t)
        }
    }

    private fun openStore(activity: Activity, packageName: String) {
        val market = Intent(Intent.ACTION_VIEW, Uri.parse("market://details?id=$packageName"))
            .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        try {
            activity.startActivity(market)
        } catch (_: Throwable) {
            try {
                activity.startActivity(
                    Intent(
                        Intent.ACTION_VIEW,
                        Uri.parse("https://play.google.com/store/apps/details?id=$packageName"),
                    ).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK),
                )
            } catch (t: Throwable) {
                Log.e(LOG_TAG, "no store available to update $packageName", t)
            }
        }
    }
}
