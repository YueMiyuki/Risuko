package app.risuko.webview_upgrade

import android.content.ContentProvider
import android.content.ContentValues
import android.database.Cursor
import android.net.Uri
import android.util.Log

class BootstrapProvider : ContentProvider() {

    override fun onCreate(): Boolean {
        val ctx = context
        if (ctx == null) {
            Log.w(LOG_TAG, "BootstrapProvider has no context; skipping")
            return false
        }
        try {
            WebViewUpgradeBootstrap.run(ctx)
        } catch (t: Throwable) {
            Log.e(LOG_TAG, "WebView upgrade bootstrap threw", t)
        }
        return true
    }

    override fun query(
        uri: Uri,
        projection: Array<out String>?,
        selection: String?,
        selectionArgs: Array<out String>?,
        sortOrder: String?,
    ): Cursor? = null

    override fun getType(uri: Uri): String? = null
    override fun insert(uri: Uri, values: ContentValues?): Uri? = null
    override fun delete(uri: Uri, selection: String?, selectionArgs: Array<out String>?): Int = 0
    override fun update(
        uri: Uri,
        values: ContentValues?,
        selection: String?,
        selectionArgs: Array<out String>?,
    ): Int = 0
}
