package app.risuko.webview_upgrade.sandbox

import android.app.Service
import android.content.Intent
import android.os.IBinder

class StubSandboxedProcessService2 : Service() {
    private val delegate = SandboxedProcessServiceDelegate()
    override fun onCreate() {
        super.onCreate()
        delegate.onCreate(this, applicationContext)
    }
    override fun onBind(intent: Intent): IBinder? = delegate.onBind(intent)
    override fun onRebind(intent: Intent) {
        super.onRebind(intent)
        delegate.onRebind(intent)
    }
    override fun onUnbind(intent: Intent): Boolean {
        val result = super.onUnbind(intent)
        return delegate.onUnbind(intent) || result
    }
    override fun onDestroy() {
        super.onDestroy()
        delegate.onDestroy()
    }
}
