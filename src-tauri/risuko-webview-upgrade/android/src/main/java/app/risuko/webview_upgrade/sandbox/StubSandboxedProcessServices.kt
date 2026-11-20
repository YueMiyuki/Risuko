package app.risuko.webview_upgrade.sandbox

import android.app.Service
import android.content.Intent
import android.os.IBinder

abstract class SandboxedProcessServiceBase : Service() {
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

class StubSandboxedProcessService0 : SandboxedProcessServiceBase()

class StubSandboxedProcessService1 : SandboxedProcessServiceBase()

class StubSandboxedProcessService2 : SandboxedProcessServiceBase()

class StubSandboxedProcessService3 : SandboxedProcessServiceBase()

class StubSandboxedProcessService4 : SandboxedProcessServiceBase()
