package app.risuko.mobile

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat

class RisukoForegroundService : Service() {
  override fun onBind(intent: Intent?): IBinder? = null

  override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
    ensureChannel(this)
    val progress = intent?.getIntExtra(EXTRA_PROGRESS, 0)?.coerceIn(0, 100) ?: 0
    val activeCount = intent?.getIntExtra(EXTRA_ACTIVE_COUNT, 0)?.coerceAtLeast(0) ?: 0
    val detail = intent?.getStringExtra(EXTRA_DETAIL).orEmpty()

    if (activeCount <= 0) {
      stopForeground(STOP_FOREGROUND_REMOVE)
      stopSelf()
      return START_NOT_STICKY
    }

    try {
      startForeground(NOTIFICATION_ID, buildNotification(this, progress, activeCount, detail))
    } catch (e: IllegalStateException) {
      Log.w(TAG, "foreground service could not start", e)
      stopSelf()
      return START_NOT_STICKY
    }
    return START_STICKY
  }

  override fun onTimeout(startId: Int, fgsType: Int) {
    Log.w(TAG, "foreground service time limit reached")
    stopForeground(STOP_FOREGROUND_REMOVE)
    stopSelf()
  }

  companion object {
    private const val TAG = "RisukoService"
    private const val CHANNEL_ID = "risuko_downloads"
    private const val CHANNEL_NAME = "Downloads"
    private const val NOTIFICATION_ID = 4737
    private const val EXTRA_PROGRESS = "progress"
    private const val EXTRA_ACTIVE_COUNT = "activeCount"
    private const val EXTRA_DETAIL = "detail"

    fun show(context: Context, progress: Int, activeCount: Int, detail: String) {
      val intent = Intent(context, RisukoForegroundService::class.java).apply {
        putExtra(EXTRA_PROGRESS, progress.coerceIn(0, 100))
        putExtra(EXTRA_ACTIVE_COUNT, activeCount.coerceAtLeast(0))
        putExtra(EXTRA_DETAIL, detail)
      }
      try {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
          context.startForegroundService(intent)
        } else {
          context.startService(intent)
        }
      } catch (e: IllegalStateException) {
        Log.w(TAG, "foreground service could not start", e)
      }
    }

    fun hide(context: Context) {
      context.stopService(Intent(context, RisukoForegroundService::class.java))
    }

    private fun ensureChannel(context: Context) {
      if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
        return
      }
      val manager = context.getSystemService(NotificationManager::class.java)
      val existing = manager.getNotificationChannel(CHANNEL_ID)
      if (existing != null) {
        return
      }
      val channel = NotificationChannel(
        CHANNEL_ID,
        CHANNEL_NAME,
        NotificationManager.IMPORTANCE_LOW,
      ).apply {
        setShowBadge(false)
      }
      manager.createNotificationChannel(channel)
    }

    private fun buildNotification(
      context: Context,
      progress: Int,
      activeCount: Int,
      detail: String,
    ): Notification {
      val launchIntent = context.packageManager.getLaunchIntentForPackage(context.packageName)
        ?: Intent(context, MainActivity::class.java)
      val pendingIntent = PendingIntent.getActivity(
        context,
        0,
        launchIntent,
        PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
      )
      val title = if (activeCount == 1) {
        "1 active download"
      } else {
        "$activeCount active downloads"
      }
      val text = detail.ifBlank { "$progress% complete" }

      return NotificationCompat.Builder(context, CHANNEL_ID)
        .setSmallIcon(R.drawable.ic_notification)
        .setContentTitle(title)
        .setContentText(text)
        .setContentIntent(pendingIntent)
        .setOngoing(true)
        .setOnlyAlertOnce(true)
        .setShowWhen(false)
        .setProgress(100, progress.coerceIn(0, 100), false)
        .setPriority(NotificationCompat.PRIORITY_LOW)
        .build()
    }
  }
}
