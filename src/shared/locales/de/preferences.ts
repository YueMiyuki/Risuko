export default {
	basic: "Standard",
	advanced: "Erweitert",
	cloudSinks: "Cloud-Ziele",
	usenet: "Usenet",
	search: "Suche",
	"search-placeholder": "Alle Einstellungen durchsuchen",
	"search-all-tabs": "Alle Einstellungen",
	"search-results": "{{count}} passende Einstellungen",
	"search-no-results": "Keine passenden Einstellungen gefunden",
	"search-clear": "Suche löschen",
	"search-go-to-tab": "Einstellung öffnen",
	"search-more-results":
		"Verfeinern Sie Ihre Suche, um mehr Ergebnisse zu sehen",
	"usenet-tips":
		"NNTP-Anbieter und sichere Standardeinstellungen für die Archivbereinigung verwalten.",
	"usenet-profiles": "Anbieterprofile",
	"usenet-profiles-tips":
		"Anbieterpriorität, Sicherheit, Authentifizierung und Verbindungslimits konfigurieren.",
	"usenet-add-profile": "Profil hinzufügen",
	"usenet-no-profiles": "Keine Anbieterprofile konfiguriert.",
	"usenet-no-profiles-tips":
		"Fügen Sie mindestens einen Anbieter hinzu, bevor Sie einen NZB-Download importieren.",
	"usenet-edit-profile": "Profil bearbeiten",
	"usenet-profile-dialog-tips":
		"Profileinstellungen werden möglicherweise zwischen Geräten synchronisiert. Zugangsdaten bleiben lokal auf diesem Gerät.",
	"usenet-profile-dialog-tips-vault":
		"Profileinstellungen werden möglicherweise zwischen Geräten synchronisiert. Zugangsdaten bleiben im sicheren Schlüsselbund dieses Geräts.",
	"usenet-profile-name": "Profilname",
	"usenet-profile-name-placeholder": "Primärer Anbieter",
	"usenet-profile-host": "NNTP-Host",
	"usenet-profile-port": "Port",
	"usenet-security": "Verbindungssicherheit",
	"usenet-tls": "Implizites TLS",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "Klartext-NNTP",
	"usenet-authentication": "Authentifizierung",
	"usenet-auth-credentials": "Benutzername und Passwort",
	"usenet-auth-anonymous": "Anonym",
	"usenet-username": "Benutzername",
	"usenet-password": "Passwort",
	"usenet-credentials-saved": "Zugangsdaten gespeichert",
	"usenet-credentials-unavailable": "Status der Zugangsdaten nicht verfügbar",
	"usenet-credentials-saved-tips":
		"Zugangsdaten sind bereits gespeichert. Lassen Sie beide Felder leer, um sie unverändert zu lassen.",
	"usenet-plain-warning-title": "Unverschlüsselte Verbindung",
	"usenet-plain-opt-in":
		"Ich verstehe das und möchte Klartext-NNTP für dieses Profil zulassen.",
	"usenet-plain-warning":
		"Klartext-NNTP verschlüsselt weder Zugangsdaten noch heruntergeladene Daten. Verwenden Sie es nur, wenn Ihr Anbieter es ausdrücklich verlangt.",
	"usenet-enabled": "Aktiviert",
	"usenet-enabled-tips":
		"Deaktivierte Profile bleiben erhalten, werden aber nicht für Downloads verwendet.",
	"usenet-disabled": "Deaktiviert",
	"usenet-priority": "Priorität",
	"usenet-priority-tips":
		"Niedrigere Werte werden zuerst versucht. Gleiche Prioritäten teilen sich die Last.",
	"usenet-priority-value": "Priorität {{value}}",
	"usenet-max-connections": "Maximale Verbindungen",
	"usenet-max-connections-tips":
		"Das Verbindungslimit für dieses Anbieterprofil.",
	"usenet-connections-value": "{{value}} Verbindungen",
	"usenet-test": "Testen",
	"usenet-edit": "Profil bearbeiten",
	"usenet-remove": "Entfernen",
	"usenet-remove-title": "Anbieterprofil entfernen?",
	"usenet-remove-confirm":
		"{{name}} steht für Downloads nicht mehr zur Verfügung. Die gespeicherten Zugangsdaten werden ebenfalls von diesem Gerät entfernt.",
	"usenet-profile-saved": "Anbieterprofil gespeichert.",
	"usenet-profile-removed": "{{name}} wurde entfernt.",
	"usenet-test-success": "Mit {{name}} verbunden.",
	"usenet-test-failed": "Verbindung fehlgeschlagen: {{message}}",
	"usenet-validation-name": "Geben Sie einen Profilnamen ein.",
	"usenet-validation-host": "Geben Sie einen NNTP-Host ein.",
	"usenet-validation-port": "Der Port muss zwischen 1 und 65535 liegen.",
	"usenet-validation-plain":
		"Bestätigen Sie vor dem Speichern die Warnung zu Klartext-NNTP.",
	"usenet-validation-priority": "Die Priorität muss null oder größer sein.",
	"usenet-validation-connections":
		"Die Anzahl der Verbindungen muss zwischen 1 und 128 liegen.",
	"usenet-validation-credentials":
		"Geben Sie Benutzername und Passwort ein oder wählen Sie die anonyme Authentifizierung.",
	"usenet-validation-credential-status":
		"Der Status der Zugangsdaten ist nicht verfügbar. Versuchen Sie es erneut, bevor Sie dieses Profil ändern.",
	"usenet-cleanup": "Bereinigung nach Erfolg",
	"usenet-cleanup-tips":
		"Die Bereinigung läuft erst nach vollständigem Zusammensetzen und erfolgreichem Entpacken.",
	"usenet-cleanup-action": "Nach verifiziertem Erfolg",
	"usenet-cleanup-keep": "Alle Eingabedateien behalten",
	"usenet-cleanup-keep-tips": "Behält PAR2-Dateien und jeden Archivteil.",
	"usenet-cleanup-par2": "PAR2-Dateien löschen",
	"usenet-cleanup-par2-tips":
		"Entfernt Paritätsdateien, behält aber die heruntergeladenen Archivteile.",
	"usenet-cleanup-all": "PAR2-Dateien und Archivteile löschen",
	"usenet-cleanup-all-tips":
		"Entfernt Paritätsdateien und Archivteile, nachdem das Entpacken erfolgreich war.",
	"usenet-archive-safety": "Archivsicherheit",
	"usenet-limits-defaults":
		"Aufgabenweite Limits schützen Speicher und Verarbeitung bei Überprüfung und Entpacken.",
	"usenet-desktop-limits": "Desktop-Limits",
	"usenet-android-limits": "Android-Limits",
	"usenet-limits-summary":
		"{{entries}} Einträge · {{total}} entpackt insgesamt · {{perEntry}} pro Eintrag",
	"usenet-limits-secondary-summary":
		"Verhältnis {{ratio}}:1 · {{depth}} Verschachtelungsebenen · {{reserve}} freier Speicher als Reserve · {{hours}} aktive Stunden",
	"usenet-customize-limits": "Limits anpassen",
	"usenet-hide-limits": "Limits ausblenden",
	"usenet-limits-adjusted": "Synchronisierte Limits wurden angepasst",
	"usenet-limits-adjusted-tips":
		"Mindestens ein Wert überschritt die harte Obergrenze dieser Plattform und wurde reduziert.",
	"usenet-max-entries": "Maximale Einträge",
	"usenet-max-expanded": "Gesamtgröße nach dem Entpacken",
	"usenet-max-entry": "Größe pro Eintrag",
	"usenet-max-depth": "Verschachtelungstiefe",
	"usenet-max-ratio": "Kompressionsverhältnis",
	"usenet-free-space-reserve": "Reserve an freiem Speicher",
	"usenet-max-active-time": "Aktive Verarbeitungszeit",
	"usenet-unit-entries": "Einträge",
	"usenet-unit-levels": "Ebenen",
	"usenet-unit-hours": "Stunden",
	"usenet-limits-hard-cap":
		"Werte dürfen das Vierfache der Plattform-Standardwerte nicht überschreiten. Überschreibungen pro Aufgabe werden weiterhin separat bestätigt.",
	"usenet-restore-limits": "Plattform-Standardwerte wiederherstellen",
	"usenet-limits-restored":
		"Archivlimits auf Plattform-Standardwerte zurückgesetzt.",
	save: "Speichern & übernehmen",
	"save-success-message": "Einstellungen erfolgreich gespeichert",
	"save-fail-message": "Speichern der Einstellungen fehlgeschlagen",
	discard: "Verwerfen",
	"history-directories": "Letzte Download-Ordner",
	"favorite-directory": "Favoritenordner",
	"unfavorite-directory": "Ordner aus Favoriten entfernen",
	"remove-history-directory": "Ordner aus Verlauf entfernen",
	startup: "Startup",
	"open-at-login": "Beim Login öffnen",
	"keep-window-state":
		"Stellen Sie die Größe und Position des Fensters wieder her",
	"auto-resume-all":
		"Alle nicht abgeschlossenen Aufgaben automatisch fortsetzen",
	"default-dir": "Standardpfad",
	"file-category-dirs": "Kategoriepfade",
	"file-category-dirs-tips":
		"Den Standard-Downloadpfad für bestimmte Dateitypen überschreiben. Leer lassen, um den Standardpfad zu verwenden.",
	"task-routing-rules": "Aufgaben-Routing-Regeln",
	"task-routing-rules-tips":
		"Leitet Aufgaben anhand von Dateinamenmustern in Ordner um. Die erste passende Regel gewinnt.",
	"task-routing-rule-pattern-placeholder": "Muster (z. B. *.mkv)",
	"task-routing-rule-label-placeholder": "Tag",
	"task-routing-rule-dir-placeholder": "Ordner",
	"task-routing-rule-add": "Regel hinzufügen",
	"file-category-music": "Musik",
	"file-category-video": "Video",
	"file-category-image": "Bild",
	"file-category-document": "Dokument",
	"file-category-compressed": "Komprimiert",
	"file-category-program": "Programm",
	"mas-default-dir-tips":
		"Aufgrund der Einschränkungen durch Sandbox-Berechtigungen im App Store wird der Download Ordner als Standard empfohlen",
	"transfer-settings": "Übertragung",
	"transfer-speed-upload": "Upload-Limit",
	"transfer-speed-download": "Download-Limit",
	"transfer-speed-unlimited": "Unbegrenzt",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Magnetlink als Torrent-Datei speichern",
	"bt-create-subfolder":
		"Torrents mit mehreren Dateien in einem Unterordner zusammenfassen",
	"bt-create-subfolder-tips":
		"Wenn aktiviert, werden BitTorrent- und Magnet-Downloads mit mehreren Dateien in einem Ordner abgelegt, der nach dem Torrent benannt ist. Wenn deaktiviert, werden die Dateien direkt in den Download-Ordner geschrieben.",
	"bt-upload-rate-limit": "BitTorrent-Upload-Limit",
	"bt-upload-rate-limit-tips":
		"Begrenzt BitTorrent-Uploads zusätzlich zum globalen Upload-Limit. 0 bedeutet keine separate Begrenzung.",
	"bt-auto-download-content":
		"Laden Sie den Magneten- und Torrent-Inhalt automatisch herunter",
	"bt-force-encryption": "BT Zwangskodierung",
	"keep-seeding": "Setzen Sie die Aussaat fort, bis Sie sie manuell stoppen",
	"seed-ratio": "Seed-Verhältnis",
	"seed-time": "Seed-Zeit",
	"seed-time-unit": "Minuten",
	"task-manage": "Aufgaben verwalten",
	"max-concurrent-downloads": "Maximal aktive Aufgaben",
	"connections-per-task": "Verbindungen pro Aufgabe",
	"max-connection-per-server": "Maximale Verbindungen pro Server",
	"auto-retry": "Fehlgeschlagene Aufgaben automatisch wiederholen",
	"auto-retry-strategy": "Modus des Wiederholungsintervalls",
	"auto-retry-strategy-static": "Statisch",
	"auto-retry-strategy-exponential": "Exponentieller Backoff",
	"auto-retry-interval": "Basis-Wiederholungsintervall",
	"auto-retry-interval-unit": "Sekunden",
	"auto-retry-max-attempts": "Maximale Wiederholungsversuche (0 = unbegrenzt)",
	"worker-max-retries": "Max. Wiederholungen pro Worker-Stück",
	"auto-detect-low-speed-tasks": "Langsame Aufgaben automatisch erkennen",
	"auto-detect-low-speed-tasks-tips":
		"Bleibt eine laufende Aufgabe unter dem Geschwindigkeitsschwellenwert, wird sie automatisch pausiert und neu gestartet.",
	"low-speed-threshold": "Schwellenwert für niedrige Geschwindigkeit",
	"low-speed-threshold-unit": "KB/s",
	"http-reliability": "HTTP-/Netzwerkzuverlässigkeit",
	"http-reliability-tips":
		"Verbindungs-Timeout und Erkennung blockierter Übertragungen für HTTP/HTTPS/FTP-Downloads anpassen.",
	"connect-timeout": "Verbindungs-Timeout",
	"nzb-body-timeout": "NZB-Body-Timeout",
	"lowest-speed-limit": "Mindestgeschwindigkeit",
	"lowest-speed-limit-timeout": "Stillstands-Timeout",
	"lowest-speed-limit-help":
		"Bleibt ein Download für die Dauer des Stillstands-Timeouts unter der Mindestgeschwindigkeit, wird der Worker abgebrochen und (bei aktivierter automatischer Wiederholung) neu gestartet. Setzen Sie das Limit auf 0, um die Funktion zu deaktivieren.",
	"uri-selector": "Spiegelauswahl",
	"uri-selector-feedback": "Feedback (langsame/fehlerhafte Spiegel verwerfen)",
	"uri-selector-inorder":
		"In Reihenfolge (Spiegel von links nach rechts versuchen)",
	"uri-selector-adaptive": "Adaptiv (schnellsten bevorzugen)",
	"unit-seconds": "Sekunden",
	"unit-kib-per-sec": "KiB/s",
	storage: "Speicher",
	"storage-tips": "Wie die Engine vor dem Download Speicherplatz reserviert.",
	"file-allocation": "Dateizuweisung",
	"file-allocation-falloc": "Vorab zuweisen (falloc)",
	"file-allocation-trunc": "Abschneiden (nur Länge setzen)",
	"file-allocation-none": "Keine (bei Bedarf wachsen)",
	"file-allocation-tips":
		"Vorab zuweisen reserviert echte Datenträgerblöcke im Voraus, um ENOSPC mitten im Download zu vermeiden und die Fragmentierung zu verringern. Fällt auf Abschneiden zurück, wenn das Dateisystem dies nicht unterstützt.",
	"new-task-show-downloading":
		"Nach hinzufügen einer Aufgabe zu aktiven Downloads wechseln",
	"no-confirm-before-delete-task":
		"Vor dem Löschen der Aufgabe ist keine Bestätigung erforderlich",
	"auto-file-renaming": "Doppelte Dateien automatisch umbenennen",
	"auto-file-renaming-tips":
		'Wenn bereits eine Datei mit demselben Namen existiert, als "file.1.ext" speichern, statt sie zu überschreiben',
	"use-remote-file-time":
		"Beim Speichern die Änderungszeit der Remote-Datei verwenden",
	"use-remote-file-time-tips":
		"Setzt die Änderungszeit der Datei auf den Zeitstempel vom Remote-Server",
	continue: "HTTPS/FTP Downloads fortsetzen wenn bereits angefangen",
	"task-completed-notify":
		"Benachrichtigung nach abgeschlossenen Download anzeigen",
	"clipboard-watch": "Zwischenablage auf Download-Links überwachen",
	"clipboard-watch-tips":
		"Wenn Sie irgendwo einen Magnet- oder Dateilink kopieren, bietet Risuko an, ihn herunterzuladen.",
	"clipboard-watch-ext": "Überwachte Dateiendungen",
	"clipboard-watch-ext-tips":
		"Nur HTTP(S)-Links, die auf diese Endungen enden, lösen die Abfrage aus. Magnet-, ed2k- und Torrent-Links lösen sie immer aus. Mit Leerzeichen oder Kommas trennen.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"Risuko überwacht jetzt Ihre Zwischenablage auf Download-Links. Sie können dies unter Einstellungen → Standard deaktivieren.",
	"prevent-sleep-while-downloading":
		"Systemschlaf während des Downloads verhindern",
	"prevent-sleep-while-downloading-tips":
		"Computer wach halten, solange eine aktive Aufgabe läuft",
	"shutdown-when-complete":
		"Computer herunterfahren, wenn alle Aufgaben abgeschlossen sind",
	"shutdown-when-complete-tips":
		"Einmalig: Fährt den Computer herunter, sobald die Download-Warteschlange leer ist",
	"shutdown-countdown-title": "Herunterfahren in {{seconds}} s…",
	"shutdown-countdown-cancel": "Abbrechen",
	"shutdown-failed": "Herunterfahren fehlgeschlagen. Berechtigungen prüfen.",
	"auto-purge-record": "Download Protokoll beim Schließen der App löschen",
	"purge-record-on-start": "Download-Protokoll beim Start der App löschen",
	ui: "UI",
	appearance: "Erscheinungsbild",
	"theme-auto": "Automatisch",
	"theme-light": "Hell",
	"theme-dark": "Dunkel",
	"font-family": "Schriftart",
	"font-family-system": "Systemschrift",
	"font-family-rounded": "Abgerundet",
	"font-family-serif": "Serifenschrift",
	"font-family-mono": "Monospace",
	"font-family-sample": "Der schnelle braune Fuchs lädt schneller herunter.",
	"font-size": "Größe",
	"font-size-small": "Klein",
	"font-size-small-short": "Klein",
	"font-size-default": "Standard",
	"font-size-default-short": "Standard",
	"font-size-large": "Groß",
	"font-size-large-short": "Groß",
	"font-size-extra-large": "Sehr groß",
	"font-size-extra-large-short": "Sehr groß",
	"auto-hide-window": "Fenster automatisch ausblenden",
	"run-mode": "Ausführen als",
	"run-mode-standard": "Standardanwendung",
	"run-mode-tray": "Infobereichsanwendung",
	"run-mode-hide-tray": "Infobereichsanwendung ausblenden",
	"tray-speedometer":
		"Das Menüleistenfach zeigt die Echtzeitgeschwindigkeit an",
	"show-progress-bar": "Fortschrittsbalken anzeigen",
	language: "Sprache",
	"change-language": "Sprache ändern",
	"hide-app-menu": "App Menü ausblenden (nur auf Windows & Linux)",
	proxy: "Proxy",
	"proxy-http-profile": "HTTP-/FTP-/Usenet-Proxy",
	"proxy-p2p-profile": "P2P-Proxy",
	"enable-proxy": "Proxy aktivieren",
	"enable-p2p-proxy": "P2P-Proxy aktivieren",
	"proxy-p2p-tcp-server": "TCP-Proxyserver",
	"proxy-p2p-tcp-bypass": "TCP-Proxy-Ausnahmen",
	"proxy-p2p-udp-server": "UDP-Proxyserver",
	"proxy-p2p-udp-server-placeholder":
		"Optional; leer lassen, um den TCP-P2P-Proxy zu verwenden",
	"proxy-p2p-udp-bypass": "UDP-Proxy-Ausnahmen",
	"proxy-bypass-input-tips":
		"Proxy-Einstellungen für diese Hosts und Domänen umgehen, eine pro Zeile",
	"proxy-scope-label": "Verwendet für",
	"proxy-scope-download": "Herunterladen",
	"proxy-scope-download-desc": "Download-Datenverkehr über den Proxy leiten",
	"proxy-scope-update-app": "Anwendung aktualisieren",
	"proxy-scope-update-app-desc": "Nach App-Updates über den Proxy suchen",
	"proxy-scope-update-trackers": "Tracker aktualisieren",
	"proxy-scope-update-trackers-desc": "BT-Trackerlisten über den Proxy abrufen",
	"proxy-tips": "Proxy-Handbuch anzeigen",
	"proxy-p2p-tips":
		"Für DHT, UDP-Tracker und uTP ist SOCKS5 oder SOCKS5H erforderlich. HTTP-Proxys werden nur für TCP-P2P-Verbindungen verwendet.",
	"proxy-p2p-udp-tips":
		"Lassen Sie den UDP-Proxy leer, um die TCP-P2P-Route wiederzuverwenden. UDP-Proxying erfordert SOCKS5 oder SOCKS5H; HTTP-Proxys können kein UDP übertragen.",
	"proxy-p2p-restart-title": "P2P-Downloads neu starten?",
	"proxy-p2p-restart-confirm":
		"Eine Änderung des P2P-Proxys startet aktive Peer-to-Peer-Downloads neu. HTTP-, FTP- und Usenet-Downloads laufen weiter.",
	doh: "DNS über HTTPS",
	"doh-tips":
		"Hostnamen über einen verschlüsselten DNS-Endpunkt statt über den System-Resolver auflösen",
	"doh-enable": "DNS über HTTPS aktivieren",
	"doh-provider": "Anbieter",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Benutzerdefiniert",
	"doh-url": "Endpunkt-URL",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "Der DoH-Endpunkt muss mit https:// beginnen",
	"doh-bootstrap": "Bootstrap-IPs",
	"doh-bootstrap-placeholder":
		"IPs zum Erreichen des Endpunkts, durch Komma getrennt (optional)",
	"doh-fallback": "Auf System-DNS zurückfallen",
	"doh-fallback-tips":
		"Schlägt eine DoH-Abfrage fehl, über den System-Resolver auflösen, statt den Download scheitern zu lassen",
	"bt-tracker": "Tracker-Server",
	"bt-tracker-input-tips": "Tracker-Server, einer pro Zeile",
	"bt-tracker-tips": "Empfohlen:",
	"bt-max-peers-per-torrent": "Max. Peers pro Torrent",
	"bt-max-peers-per-torrent-tips":
		"Maximale Anzahl gleichzeitiger Peer-Verbindungen pro Torrent. Höhere Werte erleichtern das Auffinden von Peers im Schwarm (Engine-Neustart erforderlich)",
	"bt-max-outstanding-per-peer": "Max. ausstehende Anfragen pro Peer",
	"bt-max-outstanding-per-peer-tips":
		"Maximale Anzahl gleichzeitiger Chunk-Anfragen, die pro Peer in der Pipeline stehen (Obergrenze, bis zu 500). 0 verwendet den adaptiven Standard von 6–500; ein torrentweites Anfragebudget wird auf die aktiven Peers verteilt. Engine-Neustart erforderlich",
	"bt-max-connections": "Max. Peer-Verbindungen",
	"bt-max-connections-tips":
		"Gesamtzahl der Peer-Verbindungen über alle Torrents, 20 bis 5000. Eine Verringerung trennt nie bereits offene Verbindungen, sondern hält nur neue zurück. Unter Linux, macOS und Android ist der wirksame Wert außerdem auf die Hälfte des Limits für Dateideskriptoren des Systems begrenzt",
	"bt-ban-corrupt-peers": "Peers sperren, die fehlerhafte Daten senden",
	"bt-ban-corrupt-peers-tips":
		"Eine IP-Adresse für den Rest der Sitzung sperren, sobald von ihr mitgesendete Stücke die Hash-Prüfung wiederholt nicht bestehen. Viele Nutzer können sich eine IP teilen (Mobilfunkanbieter, gemeinsame Netzwerke), daher kann eine Sperre auch unbeteiligte Peers ausschließen (Engine-Neustart erforderlich)",
	"bt-ban-corrupt-strikes": "Fehlgeschlagene Stücke vor der Sperre",
	"bt-ban-corrupt-strikes-tips":
		"Wie viele fehlgeschlagene Stücke eine IP-Adresse verursachen darf, bevor sie gesperrt wird. Höhere Werte tolerieren gelegentliche Datenfehler (Engine-Neustart erforderlich)",
	"bt-enable-upnp": "UPnP- / NAT-PMP-Portweiterleitung",
	"bt-enable-upnp-tips":
		"Den BitTorrent-Listen-Port automatisch per UPnP IGD, NAT-PMP oder PCP am Router öffnen (Engine-Neustart erforderlich)",
	"bt-upnp-lease": "Dauer der Portzuordnung (Sekunden)",
	"bt-upnp-lease-tips":
		"Gültigkeitsdauer für UPnP- und NAT-PMP-Portzuordnungen. Kürzere Zeiten erholen sich schneller von veralteten Zuordnungen (Engine-Neustart erforderlich)",
	"bt-enable-lsd": "Local Service Discovery",
	"bt-enable-lsd-tips":
		"Peers im lokalen Netzwerk per BEP-14-Multicast finden (Engine-Neustart erforderlich)",
	"bt-encryption-policy": "Protokollverschlüsselung",
	"bt-encryption-policy-tips":
		"MSE/PE-Richtlinie für Peer-Verbindungen. 'Verschlüsselung bevorzugen' versucht zuerst die Verschlüsselung und fällt auf Klartext zurück (Engine-Neustart erforderlich)",
	"bt-encryption-plaintext": "Nur Klartext",
	"bt-encryption-prefer": "Verschlüsselung bevorzugen",
	"bt-encryption-require": "Verschlüsselung erzwingen",
	"bt-listen-v6": "Auf IPv6 lauschen",
	"bt-listen-v6-tips":
		"Einen zusätzlichen IPv6-TCP-Listener am selben Port binden (Engine-Neustart erforderlich)",
	"sync-tracker-tips": "Synchronisieren",
	"auto-sync-tracker":
		"Aktualisieren Sie die Trackerliste jeden Tag automatisch",
	"ed2k-server": "eDonkey-Server",
	"ed2k-server-input-tips": "eDonkey-Server, einer pro Zeile (ip:port)",
	"ed2k-server-tips": "Wenn leer, werden die Standardserver verwendet",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Quellensuche über Kad aktivieren",
	"ed2k-enable-kad-tips":
		"ED2K-Quellen über das eMule Kad-Netzwerk finden (Engine-Neustart erforderlich)",
	"ed2k-kad-port": "Kad-UDP-Port",
	"ed2k-kad-port-tips":
		"UDP-Port für die Suche über Kad. Gültige Werte liegen zwischen 1 und 65535 (Engine-Neustart erforderlich)",
	"ed2k-kad-port-invalid":
		"Der Kad-UDP-Port muss eine Ganzzahl zwischen 1 und 65535 sein",
	"listen-port-invalid":
		"Der BT-Port muss ein Port von 1 bis 65535, ein Bereich wie 6881-6889 oder eine durch Kommas getrennte Liste davon sein",
	"dht-listen-port-invalid":
		"Der DHT-Port muss ein Port von 1 bis 65535, ein Bereich wie 6881-6889 oder eine durch Kommas getrennte Liste davon sein",
	"ed2k-port-invalid": "Der ED2K-Port muss eine Ganzzahl von 1 bis 65535 sein",
	"ed2k-enable-upload": "Dateien über ED2K teilen",
	"ed2k-enable-upload-tips":
		"Fertige Dateien und geprüfte Teile laufender ED2K-Downloads an andere Peers hochladen. Kad kann eine nicht mehr geteilte Datei noch bis zu 5 Stunden auflisten (Engine-Neustart erforderlich)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Standard-Zugangsdaten für FTP-, FTPS- und SFTP-Downloads",
	"ftp-username": "FTP-Benutzername",
	"ftp-password": "FTP-Passwort",
	"sftp-private-key": "Pfad zum privaten SSH-Schlüssel",
	"sftp-private-key-content": "Inhalt des privaten SSH-Schlüssels",
	"sftp-private-key-tips":
		"Pfad zur privaten PEM-Schlüsseldatei, z. B. ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips":
		"Inhalt des privaten PEM-Schlüssels hier einfügen",
	"sftp-key-passphrase": "Schlüssel-Passphrase",
	"sftp-key-passphrase-tips":
		"Leer lassen, wenn der Schlüssel keine Passphrase hat",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Ausgabeformat",
	"media-settings": "Medienseiten (yt-dlp)",
	"media-settings-tips":
		"Standardeinstellungen für Downloads von Medienseiten (YouTube, Vimeo, Bilibili und mehr). Erfordert yt-dlp in PATH; ffmpeg wird benötigt, um getrennte Video- und Audiostreams zusammenzuführen.",
	"media-format": "Bevorzugtes Format",
	"media-format-placeholder":
		"z. B. bestvideo+bestaudio/best (yt-dlp-Formatstring)",
	"media-format-tips":
		"yt-dlp-Formatauswahl. Leer lassen für beste Qualität. Beispiele: bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "Listen Ports",
	"bt-port": "BT Listen Port",
	"dht-port": "DHT Listen Port",
	"ed2k-port": "ed2k-Listen-Port",
	security: "Sicherheit",
	rpc: "RPC",
	"rpc-host": "Serveradresse",
	"rpc-host-local": "Nur dieses Gerät",
	"rpc-host-all": "Alle Netzwerkschnittstellen",
	"rpc-host-custom": "Benutzerdefiniert ({{host}})",
	"rpc-host-exposed-tips":
		"Andere Geräte in Ihrem Netzwerk können den RPC-Port und, sofern aktiviert, den PeerBanHelper-Port erreichen. Jeder, der das Geheimnis kennt, kann Downloads hinzufügen und steuern.",
	"rpc-listen-port": "RPC-Hörport",
	"rpc-secret": "RPC-Geheimnis",
	"rpc-secret-tips": "Geheime RPC-Anleitung anzeigen",
	"rpc-allow-any-dir": "RPC-Clients beliebige lokale Pfade verwenden lassen",
	"rpc-allow-any-dir-tips":
		"Wenn deaktiviert, können über RPC hinzugefügte Downloads nur im Download-Ordner, in dessen Unterordnern sowie in Ihren Kategorie- und Routing-Ordnern gespeichert werden, und RPC-Clients können die App nicht dazu bringen, eine Cookie-, netrc- oder SSH-Schlüsseldatei zu lesen.",
	pbh: "PeerBanHelper",
	"pbh-enable": "PeerBanHelper-API aktivieren",
	"pbh-enable-tips":
		"Stellt einen Aria2Next-kompatiblen JSON-RPC-Listener bereit, damit PeerBanHelper BitTorrent-Peers überwachen und sperren kann",
	"pbh-listen-port": "PeerBanHelper-Lauschport",
	"pbh-rpc-secret": "PeerBanHelper-RPC-Token",
	"generate-pbh-rpc-secret": "Neues Token erzeugen",
	"pbh-endpoint-hint":
		"Fügen Sie in PeerBanHelper einen Aria2Next-Downloader hinzu, der auf http://127.0.0.1:{{port}}/jsonrpc zeigt",
	"randomize-port": "Zufälligen Port wählen",
	"generate-rpc-secret": "Neues RPC-Geheimnis erzeugen",
	"copy-rpc-url": "RPC-URL kopieren",
	"engine-overrides": "Engine-Überschreibungen (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Erweitert: Beliebige Engine-Optionsschlüssel als JSON-Objekt setzen. Änderungen werden nach einem Neustart wirksam.",
	"engine-overrides-invalid":
		"Engine-Überschreibungen müssen ein gültiges JSON-Objekt sein.",
	"engine-overrides-reserved-keys":
		"Reservierte Schlüssel für Engine-Überschreibungen wurden ignoriert: {{keys}}",
	"engine-overrides-too-large":
		"Die Engine-Überschreibungen sind zu groß: höchstens 64 Schlüssel, 64 Zeichen pro Schlüssel und 2048 Zeichen pro Wert.",
	developer: "Entwickler",
	"user-agent": "User-Agent",
	"mock-user-agent": "User-Agent simulieren",
	cookies: "Cookies",
	"cookies-tips":
		"Cookies aus einer cookies.txt-Datei im Netscape-/Mozilla-Format laden (akzeptiert auch curl-/wget-Ausgabe).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Gespeicherte Domain-Zugangsdaten",
	"saved-cookies-tips":
		"Aus einem Browser importierte Cookies und User-Agent, um Cloudflare oder andere authentifizierungsgeschützte Downloads zu umgehen. Risuko verwendet sie für neue Downloads derselben Domain erneut, bis sie ablaufen oder nicht mehr funktionieren.",
	"saved-cookies-empty": "Noch keine gespeicherten Zugangsdaten.",
	"saved-cookies-host": "Domain",
	"saved-cookies-browser": "Browser",
	"saved-cookies-count": "Cookies",
	"saved-cookies-imported": "Importiert",
	"saved-cookies-actions": "Aktionen",
	"saved-cookies-clear": "Alle löschen",
	"saved-cookies-confirm-clear":
		"Alle gespeicherten Domain-Zugangsdaten löschen? Künftige Downloads von diesen Websites schlagen möglicherweise fehl, bis Sie sie erneut importieren.",
	"saved-cookies-delete": "Löschen",
	netrc: ".netrc-Zugangsdaten",
	"netrc-tips":
		"HTTP-Basic-Authentifizierung aus einer .netrc-Datei einfügen, wenn die URL keine eingebetteten Zugangsdaten enthält. Standard: ~/.netrc (unter Windows %USERPROFILE%/_netrc).",
	"netrc-path-placeholder": ".netrc-Pfad überschreiben (optional)",
	"no-netrc": ".netrc-Suche deaktivieren",
	"no-netrc-tips":
		".netrc vollständig überspringen. Nützlich, wenn die Datei Zugangsdaten enthält, die nicht für Downloads gedacht sind.",
	"app-log-path": "Appprotokollpfad",
	"log-dir-override-placeholder":
		"Benutzerdefinierter Protokollordner (leer lassen für Standard)",
	"log-dir-override-tips":
		"Protokolle werden derzeit nach {{path}} geschrieben. Ein benutzerdefinierter Ordner (z. B. in Downloads) macht sie in jedem Dateimanager sichtbar. Änderungen werden nach dem nächsten Neustart der App wirksam.",
	"download-session-path": "Downloadsitzungspfad",
	"session-reset": "Download-Session zurücksetzen",
	"session-reset-confirm":
		"Sind Sie sicher, dass Sie die Download-Session zurücksetzen wollen?",
	"factory-reset": "Werkseinstellungen",
	"factory-reset-confirm":
		"Sollen die Einstellungen auf die Werkseinstellungen unwiderruflich zurückgesetzt werden?",
	"download-protocol": "Protokoll",
	"protocols-default-client":
		"Als Standardclient für die folgenden Protokolle festlegen",
	"protocols-magnet": "Magnet [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc:// adcs:// dchub:// nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella://, gnet:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "giFT-Daemon-Integration",
	"gift-enabled": "giFT-Delegation aktivieren",
	"gift-host": "giFT-Host",
	"gift-port": "giFT-Port",
	"gift-not-detected":
		"giFT-Daemon wurde unter dem konfigurierten Host:Port nicht erkannt.",
	"browser-extensions": "Erweiterungen",
	"baidu-exporter": "Baidu Exporter",
	"browser-extensions-tips": "Von der Community bereitgestellt, ",
	"baidu-exporter-help": "mehr über die Verwendung zu erfahren",
	"auto-update": "Auto-Update",
	"auto-check-update": "Automatisch auf Updates prüfen",
	"last-check-update-time": "Letzte Update-Prüfung",
	"completion-script": "Abschluss-Skript",
	"completion-script-enabled": "Skript nach Abschluss des Downloads ausführen",
	"completion-script-tips":
		"In einer Sandbox über tauri-plugin-shell ausgeführt. Läuft einmal pro Aufgabenende (Erfolg oder Fehler).",
	"completion-script-command": "Befehl",
	"completion-script-command-placeholder":
		"z. B. /usr/local/bin/notify.sh oder C:\\Tools\\notify.exe",
	"completion-script-args": "Argumente",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Platzhalter: {path}, {hash}, {status}. Zusätzlich als Umgebungsvariablen RISUKO_PATH, RISUKO_HASH, RISUKO_STATUS verfügbar.",
	"completion-script-timeout": "Zeitlimit (ms)",
	"completion-script-test": "Skript testen",
	"completion-script-test-ok": "Skript-Test erfolgreich",
	"completion-script-test-fail": "Skript-Test fehlgeschlagen",
	"completion-script-empty": "Bitte zuerst einen Befehl angeben",
	"completion-script-not-approved":
		"Das Abschluss-Skript wurde nicht zugelassen, daher wurde nichts gespeichert.",
	"not-saved": "Einstellungen nicht gespeichert",
	"not-saved-confirm":
		"Die geänderten Einstellungen gehen verloren. Möchten Sie wirklich gehen?",
	"saved-credentials": "Gespeicherte Zugangsdaten",
	"saved-credentials-description":
		"Gespeicherte Authentifizierungsdaten für Downloads verwalten",
	"no-saved-credentials": "Keine gespeicherten Zugangsdaten",
	"credential-host": "Host",
	"credential-label": "Bezeichnung",
	"credential-protocol": "Protokoll",
	"credential-last-used": "Zuletzt verwendet",
	"credential-delete": "Zugangsdaten löschen",
	"credential-delete-confirm":
		"Möchten Sie diese gespeicherten Zugangsdaten wirklich löschen?",
	"vault-enabled":
		"Sensible Daten im Schlüsselbund des Betriebssystems gespeichert",
	"vault-disabled":
		"Schlüsselbund des Betriebssystems nicht verfügbar – sensible Daten werden im Klartext gespeichert",
	"credential-encrypted-badge":
		"Im Schlüsselbund des Betriebssystems gespeichert",
	"credential-vault-unavailable":
		"Schlüsselbund des Betriebssystems nicht erreichbar; neue Zugangsdaten werden im Klartext gespeichert",
	"speed-limit-enabled": "Geschwindigkeitslimits aktivieren",
	"speed-limit-enabled-description":
		"Die unten stehenden Limits anwenden. Wenn deaktiviert, laufen Downloads mit voller Geschwindigkeit; die Werte bleiben erhalten.",
	"appearance-theme": "Design",
	"appearance-typography": "Typografie",
	"appearance-window": "Fenster",
	"task-list-style": "Aufgabenliste",
	"task-list-style-description":
		"Wie Downloads in der Aufgabenliste angezeigt werden.",
	"task-list-style-compact": "Kompakt",
	"task-list-style-card": "Karten",
	"sidebar-collapsed": "Seitenleiste einklappen",
	"sidebar-collapsed-description": "Nur Symbole in der Seitenleiste anzeigen.",
};
