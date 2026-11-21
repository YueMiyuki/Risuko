export default {
	basic: "Base",
	advanced: "Avanzate",
	cloudSinks: "Destinazioni cloud",
	usenet: "Usenet",
	search: "Cerca",
	"search-placeholder": "Cerca in tutte le preferenze",
	"search-all-tabs": "Tutte le preferenze",
	"search-results": "{{count}} impostazioni corrispondenti",
	"search-no-results": "Nessuna impostazione corrispondente trovata",
	"search-clear": "Cancella ricerca",
	"search-go-to-tab": "Apri impostazione",
	"search-more-results": "Affina la ricerca per vedere altri risultati",
	"usenet-tips":
		"Gestisci i provider NNTP e le impostazioni predefinite per la pulizia sicura degli archivi.",
	"usenet-profiles": "Profili provider",
	"usenet-profiles-tips":
		"Configura priorità, sicurezza, autenticazione e limiti di connessione del provider.",
	"usenet-add-profile": "Aggiungi profilo",
	"usenet-no-profiles": "Nessun profilo provider configurato.",
	"usenet-no-profiles-tips":
		"Aggiungi almeno un provider prima di importare un download NZB.",
	"usenet-edit-profile": "Modifica profilo",
	"usenet-profile-dialog-tips":
		"Le impostazioni del profilo possono essere sincronizzate tra i dispositivi. Le credenziali restano locali su questo dispositivo.",
	"usenet-profile-dialog-tips-vault":
		"Le impostazioni del profilo possono essere sincronizzate tra i dispositivi. Le credenziali restano nel portachiavi sicuro di questo dispositivo.",
	"usenet-profile-name": "Nome del profilo",
	"usenet-profile-name-placeholder": "Provider principale",
	"usenet-profile-host": "Host NNTP",
	"usenet-profile-port": "Porta",
	"usenet-security": "Sicurezza della connessione",
	"usenet-tls": "TLS implicito",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "NNTP in chiaro",
	"usenet-authentication": "Autenticazione",
	"usenet-auth-credentials": "Nome utente e password",
	"usenet-auth-anonymous": "Anonima",
	"usenet-username": "Nome utente",
	"usenet-password": "Password",
	"usenet-credentials-saved": "Credenziali salvate",
	"usenet-credentials-unavailable": "Stato delle credenziali non disponibile",
	"usenet-credentials-saved-tips":
		"Le credenziali sono già salvate. Lascia vuoti entrambi i campi per mantenerle invariate.",
	"usenet-plain-warning-title": "Connessione non cifrata",
	"usenet-plain-opt-in":
		"Ho capito e voglio consentire NNTP in chiaro per questo profilo.",
	"usenet-plain-warning":
		"NNTP in chiaro non cifra le credenziali né i dati scaricati. Usalo solo quando il provider lo richiede esplicitamente.",
	"usenet-enabled": "Attivo",
	"usenet-enabled-tips":
		"I profili disattivati vengono conservati ma non usati per i download.",
	"usenet-disabled": "Disattivato",
	"usenet-priority": "Priorità",
	"usenet-priority-tips":
		"I valori più bassi vengono provati per primi. Le priorità uguali si dividono il carico.",
	"usenet-priority-value": "Priorità {{value}}",
	"usenet-max-connections": "Connessioni massime",
	"usenet-max-connections-tips":
		"Il limite di connessioni per questo profilo provider.",
	"usenet-connections-value": "{{value}} connessioni",
	"usenet-test": "Test",
	"usenet-edit": "Modifica profilo",
	"usenet-remove": "Rimuovi",
	"usenet-remove-title": "Rimuovere il profilo provider?",
	"usenet-remove-confirm":
		"{{name}} non sarà più disponibile per i download. Anche le sue credenziali salvate verranno rimosse da questo dispositivo.",
	"usenet-profile-saved": "Profilo provider salvato.",
	"usenet-profile-removed": "{{name}} è stato rimosso.",
	"usenet-test-success": "Connesso a {{name}}.",
	"usenet-test-failed": "Connessione non riuscita: {{message}}",
	"usenet-validation-name": "Inserisci un nome per il profilo.",
	"usenet-validation-host": "Inserisci un host NNTP.",
	"usenet-validation-port": "La porta deve essere compresa tra 1 e 65535.",
	"usenet-validation-plain":
		"Conferma l'avviso su NNTP in chiaro prima di salvare.",
	"usenet-validation-priority": "La priorità deve essere zero o maggiore.",
	"usenet-validation-connections":
		"Le connessioni devono essere comprese tra 1 e 128.",
	"usenet-validation-credentials":
		"Inserisci sia il nome utente sia la password, oppure scegli l'autenticazione anonima.",
	"usenet-validation-credential-status":
		"Lo stato delle credenziali non è disponibile. Riprova prima di modificare questo profilo.",
	"usenet-cleanup": "Pulizia dopo il successo",
	"usenet-cleanup-tips":
		"La pulizia viene eseguita solo dopo l'assemblaggio completo e l'estrazione riuscita.",
	"usenet-cleanup-action": "Dopo il successo verificato",
	"usenet-cleanup-keep": "Mantieni tutti i file di input",
	"usenet-cleanup-keep-tips":
		"Conserva i file PAR2 e ogni volume dell'archivio.",
	"usenet-cleanup-par2": "Elimina i file PAR2",
	"usenet-cleanup-par2-tips":
		"Rimuove i file di parità ma conserva i volumi dell'archivio scaricati.",
	"usenet-cleanup-all": "Elimina PAR2 e volumi dell'archivio",
	"usenet-cleanup-all-tips":
		"Rimuove i file di parità e i volumi dell'archivio dopo l'estrazione riuscita.",
	"usenet-archive-safety": "Sicurezza degli archivi",
	"usenet-limits-defaults":
		"I limiti per attività proteggono archiviazione ed elaborazione durante la verifica e l'estrazione.",
	"usenet-desktop-limits": "Limiti desktop",
	"usenet-android-limits": "Limiti Android",
	"usenet-limits-summary":
		"{{entries}} voci · {{total}} di espansione totale · {{perEntry}} per voce",
	"usenet-limits-secondary-summary":
		"rapporto {{ratio}}:1 · {{depth}} livelli annidati · {{reserve}} di spazio libero riservato · {{hours}} ore attive",
	"usenet-customize-limits": "Personalizza i limiti",
	"usenet-hide-limits": "Nascondi i limiti",
	"usenet-limits-adjusted": "I limiti sincronizzati sono stati modificati",
	"usenet-limits-adjusted-tips":
		"Uno o più valori superavano il limite massimo di questa piattaforma e sono stati ridotti.",
	"usenet-max-entries": "Voci massime",
	"usenet-max-expanded": "Dimensione totale espansa",
	"usenet-max-entry": "Dimensione della singola voce",
	"usenet-max-depth": "Profondità di annidamento",
	"usenet-max-ratio": "Rapporto di compressione",
	"usenet-free-space-reserve": "Spazio libero riservato",
	"usenet-max-active-time": "Tempo di elaborazione attivo",
	"usenet-unit-entries": "voci",
	"usenet-unit-levels": "livelli",
	"usenet-unit-hours": "ore",
	"usenet-limits-hard-cap":
		"I valori non possono superare il quadruplo dei valori predefiniti della piattaforma. Le sostituzioni per singola attività restano confermate separatamente.",
	"usenet-restore-limits": "Ripristina i valori predefiniti della piattaforma",
	"usenet-limits-restored":
		"Limiti degli archivi ripristinati ai valori predefiniti della piattaforma.",
	save: "Salva e applica",
	"save-success-message": "Preferenze salvate con successo",
	"save-fail-message": "Preferenze non salvate",
	discard: "Scarta modifiche",
	"history-directories": "Directory di download recenti",
	"favorite-directory": "Directory preferita",
	"unfavorite-directory": "Rimuovi directory dai preferiti",
	"remove-history-directory": "Rimuovi directory dalla cronologia",
	startup: "Avvio",
	"open-at-login": "Apri al login",
	"keep-window-state":
		"Mantieni le dimensioni e la posizione della finestra quando l'app viene chiusa",
	"auto-resume-all":
		"Ricomincia tutte le attività non finite alla riapertura dell'app",
	"default-dir": "Posizione di default per i download",
	"file-category-dirs": "Percorsi per categoria",
	"file-category-dirs-tips":
		"Sostituisci il percorso di download predefinito per tipi di file specifici. Lascia vuoto per usare il percorso predefinito.",
	"task-routing-rules": "Regole di instradamento delle attività",
	"task-routing-rules-tips":
		"Instrada le attività verso directory in base ai criteri del nome file. Vince la prima regola corrispondente.",
	"task-routing-rule-pattern-placeholder": "Criterio (es. *.mkv)",
	"task-routing-rule-label-placeholder": "Tag",
	"task-routing-rule-dir-placeholder": "Directory",
	"task-routing-rule-add": "Aggiungi regola",
	"file-category-music": "Musica",
	"file-category-video": "Video",
	"file-category-image": "Immagine",
	"file-category-document": "Documento",
	"file-category-compressed": "Compressi",
	"file-category-program": "Programma",
	"mas-default-dir-tips":
		"A causa delle restrizioni imposte dall'App Store, è raccomandato impostare la directory di default su ~/Downloads",
	"transfer-settings": "Trasmissione dati",
	"transfer-speed-upload": "Limite di upload",
	"transfer-speed-download": "Limite di download",
	"transfer-speed-unlimited": "Illimitata",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Salva magnet link come file torrent",
	"bt-create-subfolder": "Raggruppa i torrent multi-file in una sottocartella",
	"bt-create-subfolder-tips":
		"Se attivo, i download BitTorrent e magnet con più file vengono collocati in una cartella con il nome del torrent. Se disattivo, i file vengono scritti direttamente nella directory di download.",
	"bt-upload-rate-limit": "Limite di upload BitTorrent",
	"bt-upload-rate-limit-tips":
		"Limita gli upload BitTorrent in aggiunta al limite di upload globale. 0 significa nessun limite separato.",
	"bt-auto-download-content":
		"Scarica automaticamente il contenuto di magnete e torrent",
	"bt-force-encryption": "Forzare la crittografia di BT",
	"keep-seeding": "Continua a seminare fino a interromperlo manualmente",
	"seed-ratio": "Rapporto di semina",
	"seed-time": "Tempo di semi",
	"seed-time-unit": "minuti",
	"task-manage": "Gestione attività",
	"max-concurrent-downloads":
		"Massimo numero di attività eseguibili contemporaneamente",
	"connections-per-task": "Connessioni per attività",
	"max-connection-per-server":
		"Massimo numero di connessioni simultanee per server",
	"auto-retry": "Riprova automaticamente le attività fallite",
	"auto-retry-strategy": "Modalità dell'intervallo tra i tentativi",
	"auto-retry-strategy-static": "Fisso",
	"auto-retry-strategy-exponential": "Backoff esponenziale",
	"auto-retry-interval": "Intervallo base tra i tentativi",
	"auto-retry-interval-unit": "secondi",
	"auto-retry-max-attempts": "Numero massimo di tentativi (0 = illimitati)",
	"worker-max-retries": "Tentativi massimi per pezzo del worker",
	"auto-detect-low-speed-tasks": "Rileva automaticamente le attività lente",
	"auto-detect-low-speed-tasks-tips":
		"Se un'attività in esecuzione resta sotto la soglia di velocità, viene messa in pausa e riavviata automaticamente.",
	"low-speed-threshold": "Soglia di bassa velocità",
	"low-speed-threshold-unit": "KB/s",
	"http-reliability": "Affidabilità HTTP / rete",
	"http-reliability-tips":
		"Regola il timeout di connessione e il rilevamento dei trasferimenti bloccati per i download HTTP/HTTPS/FTP.",
	"connect-timeout": "Timeout di connessione",
	"nzb-body-timeout": "Timeout del corpo NZB",
	"lowest-speed-limit": "Limite minimo di velocità",
	"lowest-speed-limit-timeout": "Timeout di stallo",
	"lowest-speed-limit-help":
		"Se un download resta sotto il limite minimo di velocità per la durata del timeout di stallo, il worker viene interrotto e (se i tentativi automatici sono attivi) riavviato. Imposta il limite a 0 per disattivare.",
	"uri-selector": "Selezione dei mirror",
	"uri-selector-feedback": "Feedback (scarta i mirror lenti o difettosi)",
	"uri-selector-inorder": "In ordine (prova i mirror da sinistra a destra)",
	"uri-selector-adaptive": "Adattiva (preferisce il più veloce)",
	"unit-seconds": "secondi",
	"unit-kib-per-sec": "KiB/s",
	storage: "Archiviazione",
	"storage-tips": "Come il motore riserva spazio su disco prima del download.",
	"file-allocation": "Allocazione dei file",
	"file-allocation-falloc": "Pre-allocazione (falloc)",
	"file-allocation-trunc": "Troncamento (imposta solo la lunghezza)",
	"file-allocation-none": "Nessuna (cresce secondo necessità)",
	"file-allocation-tips":
		"La pre-allocazione riserva in anticipo blocchi reali del disco per evitare ENOSPC durante il download e ridurre la frammentazione. Ripiega sul troncamento nei file system che non la supportano.",
	"new-task-show-downloading":
		"Mostra automaticamente il download quando aggiungo una nuova attività",
	"no-confirm-before-delete-task":
		"Nessuna conferma richiesta prima di eliminare un'attività",
	"auto-file-renaming": "Rinomina automaticamente i file duplicati",
	"auto-file-renaming-tips":
		'Quando esiste già un file con lo stesso nome, salva come "file.1.ext" invece di sovrascriverlo',
	"use-remote-file-time":
		"Usa la data di modifica remota del file al salvataggio",
	"use-remote-file-time-tips":
		"Imposta la data di modifica del file sul timestamp del server remoto",
	continue: "Continua",
	"task-completed-notify": "Notifica quando un download è finito",
	"clipboard-watch": "Controlla gli appunti per i link di download",
	"clipboard-watch-tips":
		"Quando copi ovunque un link magnet o a un file, Risuko propone di scaricarlo.",
	"clipboard-watch-ext": "Estensioni di file controllate",
	"clipboard-watch-ext-tips":
		"Solo i link http(s) che terminano con queste estensioni attivano la richiesta. I link magnet, ed2k e torrent la attivano sempre. Separa con spazi o virgole.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"Risuko ora controlla gli appunti per i link di download. Disattivalo in Impostazioni → Base.",
	"prevent-sleep-while-downloading":
		"Impedisci la sospensione del sistema durante il download",
	"prevent-sleep-while-downloading-tips":
		"Mantieni il computer attivo finché è in corso un'attività",
	"shutdown-when-complete":
		"Spegni il computer al termine di tutte le attività",
	"shutdown-when-complete-tips":
		"Una tantum: spegne il computer quando la coda dei download è vuota",
	"shutdown-countdown-title": "Spegnimento tra {{seconds}} s…",
	"shutdown-countdown-cancel": "Annulla",
	"shutdown-failed": "Spegnimento non riuscito. Controlla i permessi.",
	"auto-purge-record":
		"Elimina automaticamente la cronologia di download quando l'app viene chiusa",
	"purge-record-on-start":
		"Elimina la cronologia dei download all'avvio dell'app",
	ui: "UI",
	appearance: "Aspetto",
	"theme-auto": "Auto",
	"theme-light": "Chiaro",
	"theme-dark": "Scuro",
	"font-family": "Carattere",
	"font-family-system": "Sistema",
	"font-family-rounded": "Arrotondato",
	"font-family-serif": "Con grazie",
	"font-family-mono": "Monospaziato",
	"font-family-sample": "La veloce volpe marrone scarica più rapidamente.",
	"font-size": "Dimensione",
	"font-size-small": "Piccola",
	"font-size-small-short": "Piccola",
	"font-size-default": "Predefinita",
	"font-size-default-short": "Predefinita",
	"font-size-large": "Grande",
	"font-size-large-short": "Grande",
	"font-size-extra-large": "Molto grande",
	"font-size-extra-large-short": "Molto grande",
	"auto-hide-window": "Nascondi automaticamente la finestra",
	"run-mode": "Avvia come",
	"run-mode-standard": "Applicazione Standard ",
	"run-mode-tray": "Applicazione della barra delle applicazioni",
	"run-mode-hide-tray": "Nascondi applicazione della barra delle applicazioni",
	"tray-speedometer": "La barra dei menu mostra la velocità in tempo reale",
	"show-progress-bar": "Mostra la barra di progresso del download",
	language: "Lingua",
	"change-language": "Cambia lingua",
	"hide-app-menu": "Nascondi dal menu delle app (Solo Windows & Linux)",
	proxy: "Proxy",
	"proxy-http-profile": "Proxy HTTP / FTP / Usenet",
	"proxy-p2p-profile": "Proxy P2P",
	"enable-proxy": "Usa Proxy",
	"enable-p2p-proxy": "Abilita proxy P2P",
	"proxy-p2p-tcp-server": "Server proxy TCP",
	"proxy-p2p-tcp-bypass": "Esclusioni del proxy TCP",
	"proxy-p2p-udp-server": "Server proxy UDP",
	"proxy-p2p-udp-server-placeholder":
		"Facoltativo; lascia vuoto per usare il proxy P2P TCP",
	"proxy-p2p-udp-bypass": "Esclusioni del proxy UDP",
	"proxy-bypass-input-tips":
		"Non usare proxy per questi Host e Domini, uno per linea",
	"proxy-scope-label": "Usato per",
	"proxy-scope-download": "Scarica",
	"proxy-scope-download-desc":
		"Instrada il traffico dei download tramite il proxy",
	"proxy-scope-update-app": "Aggiorna applicazione",
	"proxy-scope-update-app-desc":
		"Controlla gli aggiornamenti dell'applicazione tramite il proxy",
	"proxy-scope-update-trackers": "Aggiorna tracker",
	"proxy-scope-update-trackers-desc":
		"Recupera gli elenchi di tracker BT tramite il proxy",
	"proxy-tips": "Guida all'uso dei proxy (In Inglese)",
	"proxy-p2p-tips":
		"SOCKS5 o SOCKS5H è necessario per DHT, tracker UDP e uTP. I proxy HTTP vengono usati solo per le connessioni P2P TCP.",
	"proxy-p2p-udp-tips":
		"Lascia vuoto il proxy UDP per riutilizzare il percorso P2P TCP. Il proxy UDP richiede SOCKS5 o SOCKS5H; i proxy HTTP non possono trasportare UDP.",
	"proxy-p2p-restart-title": "Riavviare i download P2P?",
	"proxy-p2p-restart-confirm":
		"Cambiare il proxy P2P riavvia i download peer-to-peer attivi. I download HTTP, FTP e Usenet continuano a funzionare.",
	doh: "DNS over HTTPS",
	"doh-tips":
		"Risolvi i nomi host tramite un endpoint DNS cifrato invece del resolver di sistema",
	"doh-enable": "Abilita DNS over HTTPS",
	"doh-provider": "Provider",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Personalizzato",
	"doh-url": "URL dell'endpoint",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "L'endpoint DoH deve iniziare con https://",
	"doh-bootstrap": "IP di bootstrap",
	"doh-bootstrap-placeholder":
		"IP per raggiungere l'endpoint, separati da virgola (facoltativo)",
	"doh-fallback": "Ripiega sul DNS di sistema",
	"doh-fallback-tips":
		"Se una query DoH fallisce, risolvi tramite il resolver di sistema invece di far fallire il download",
	"bt-tracker": "Server di monitoraggio",
	"bt-tracker-input-tips": "Tracker servers, uno per linea",
	"bt-tracker-tips": "Raccomandati: ",
	"bt-max-peers-per-torrent": "Peer massimi per torrent",
	"bt-max-peers-per-torrent-tips":
		"Numero massimo di connessioni peer simultanee per torrent. Valori più alti migliorano la scoperta dello swarm (richiede il riavvio del motore)",
	"bt-max-outstanding-per-peer": "Richieste in sospeso massime per peer",
	"bt-max-outstanding-per-peer-tips":
		"Numero massimo di richieste di chunk simultanee in pipeline verso ogni peer (limite, fino a 500). 0 usa il valore adattivo predefinito 6–500; un budget di richieste a livello di torrent è condiviso tra i peer attivi. Richiede il riavvio del motore",
	"bt-max-connections": "Connessioni peer massime",
	"bt-max-connections-tips":
		"Connessioni peer totali su tutti i torrent, da 20 a 5000. Abbassare il valore non chiude mai le connessioni già aperte, trattiene solo quelle nuove. Su Linux, macOS e Android il valore effettivo è inoltre limitato alla metà del limite di descrittori di file del sistema",
	"bt-ban-corrupt-peers": "Blocca i peer che inviano dati corrotti",
	"bt-ban-corrupt-peers-tips":
		"Blocca un indirizzo IP per il resto della sessione quando i pezzi che ha contribuito a inviare continuano a fallire il controllo dell'hash. Molti utenti possono condividere un solo IP (operatori mobili, reti condivise), quindi un blocco può tagliare fuori anche peer innocenti (richiede il riavvio del motore)",
	"bt-ban-corrupt-strikes": "Pezzi falliti prima del blocco",
	"bt-ban-corrupt-strikes-tips":
		"Quanti pezzi falliti può aver contribuito a inviare un indirizzo IP prima di essere bloccato. Valori più alti tollerano corruzioni occasionali (richiede il riavvio del motore)",
	"bt-enable-upnp": "Port forwarding UPnP / NAT-PMP",
	"bt-enable-upnp-tips":
		"Apri automaticamente la porta di ascolto BitTorrent sul router tramite UPnP IGD, NAT-PMP o PCP (richiede il riavvio del motore)",
	"bt-upnp-lease": "Durata della mappatura delle porte (secondi)",
	"bt-upnp-lease-tips":
		"Durata del lease per le mappature di porte UPnP e NAT-PMP. Lease più brevi recuperano più in fretta da mappature obsolete (richiede il riavvio del motore)",
	"bt-enable-lsd": "Local Service Discovery",
	"bt-enable-lsd-tips":
		"Individua i peer nella rete locale tramite multicast BEP-14 (richiede il riavvio del motore)",
	"bt-encryption-policy": "Crittografia del protocollo",
	"bt-encryption-policy-tips":
		"Criterio MSE/PE per le connessioni peer. «Preferisci la crittografia» prova prima la crittografia e ripiega sul testo in chiaro (richiede il riavvio del motore)",
	"bt-encryption-plaintext": "Solo testo in chiaro",
	"bt-encryption-prefer": "Preferisci la crittografia",
	"bt-encryption-require": "Richiedi la crittografia",
	"bt-listen-v6": "Ascolta su IPv6",
	"bt-listen-v6-tips":
		"Collega un ulteriore listener TCP IPv6 sulla stessa porta (richiede il riavvio del motore)",
	"sync-tracker-tips": "Sincronizza",
	"auto-sync-tracker":
		"Aggiorna automaticamente la lista dei tracker ogni giorno",
	"ed2k-server": "Server eDonkey",
	"ed2k-server-input-tips": "Server eDonkey, uno per riga (ip:port)",
	"ed2k-server-tips": "Se vuoto, verranno usati i server predefiniti",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Abilita la ricerca delle fonti tramite Kad",
	"ed2k-enable-kad-tips":
		"Trova fonti ED2K tramite la rete eMule Kad (richiede il riavvio del motore)",
	"ed2k-kad-port": "Porta UDP di Kad",
	"ed2k-kad-port-tips":
		"Porta UDP usata per la ricerca tramite Kad. I valori validi sono compresi tra 1 e 65535 (richiede il riavvio del motore)",
	"ed2k-kad-port-invalid":
		"La porta UDP di Kad deve essere un numero intero compreso tra 1 e 65535",
	"listen-port-invalid":
		"La porta BT deve essere una porta da 1 a 65535, un intervallo come 6881-6889 o un elenco di questi separati da virgole",
	"dht-listen-port-invalid":
		"La porta DHT deve essere una porta da 1 a 65535, un intervallo come 6881-6889 o un elenco di questi separati da virgole",
	"ed2k-port-invalid":
		"La porta ED2K deve essere un numero intero da 1 a 65535",
	"ed2k-enable-upload": "Condividi file tramite ED2K",
	"ed2k-enable-upload-tips":
		"Carica verso altri peer i file completati e le parti verificate dei download ED2K in corso. Kad può continuare a elencare un file che non condividi più per un massimo di 5 ore (richiede il riavvio del motore)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Credenziali predefinite per i download FTP, FTPS e SFTP",
	"ftp-username": "Nome utente FTP",
	"ftp-password": "Password FTP",
	"sftp-private-key": "Percorso della chiave privata SSH",
	"sftp-private-key-content": "Contenuto della chiave privata SSH",
	"sftp-private-key-tips":
		"Percorso del file della chiave privata PEM, es. ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips":
		"Incolla qui il contenuto della chiave privata PEM",
	"sftp-key-passphrase": "Passphrase della chiave",
	"sftp-key-passphrase-tips": "Lascia vuoto se la chiave non ha una passphrase",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Formato di output",
	"media-settings": "Siti multimediali (yt-dlp)",
	"media-settings-tips":
		"Impostazioni predefinite per i download dai siti multimediali (YouTube, Vimeo, Bilibili e altri). Richiede yt-dlp nel PATH; ffmpeg è necessario per unire flussi video e audio separati.",
	"media-format": "Formato preferito",
	"media-format-placeholder":
		"es. bestvideo+bestaudio/best (stringa di formato yt-dlp)",
	"media-format-tips":
		"Selettore di formato yt-dlp. Lascia vuoto per la qualità migliore. Esempi: bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "Porte in ascolto",
	"bt-port": "Porte in ascolto BT",
	"dht-port": "Porte in ascolto DHT",
	"ed2k-port": "Porta di ascolto ed2k",
	security: "Sicurezza",
	rpc: "RPC",
	"rpc-host": "Indirizzo del server",
	"rpc-host-local": "Solo questo dispositivo",
	"rpc-host-all": "Tutte le interfacce di rete",
	"rpc-host-custom": "Personalizzato ({{host}})",
	"rpc-host-exposed-tips":
		"Gli altri dispositivi della tua rete possono raggiungere la porta RPC e, se abilitata, la porta PeerBanHelper. Chiunque conosca il segreto può aggiungere e controllare i download.",
	"rpc-listen-port": "Porta di Ascolto RPC",
	"rpc-secret": "RPC Secret",
	"rpc-secret-tips": "Guida sull'uso degli rpc secret (in Inglese)",
	"rpc-allow-any-dir":
		"Consenti ai client RPC di usare qualsiasi percorso locale",
	"rpc-allow-any-dir-tips":
		"Se disattivato, i download aggiunti tramite RPC possono essere salvati solo nella cartella di download, nelle sue sottocartelle e nelle cartelle di categoria e di instradamento, e i client RPC non possono far leggere all'app un file di cookie, netrc o chiave SSH.",
	pbh: "PeerBanHelper",
	"pbh-enable": "Abilita API PeerBanHelper",
	"pbh-enable-tips":
		"Espone un listener JSON-RPC compatibile con Aria2Next così PeerBanHelper può monitorare e bannare i peer BitTorrent",
	"pbh-listen-port": "Porta di ascolto PeerBanHelper",
	"pbh-rpc-secret": "Token RPC PeerBanHelper",
	"generate-pbh-rpc-secret": "Genera un nuovo token",
	"pbh-endpoint-hint":
		"In PeerBanHelper, aggiungi un downloader Aria2Next puntato a http://127.0.0.1:{{port}}/jsonrpc",
	"randomize-port": "Scegli una porta casuale",
	"generate-rpc-secret": "Genera un nuovo segreto RPC",
	"copy-rpc-url": "Copia URL RPC",
	"engine-overrides": "Override del motore (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Avanzato: imposta chiavi arbitrarie delle opzioni del motore come oggetto JSON. Le modifiche avranno effetto dopo il riavvio.",
	"engine-overrides-invalid":
		"Gli override del motore devono essere un oggetto JSON valido.",
	"engine-overrides-reserved-keys":
		"Chiavi riservate di override del motore ignorate: {{keys}}",
	"engine-overrides-too-large":
		"L'override del motore è troppo grande: al massimo 64 chiavi, 64 caratteri per chiave e 2048 caratteri per valore.",
	developer: "Sviluppatore",
	"user-agent": "User-Agent",
	"mock-user-agent": "Cambia User-Agent",
	cookies: "Cookie",
	"cookies-tips":
		"Carica i cookie da un file cookies.txt in formato Netscape/Mozilla (accetta anche l'output di curl/wget).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Credenziali di dominio salvate",
	"saved-cookies-tips":
		"Cookie e User-Agent importati da un browser per aggirare Cloudflare o altri download protetti da autenticazione. Risuko li riutilizza per i nuovi download sullo stesso dominio finché non scadono o smettono di funzionare.",
	"saved-cookies-empty": "Ancora nessuna credenziale salvata.",
	"saved-cookies-host": "Dominio",
	"saved-cookies-browser": "Browser",
	"saved-cookies-count": "Cookie",
	"saved-cookies-imported": "Importato",
	"saved-cookies-actions": "Azioni",
	"saved-cookies-clear": "Cancella tutto",
	"saved-cookies-confirm-clear":
		"Eliminare tutte le credenziali di dominio salvate? I download futuri su quei siti potrebbero fallire finché non le reimporti.",
	"saved-cookies-delete": "Elimina",
	netrc: "Credenziali .netrc",
	"netrc-tips":
		"Inserisce l'autenticazione HTTP Basic da un file .netrc quando l'URL non contiene credenziali incorporate. Predefinito: ~/.netrc (o %USERPROFILE%/_netrc su Windows).",
	"netrc-path-placeholder": "Percorso .netrc personalizzato (facoltativo)",
	"no-netrc": "Disattiva la ricerca in .netrc",
	"no-netrc-tips":
		"Ignora completamente .netrc. Utile quando il file contiene credenziali non legate ai download.",
	"app-log-path": "Posizione log dell'app",
	"log-dir-override-placeholder":
		"Directory dei log personalizzata (lascia vuoto per quella predefinita)",
	"log-dir-override-tips":
		"I log vengono attualmente scritti in {{path}}. Impostando una directory personalizzata (ad es. dentro Download) diventano visibili a qualsiasi gestore di file. Le modifiche hanno effetto dopo il prossimo riavvio dell'app.",
	"download-session-path": "Posizione sessione di download",
	"session-reset": "Reimposta la sessione di download",
	"session-reset-confirm":
		"Sei sicuro di voler reimpostare la sessione di download?",
	"factory-reset": "Reset di fabbrica",
	"factory-reset-confirm":
		"Sei sicuro di voler riportare alle impostazioni di fabbrica l'app?",
	"download-protocol": "Protocolli",
	"protocols-default-client": "Imposta di Default i seguenti protocolli",
	"protocols-magnet": "Magnet [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc:// adcs:// dchub:// nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella://, gnet:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "Integrazione del demone giFT",
	"gift-enabled": "Abilita delega a giFT",
	"gift-host": "Host giFT",
	"gift-port": "Porta giFT",
	"gift-not-detected": "Demone giFT non rilevato all'host:porta configurato.",
	"browser-extensions": "Estensione per browser",
	"baidu-exporter": "BaiduExporter",
	"browser-extensions-tips": "Fornita dalla community, ",
	"baidu-exporter-help": "Clicca qui per scoprire il funzionamento",
	"auto-update": "Auto Update",
	"auto-check-update":
		"Verifica automaticamente la disponibilità di aggiornamenti",
	"last-check-update-time":
		"Ultima volta quando gli aggiornamenti sono stati verificati",
	"completion-script": "Script al completamento",
	"completion-script-enabled": "Esegui uno script al termine del download",
	"completion-script-tips":
		"Eseguito in sandbox tramite tauri-plugin-shell. Viene eseguito una volta al termine di ogni attività (successo o errore).",
	"completion-script-command": "Comando",
	"completion-script-command-placeholder":
		"es. /usr/local/bin/notify.sh o C:\\Tools\\notify.exe",
	"completion-script-args": "Argomenti",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Segnaposto: {path}, {hash}, {status}. Disponibili anche come variabili ambiente RISUKO_PATH, RISUKO_HASH, RISUKO_STATUS.",
	"completion-script-timeout": "Tempo limite (ms)",
	"completion-script-test": "Test script",
	"completion-script-test-ok": "Test script riuscito",
	"completion-script-test-fail": "Test script non riuscito",
	"completion-script-empty": "Specifica prima un comando",
	"completion-script-not-approved":
		"Lo script di completamento non è stato consentito, quindi non è stato salvato nulla.",
	"not-saved": "Preferenze non salvate",
	"not-saved-confirm":
		"Le preferenze modificate andranno perse, sei sicuro di uscire?",
	"saved-credentials": "Credenziali salvate",
	"saved-credentials-description":
		"Gestisci le credenziali di autenticazione salvate per i download",
	"no-saved-credentials": "Nessuna credenziale salvata",
	"credential-host": "Host",
	"credential-label": "Etichetta",
	"credential-protocol": "Protocollo",
	"credential-last-used": "Ultimo utilizzo",
	"credential-delete": "Elimina credenziale",
	"credential-delete-confirm":
		"Sei sicuro di voler eliminare questa credenziale salvata?",
	"vault-enabled": "Segreti archiviati nel portachiavi del sistema operativo",
	"vault-disabled":
		"Portachiavi del sistema operativo non disponibile — segreti archiviati in chiaro",
	"credential-encrypted-badge":
		"Archiviata nel portachiavi del sistema operativo",
	"credential-vault-unavailable":
		"Portachiavi del sistema operativo non raggiungibile; le nuove credenziali verranno salvate in chiaro",
	"speed-limit-enabled": "Abilita i limiti di velocità",
	"speed-limit-enabled-description":
		"Applica i limiti qui sotto. Se disattivato, i download vanno a piena velocità; i valori vengono conservati.",
	"appearance-theme": "Tema",
	"appearance-typography": "Tipografia",
	"appearance-window": "Finestra",
	"task-list-style": "Elenco attività",
	"task-list-style-description":
		"Come vengono mostrati i download nell'elenco attività.",
	"task-list-style-compact": "Compatto",
	"task-list-style-card": "Schede",
	"sidebar-collapsed": "Comprimi barra laterale",
	"sidebar-collapsed-description": "Mostra solo le icone nella barra laterale.",
};
