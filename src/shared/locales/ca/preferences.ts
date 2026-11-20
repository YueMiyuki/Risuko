export default {
	basic: "Bàsic",
	advanced: "Avançat",
	cloudSinks: "Destinacions al núvol",
	usenet: "Usenet",
	search: "Cerca",
	"search-placeholder": "Cerca a totes les preferències",
	"search-all-tabs": "Totes les preferències",
	"search-results": "{{count}} preferències coincidents",
	"search-no-results": "No s'ha trobat cap preferència coincident",
	"search-clear": "Esborra la cerca",
	"search-go-to-tab": "Obre la preferència",
	"search-more-results": "Refina la cerca per veure més resultats",
	"usenet-tips":
		"Gestiona els proveïdors NNTP i els valors per defecte de la neteja segura d'arxius.",
	"usenet-profiles": "Perfils de proveïdor",
	"usenet-profiles-tips":
		"Configura la prioritat, la seguretat, l'autenticació i els límits de connexió dels proveïdors.",
	"usenet-add-profile": "Afegeix un perfil",
	"usenet-no-profiles": "No hi ha cap perfil de proveïdor configurat.",
	"usenet-no-profiles-tips":
		"Afegeix almenys un proveïdor abans d'importar una descàrrega NZB.",
	"usenet-edit-profile": "Edita el perfil",
	"usenet-profile-dialog-tips":
		"Els paràmetres del perfil es poden sincronitzar entre dispositius. Les credencials es mantenen locals en aquest dispositiu.",
	"usenet-profile-dialog-tips-vault":
		"Els paràmetres del perfil es poden sincronitzar entre dispositius. Les credencials es queden al clauer segur d'aquest dispositiu.",
	"usenet-profile-name": "Nom del perfil",
	"usenet-profile-name-placeholder": "Proveïdor principal",
	"usenet-profile-host": "Amfitrió NNTP",
	"usenet-profile-port": "Port",
	"usenet-security": "Seguretat de la connexió",
	"usenet-tls": "TLS implícit",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "NNTP en text pla",
	"usenet-authentication": "Autenticació",
	"usenet-auth-credentials": "Nom d'usuari i contrasenya",
	"usenet-auth-anonymous": "Anònim",
	"usenet-username": "Nom d'usuari",
	"usenet-password": "Contrasenya",
	"usenet-credentials-saved": "Credencials desades",
	"usenet-credentials-unavailable": "Estat de les credencials no disponible",
	"usenet-credentials-saved-tips":
		"Les credencials ja estan desades. Deixa tots dos camps buits per mantenir-les sense canvis.",
	"usenet-plain-warning-title": "Connexió sense xifrar",
	"usenet-plain-opt-in":
		"Ho entenc i vull permetre NNTP en text pla en aquest perfil.",
	"usenet-plain-warning":
		"NNTP en text pla no xifra les credencials ni les dades descarregades. Usa'l només quan el teu proveïdor ho exigeixi explícitament.",
	"usenet-enabled": "Activat",
	"usenet-enabled-tips":
		"Els perfils desactivats es conserven, però no s'usen per a les descàrregues.",
	"usenet-disabled": "Desactivat",
	"usenet-priority": "Prioritat",
	"usenet-priority-tips":
		"Els valors més baixos es proven primer. Les prioritats iguals es reparteixen la càrrega.",
	"usenet-priority-value": "Prioritat {{value}}",
	"usenet-max-connections": "Connexions màximes",
	"usenet-max-connections-tips":
		"El límit de connexions d'aquest perfil de proveïdor.",
	"usenet-connections-value": "{{value}} connexions",
	"usenet-test": "Prova",
	"usenet-edit": "Edita el perfil",
	"usenet-remove": "Elimina",
	"usenet-remove-title": "Eliminar el perfil de proveïdor?",
	"usenet-remove-confirm":
		"{{name}} ja no estarà disponible per a les descàrregues. Les seves credencials desades també s'eliminaran d'aquest dispositiu.",
	"usenet-profile-saved": "Perfil de proveïdor desat.",
	"usenet-profile-removed": "S'ha eliminat {{name}}.",
	"usenet-test-success": "Connectat a {{name}}.",
	"usenet-test-failed": "Ha fallat la connexió: {{message}}",
	"usenet-validation-name": "Introdueix un nom de perfil.",
	"usenet-validation-host": "Introdueix un amfitrió NNTP.",
	"usenet-validation-port": "El port ha d'estar entre 1 i 65535.",
	"usenet-validation-plain":
		"Confirma l'advertència sobre NNTP en text pla abans de desar.",
	"usenet-validation-priority": "La prioritat ha de ser zero o superior.",
	"usenet-validation-connections": "Les connexions han d'estar entre 1 i 128.",
	"usenet-validation-credentials":
		"Introdueix un nom d'usuari i una contrasenya, o tria l'autenticació anònima.",
	"usenet-validation-credential-status":
		"L'estat de les credencials no està disponible. Torna-ho a provar abans de canviar aquest perfil.",
	"usenet-cleanup": "Neteja en cas d'èxit",
	"usenet-cleanup-tips":
		"La neteja només s'executa després del muntatge complet i d'una extracció correcta.",
	"usenet-cleanup-action": "Després de l'èxit verificat",
	"usenet-cleanup-keep": "Conserva tots els fitxers d'entrada",
	"usenet-cleanup-keep-tips":
		"Conserva els fitxers PAR2 i tots els volums d'arxiu.",
	"usenet-cleanup-par2": "Elimina els fitxers PAR2",
	"usenet-cleanup-par2-tips":
		"Elimina els fitxers de paritat, però conserva els volums d'arxiu descarregats.",
	"usenet-cleanup-all": "Elimina els PAR2 i els volums d'arxiu",
	"usenet-cleanup-all-tips":
		"Elimina els fitxers de paritat i els volums d'arxiu un cop l'extracció ha estat correcta.",
	"usenet-archive-safety": "Seguretat dels arxius",
	"usenet-limits-defaults":
		"Els límits aplicats a tota la tasca protegeixen l'emmagatzematge i el processament durant la verificació i l'extracció.",
	"usenet-desktop-limits": "Límits d'escriptori",
	"usenet-android-limits": "Límits d'Android",
	"usenet-limits-summary":
		"{{entries}} entrades · {{total}} d'expansió total · {{perEntry}} per entrada",
	"usenet-limits-secondary-summary":
		"relació {{ratio}}:1 · {{depth}} nivells d'imbricació · {{reserve}} d'espai lliure reservat · {{hours}} hores actives",
	"usenet-customize-limits": "Personalitza els límits",
	"usenet-hide-limits": "Amaga els límits",
	"usenet-limits-adjusted": "S'han ajustat els límits sincronitzats",
	"usenet-limits-adjusted-tips":
		"Un o més valors superaven el límit màxim d'aquesta plataforma i s'han reduït.",
	"usenet-max-entries": "Entrades màximes",
	"usenet-max-expanded": "Mida total expandida",
	"usenet-max-entry": "Mida d'una sola entrada",
	"usenet-max-depth": "Profunditat d'imbricació",
	"usenet-max-ratio": "Relació de compressió",
	"usenet-free-space-reserve": "Reserva d'espai lliure",
	"usenet-max-active-time": "Temps de processament actiu",
	"usenet-unit-entries": "entrades",
	"usenet-unit-levels": "nivells",
	"usenet-unit-hours": "hores",
	"usenet-limits-hard-cap":
		"Els valors no poden superar quatre vegades els valors per defecte de la plataforma. Les excepcions per tasca es confirmen per separat.",
	"usenet-restore-limits": "Restaura els valors per defecte de la plataforma",
	"usenet-limits-restored":
		"Els límits d'arxiu s'han restaurat als valors per defecte de la plataforma.",
	save: "Guardar i aplicar",
	"save-success-message": "Preferències guardades amb èxit",
	"save-fail-message": "Hi va haver un error al guardar les teves preferències",
	discard: "Descartar",
	"history-directories": "Directoris de baixada recents",
	"favorite-directory": "Directori preferit",
	"unfavorite-directory": "Treu el directori dels preferits",
	"remove-history-directory": "Treu el directori de l'historial",
	startup: "Inici",
	"open-at-login": "Obrir en iniciar sessió",
	"keep-window-state": "Mantenir la mida i la posició de la finestra al sortir",
	"auto-resume-all":
		"Reprendre automàticament totes les tasques sense finalitzar",
	"default-dir": "Ruta per defecte",
	"file-category-dirs": "Rutes per categoria",
	"file-category-dirs-tips":
		"Substitueix la ruta de descàrrega per defecte per a tipus de fitxer concrets. Deixa-ho buit per usar la ruta per defecte.",
	"task-routing-rules": "Regles d'encaminament de tasques",
	"task-routing-rules-tips":
		"Encamina les tasques a directoris segons patrons de nom de fitxer. Guanya la primera regla que coincideixi.",
	"task-routing-rule-pattern-placeholder": "Patró (p. ex. *.mkv)",
	"task-routing-rule-label-placeholder": "Etiqueta",
	"task-routing-rule-dir-placeholder": "Directori",
	"task-routing-rule-add": "Afegeix una regla",
	"file-category-music": "Música",
	"file-category-video": "Vídeo",
	"file-category-image": "Imatge",
	"file-category-document": "Document",
	"file-category-compressed": "Comprimit",
	"file-category-program": "Programa",
	"file-category-rss": "RSS",
	"mas-default-dir-tips":
		"Degut a les restriccions de la botiga d'aplicacions, la ruta per defecte es recomana que sigui ~/Downloads",
	"transfer-settings": "Transmission",
	"transfer-speed-upload": "Límit de pujada",
	"transfer-speed-download": "Límit de baixada",
	"transfer-speed-unlimited": "Il·limitat",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Deseu l'enllaç magnet com a fitxer torrent",
	"bt-create-subfolder":
		"Agrupa els torrents de diversos fitxers en una subcarpeta",
	"bt-create-subfolder-tips":
		"Si està activat, les descàrregues BitTorrent i magnet de diversos fitxers es col·loquen dins d'una carpeta amb el nom del torrent. Si està desactivat, els fitxers s'escriuen directament al directori de descàrrega.",
	"bt-upload-rate-limit": "Límit de pujada de BitTorrent",
	"bt-upload-rate-limit-tips":
		"Limita les pujades de BitTorrent a més del límit global de pujada. 0 vol dir que no hi ha cap límit separat.",
	"bt-auto-download-content":
		"Descarregueu automàticament el contingut de Magnet i Torrent",
	"bt-force-encryption": "Forçar xifratge de BT",
	"keep-seeding": "Seguiu sembrant fins aturar-lo manualment",
	"seed-ratio": "Relació de llavors",
	"seed-time": "Temps de llavors",
	"seed-time-unit": "minuts",
	"task-manage": "Gestió de tasques",
	"max-concurrent-downloads": "Tasques màximes actives",
	"connections-per-task": "Connexions per tasca",
	"max-connection-per-server": "Connexions màximes per servidor",
	"auto-retry": "Reintenta automàticament les tasques fallides",
	"auto-retry-strategy": "Mode d'interval de reintent",
	"auto-retry-strategy-static": "Estàtic",
	"auto-retry-strategy-exponential": "Retrocés exponencial",
	"auto-retry-interval": "Interval base de reintent",
	"auto-retry-interval-unit": "segons",
	"auto-retry-max-attempts": "Màxim d'intents de reintent (0 = il·limitat)",
	"worker-max-retries": "Màxim de reintents per peça de cada fil de treball",
	"auto-detect-low-speed-tasks": "Detecta automàticament les tasques lentes",
	"auto-detect-low-speed-tasks-tips":
		"Si una tasca en execució es manté per sota del llindar de velocitat, es posa en pausa i es reinicia automàticament.",
	"low-speed-threshold": "Llindar de velocitat baixa",
	"low-speed-threshold-unit": "KB/s",
	"http-reliability": "Fiabilitat d'HTTP / xarxa",
	"http-reliability-tips":
		"Ajusta el temps d'espera de connexió i la detecció de transferències bloquejades per a les descàrregues HTTP/HTTPS/FTP.",
	"connect-timeout": "Temps d'espera de connexió",
	"nzb-body-timeout": "Temps d'espera del cos NZB",
	"lowest-speed-limit": "Límit mínim de velocitat",
	"lowest-speed-limit-timeout": "Temps d'espera de bloqueig",
	"lowest-speed-limit-help":
		"Si una descàrrega es manté per sota del límit mínim de velocitat durant el temps d'espera de bloqueig, es cancel·la el fil de treball i (si el reintent automàtic està activat) es reinicia. Posa el límit a 0 per desactivar-ho.",
	"uri-selector": "Selecció de rèpliques",
	"uri-selector-feedback":
		"Per retroacció (descarta les rèpliques lentes o fallides)",
	"uri-selector-inorder": "En ordre (prova les rèpliques d'esquerra a dreta)",
	"uri-selector-adaptive": "Adaptatiu (prefereix la més ràpida)",
	"unit-seconds": "segons",
	"unit-kib-per-sec": "KiB/s",
	storage: "Emmagatzematge",
	"storage-tips": "Com reserva el motor l'espai de disc abans de descarregar.",
	"file-allocation": "Assignació de fitxers",
	"file-allocation-falloc": "Preassigna (falloc)",
	"file-allocation-trunc": "Trunca (només estableix la mida)",
	"file-allocation-none": "Cap (creix segons calgui)",
	"file-allocation-tips":
		"La preassignació reserva blocs de disc reals per endavant per evitar ENOSPC a mitja descàrrega i reduir la fragmentació. Si el sistema de fitxers no ho admet, es recorre a truncar.",
	"new-task-show-downloading":
		"Mostrar automàticament la descàrrega després d'afegir una tasca",
	"no-confirm-before-delete-task":
		"No cal confirmar abans de suprimir la tasca",
	"auto-file-renaming": "Reanomena automàticament els fitxers duplicats",
	"auto-file-renaming-tips":
		'Quan ja existeix un fitxer amb el mateix nom, es desa com a "file.1.ext" en lloc de sobreescriure\'l',
	"use-remote-file-time": "Usa l'hora de modificació del fitxer remot en desar",
	"use-remote-file-time-tips":
		"Estableix l'hora de modificació del fitxer segons la marca de temps del servidor remot",
	continue: "Continuar",
	"task-completed-notify": "Notificar després que la descàrrega finalitzi",
	"clipboard-watch":
		"Vigila el porta-retalls per detectar enllaços de descàrrega",
	"clipboard-watch-tips":
		"Quan copies un enllaç magnet o d'un fitxer a qualsevol lloc, Risuko ofereix descarregar-lo.",
	"clipboard-watch-ext": "Extensions de fitxer vigilades",
	"clipboard-watch-ext-tips":
		"Només els enllaços http(s) que acaben amb aquestes extensions mostren l'avís. Els enllaços magnet, ed2k i torrent ho fan sempre. Separa-les amb espais o comes.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"Risuko ara vigila el porta-retalls per detectar enllaços de descàrrega. Desactiva-ho a Preferències → Bàsic.",
	"prevent-sleep-while-downloading":
		"Evita la suspensió del sistema durant la baixada",
	"prevent-sleep-while-downloading-tips":
		"Manté l'ordinador actiu mentre hi hagi una tasca activa en curs",
	"shutdown-when-complete": "Apaga l'ordinador quan acabin totes les tasques",
	"shutdown-when-complete-tips":
		"Només una vegada: apaga l'ordinador quan la cua de descàrregues quedi buida",
	"shutdown-countdown-title": "S'apagarà d'aquí a {{seconds}} s…",
	"shutdown-countdown-cancel": "Cancel·la",
	"shutdown-failed": "Ha fallat l'apagada. Comprova els permisos.",
	"auto-purge-record":
		"Purgar automàticament el registre de descàrregues en sortir",
	"purge-record-on-start":
		"Purga els registres de descàrrega en iniciar l'aplicació",
	ui: "UI",
	appearance: "Aparença",
	"theme-auto": "Auto",
	"theme-light": "Clar",
	"theme-dark": "Fosc",
	"font-family": "Tipus de lletra",
	"font-family-system": "Sistema",
	"font-family-rounded": "Arrodonida",
	"font-family-serif": "Amb serifa",
	"font-family-mono": "Monoespaiada",
	"font-family-sample": "La guineu marró ràpida descarrega més de pressa.",
	"font-size": "Mida",
	"font-size-small": "Petita",
	"font-size-small-short": "Petita",
	"font-size-default": "Per defecte",
	"font-size-default-short": "Per defecte",
	"font-size-large": "Gran",
	"font-size-large-short": "Gran",
	"font-size-extra-large": "Molt gran",
	"font-size-extra-large-short": "Molt gran",
	"auto-hide-window": "Amaga automàticament les finestres",
	"run-mode": "Executa com",
	"run-mode-standard": "Aplicació estàndard",
	"run-mode-tray": "Aplicació de la safata",
	"run-mode-hide-tray": "Amagar l'aplicació de la safata",
	"tray-speedometer":
		"La safata de barres de menús mostra la velocitat en temps real",
	"show-progress-bar": "Mostra la barra de progrés de la descàrrega",
	language: "Idioma",
	"change-language": "Canviar idioma",
	"hide-app-menu": "Ocultar el menú (només Windows i Linux)",
	proxy: "Proxy",
	"proxy-http-profile": "Proxy HTTP / FTP / Usenet",
	"proxy-p2p-profile": "Proxy P2P",
	"enable-proxy": "Activar proxy",
	"enable-p2p-proxy": "Activa el proxy P2P",
	"proxy-p2p-tcp-server": "Servidor proxy TCP",
	"proxy-p2p-tcp-bypass": "Excepcions del proxy TCP",
	"proxy-p2p-udp-server": "Servidor proxy UDP",
	"proxy-p2p-udp-server-placeholder":
		"Opcional; deixa-ho en blanc per usar el proxy P2P TCP",
	"proxy-p2p-udp-bypass": "Excepcions del proxy UDP",
	"proxy-bypass-input-tips":
		"Eviteu la configuració del servidor intermediari per a aquests amfitrions i dominis, un per línia",
	"proxy-scope-label": "S'utilitza per a",
	"proxy-scope-download": "Descàrrega",
	"proxy-scope-download-desc":
		"Encamina el trànsit de baixada a través del servidor intermediari",
	"proxy-scope-update-app": "Actualització de l'aplicació",
	"proxy-scope-update-app-desc":
		"Comprova actualitzacions de l'aplicació a través del servidor intermediari",
	"proxy-scope-update-trackers": "Actualitza els rastrejadors",
	"proxy-scope-update-trackers-desc":
		"Obtén llistes de rastrejadors BT a través del servidor intermediari",
	"proxy-tips": "Consulteu el manual del servidor intermediari",
	"proxy-p2p-tips":
		"Cal SOCKS5 o SOCKS5H per a DHT, els trackers UDP i uTP. Els servidors intermediaris HTTP només s'usen per a connexions P2P per TCP.",
	"proxy-p2p-udp-tips":
		"Deixa el proxy UDP en blanc per reutilitzar la ruta P2P per TCP. El proxy UDP requereix SOCKS5 o SOCKS5H; els servidors intermediaris HTTP no poden transportar UDP.",
	"proxy-p2p-restart-title": "Reiniciar les descàrregues P2P?",
	"proxy-p2p-restart-confirm":
		"En canviar el proxy P2P es reinicien les descàrregues actives entre parells. Les descàrregues HTTP, FTP i Usenet continuen executant-se.",
	doh: "DNS sobre HTTPS",
	"doh-tips":
		"Resol els noms d'amfitrió mitjançant un punt final DNS xifrat en lloc del resolutor del sistema",
	"doh-enable": "Activa DNS sobre HTTPS",
	"doh-provider": "Proveïdor",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Personalitzat",
	"doh-url": "URL del punt final",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "El punt final DoH ha de començar per https://",
	"doh-bootstrap": "IP d'arrencada",
	"doh-bootstrap-placeholder":
		"IP per arribar al punt final, separades per comes (opcional)",
	"doh-fallback": "Recorre al DNS del sistema",
	"doh-fallback-tips":
		"Si una consulta DoH falla, resol amb el resolutor del sistema en lloc de fer fallar la descàrrega",
	"bt-tracker": "Servidors de rastrejadors",
	"bt-tracker-input-tips": "Servidors de rastrejadors, un per línia",
	"bt-tracker-tips": "Recomanat: ",
	"bt-max-peers-per-torrent": "Màxim de parells per torrent",
	"bt-max-peers-per-torrent-tips":
		"Nombre màxim de connexions simultànies amb parells per torrent. Els valors més alts milloren la descoberta de l'eixam (cal reiniciar el motor)",
	"bt-max-outstanding-per-peer": "Màxim de sol·licituds pendents per parell",
	"bt-max-outstanding-per-peer-tips":
		"Nombre màxim de sol·licituds de fragments simultànies encadenades cap a cada parell (límit, fins a 500). 0 usa el valor adaptatiu per defecte de 6–500; entre els parells actius es comparteix un pressupost de sol·licituds per a tot el torrent. Cal reiniciar el motor",
	"bt-max-connections": "Màxim de connexions amb parells",
	"bt-max-connections-tips":
		"Total de connexions amb parells entre tots els torrents, de 20 a 5000. Reduir-lo mai tanca les connexions ja obertes, només en frena de noves. A Linux, macOS i Android, el valor efectiu també es limita a la meitat del límit de descriptors de fitxer del sistema",
	"bt-ban-corrupt-peers": "Bloqueja els parells que envien dades corruptes",
	"bt-ban-corrupt-peers-tips":
		"Bloqueja una adreça IP durant la resta de la sessió quan les peces que ha ajudat a enviar continuen fallant la comprovació del hash. Molts usuaris poden compartir una sola IP (operadors mòbils, xarxes compartides), de manera que un bloqueig també pot deixar sense connexió parells innocents (cal reiniciar el motor)",
	"bt-ban-corrupt-strikes": "Peces fallides abans del bloqueig",
	"bt-ban-corrupt-strikes-tips":
		"Quantes peces fallides pot aportar una adreça IP abans de ser bloquejada. Els valors més alts toleren la corrupció ocasional (cal reiniciar el motor)",
	"bt-enable-upnp": "Reenviament de ports UPnP / NAT-PMP",
	"bt-enable-upnp-tips":
		"Obre automàticament el port d'escolta de BitTorrent al router mitjançant UPnP IGD, NAT-PMP o PCP (cal reiniciar el motor)",
	"bt-upnp-lease": "Durada de l'assignació de ports (segons)",
	"bt-upnp-lease-tips":
		"Durada de la concessió de les assignacions de ports UPnP i NAT-PMP. Les concessions més curtes es recuperen més de pressa d'assignacions obsoletes (cal reiniciar el motor)",
	"bt-enable-lsd": "Descoberta de serveis locals",
	"bt-enable-lsd-tips":
		"Descobreix parells a la xarxa local mitjançant multidifusió BEP-14 (cal reiniciar el motor)",
	"bt-encryption-policy": "Xifratge del protocol",
	"bt-encryption-policy-tips":
		"Política MSE/PE per a les connexions amb parells. 'Prefereix' prova primer el xifratge i recorre al text pla si falla (cal reiniciar el motor)",
	"bt-encryption-plaintext": "Només text pla",
	"bt-encryption-prefer": "Prefereix el xifratge",
	"bt-encryption-require": "Exigeix el xifratge",
	"bt-listen-v6": "Escolta per IPv6",
	"bt-listen-v6-tips":
		"Vincula un receptor TCP IPv6 addicional al mateix port (cal reiniciar el motor)",
	"sync-tracker-tips": "Sincronitzar",
	"auto-sync-tracker":
		"Actualitza la llista de seguidors automàticament cada dia",
	"ed2k-server": "Servidors eDonkey",
	"ed2k-server-input-tips": "Servidors eDonkey, un per línia (ip:port)",
	"ed2k-server-tips": "S'usaran els servidors per defecte si és buit",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Activa la descoberta de fonts mitjançant Kad",
	"ed2k-enable-kad-tips":
		"Descobreix fonts ED2K a través de la xarxa eMule Kad (cal reiniciar el motor)",
	"ed2k-kad-port": "Port UDP de Kad",
	"ed2k-kad-port-tips":
		"Port UDP utilitzat per a la descoberta mitjançant Kad. Els valors vàlids són de l'1 al 65535 (cal reiniciar el motor)",
	"ed2k-kad-port-invalid":
		"El port UDP de Kad ha de ser un enter entre 1 i 65535",
	"listen-port-invalid":
		"El port BT ha de ser un port de l'1 al 65535, un interval com 6881-6889 o una llista d'aquests separats per comes",
	"dht-listen-port-invalid":
		"El port DHT ha de ser un port de l'1 al 65535, un interval com 6881-6889 o una llista d'aquests separats per comes",
	"ed2k-port-invalid": "El port ED2K ha de ser un enter de l'1 al 65535",
	"ed2k-enable-upload": "Comparteix fitxers per ED2K",
	"ed2k-enable-upload-tips":
		"Puja a altres parells els fitxers acabats i les parts verificades de les baixades ED2K en curs. Kad pot continuar llistant un fitxer que ja no comparteixes fins a 5 hores (cal reiniciar el motor)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Credencials per defecte per a les descàrregues FTP, FTPS i SFTP",
	"ftp-username": "Nom d'usuari FTP",
	"ftp-password": "Contrasenya FTP",
	"sftp-private-key": "Ruta de la clau privada SSH",
	"sftp-private-key-content": "Contingut de la clau privada SSH",
	"sftp-private-key-tips":
		"Ruta del fitxer de clau privada PEM, p. ex. ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips":
		"Enganxa aquí el contingut de la clau privada PEM",
	"sftp-key-passphrase": "Frase de pas de la clau",
	"sftp-key-passphrase-tips": "Deixa-ho buit si la clau no té frase de pas",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Format de sortida",
	"media-settings": "Llocs multimèdia (yt-dlp)",
	"media-settings-tips":
		"Paràmetres per defecte per a les descàrregues de llocs multimèdia (YouTube, Vimeo, Bilibili i més). Requereix yt-dlp al PATH; cal ffmpeg per combinar fluxos de vídeo i àudio separats.",
	"media-format": "Format preferit",
	"media-format-placeholder":
		"p. ex. bestvideo+bestaudio/best (cadena de format de yt-dlp)",
	"media-format-tips":
		"Selector de format de yt-dlp. Deixa-ho buit per obtenir la millor qualitat. Exemples: bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "Escolta Ports",
	"bt-port": "Port d'escolta BT",
	"dht-port": "Port d'escolta DHT",
	"ed2k-port": "Port d'escolta ed2k",
	security: "Seguretat",
	rpc: "RPC",
	"rpc-host": "Adreça del servidor",
	"rpc-host-local": "Només aquest dispositiu",
	"rpc-host-all": "Totes les interfícies de xarxa",
	"rpc-host-custom": "Personalitzada ({{host}})",
	"rpc-host-exposed-tips":
		"Els altres dispositius de la teva xarxa poden accedir al port RPC i, si està activat, al port de PeerBanHelper. Qualsevol persona que conegui el secret pot afegir i controlar descàrregues.",
	"rpc-listen-port": "Port d'Escolta RPC",
	"rpc-secret": "Clau RPC",
	"rpc-secret-tips": "Mirar manual de la clau RPC",
	"rpc-allow-any-dir": "Permet que els clients RPC usin qualsevol ruta local",
	"rpc-allow-any-dir-tips":
		"Si està desactivat, les descàrregues afegides per RPC només es poden desar al directori de descàrrega, als seus subdirectoris i als teus directoris de categoria i d'encaminament, i els clients RPC no poden fer que l'aplicació llegeixi un fitxer de cookies, un fitxer netrc ni una clau SSH.",
	pbh: "PeerBanHelper",
	"pbh-enable": "Activa l’API de PeerBanHelper",
	"pbh-enable-tips":
		"Exposa un receptor JSON-RPC compatible amb Aria2Next perquè PeerBanHelper pugui supervisar i bloquejar els parells de BitTorrent",
	"pbh-listen-port": "Port d’escolta de PeerBanHelper",
	"pbh-rpc-secret": "Token RPC de PeerBanHelper",
	"generate-pbh-rpc-secret": "Genera un token nou",
	"pbh-endpoint-hint":
		"A PeerBanHelper, afegeix un descarregador Aria2Next que apunti a http://127.0.0.1:{{port}}/jsonrpc",
	"randomize-port": "Tria un port aleatori",
	"generate-rpc-secret": "Genera un secret RPC nou",
	"copy-rpc-url": "Copia l'URL RPC",
	"engine-overrides": "Sobreescriptures del motor (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Avançat: estableix claus arbitràries d'opcions del motor com a objecte JSON. Els canvis tenen efecte després de reiniciar.",
	"engine-overrides-invalid":
		"Les sobreescriptures del motor han de ser un objecte JSON vàlid.",
	"engine-overrides-reserved-keys":
		"S'han ignorat les claus reservades de les sobreescriptures del motor: {{keys}}",
	"engine-overrides-too-large":
		"Les sobreescriptures del motor són massa grans: com a màxim 64 claus, 64 caràcters per clau i 2048 caràcters per valor.",
	developer: "Desenvolupador",
	"user-agent": "User-Agent",
	"mock-user-agent": "User-Agent simulat",
	cookies: "Cookies",
	"cookies-tips":
		"Carrega cookies d'un fitxer cookies.txt en format Netscape/Mozilla (també accepta la sortida de curl/wget).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Credencials de domini desades",
	"saved-cookies-tips":
		"Cookies i User-Agent importats d'un navegador en esquivar Cloudflare o altres descàrregues protegides per autenticació. Risuko les reutilitza per a les noves descàrregues al mateix domini fins que caduquen o deixen de funcionar.",
	"saved-cookies-empty": "Encara no hi ha credencials desades.",
	"saved-cookies-host": "Domini",
	"saved-cookies-browser": "Navegador",
	"saved-cookies-count": "Cookies",
	"saved-cookies-imported": "Importat",
	"saved-cookies-actions": "Accions",
	"saved-cookies-clear": "Esborra-ho tot",
	"saved-cookies-confirm-clear":
		"Eliminar totes les credencials de domini desades? Les descàrregues futures en aquests llocs poden fallar fins que les tornis a importar.",
	"saved-cookies-delete": "Elimina",
	netrc: "Credencials .netrc",
	"netrc-tips":
		"Injecta l'autenticació HTTP Basic d'un fitxer .netrc quan l'URL no té credencials incrustades. Per defecte: ~/.netrc (o %USERPROFILE%/_netrc a Windows).",
	"netrc-path-placeholder": "Substitueix la ruta de .netrc (opcional)",
	"no-netrc": "Desactiva la cerca de .netrc",
	"no-netrc-tips":
		"Omet .netrc del tot. Útil quan el fitxer conté credencials que no són per a descàrregues.",
	"app-log-path": "Ruta del log",
	"log-dir-override-placeholder":
		"Directori de registres personalitzat (deixa-ho en blanc per usar el per defecte)",
	"log-dir-override-tips":
		"Actualment els registres s'escriuen a {{path}}. Si estableixes un directori personalitzat (p. ex. dins de Descàrregues), seran visibles des de qualsevol gestor de fitxers. Els canvis tenen efecte després del proper reinici de l'aplicació.",
	"download-session-path": "Ruta de descàrrega de la sessió",
	"session-reset": "Restableix la sessió de descàrregues",
	"session-reset-confirm":
		"Estàs segur que vols restablir la sessió de descàrregues?",
	"factory-reset": "Reseteig de fàbrica",
	"factory-reset-confirm": "Estàs segur que vols resetejar de fàbrica?",
	"download-protocol": "Protocols",
	"protocols-default-client":
		"Establir com client per defecte dels següents protocols",
	"protocols-magnet": "Magnet [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc://, adcs://, dchub://, nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella://, gnet:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "Integració del dimoni giFT",
	"gift-enabled": "Activa la delegació a giFT",
	"gift-host": "Amfitrió de giFT",
	"gift-port": "Port de giFT",
	"gift-not-detected":
		"No s'ha detectat el dimoni giFT a l'amfitrió:port configurats.",
	"browser-extensions": "Extensions",
	"baidu-exporter": "BaiduExporter",
	"browser-extensions-tips": "Proporcionats per la comunitat, ",
	"baidu-exporter-help": "Fes click aquí per veure l'ús",
	"auto-update": "Actualitzar automàticament",
	"auto-check-update": "Revisar actualitzacions automàticament",
	"last-check-update-time": "Última revisió d'actualitzacions",
	"completion-script": "Script en finalitzar",
	"completion-script-enabled": "Executa un script en completar la baixada",
	"completion-script-tips":
		"S’executa en entorn aïllat amb tauri-plugin-shell. S’executa un cop al final de cada tasca (èxit o error).",
	"completion-script-command": "Ordre",
	"completion-script-command-placeholder":
		"p. ex. /usr/local/bin/notify.sh o C:\\Tools\\notify.exe",
	"completion-script-args": "Paràmetres",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Marcadors: {path}, {hash}, {status}. També disponibles com a variables d’entorn RISUKO_PATH, RISUKO_HASH i RISUKO_STATUS.",
	"completion-script-timeout": "Temps d’espera (ms)",
	"completion-script-test": "Provar script",
	"completion-script-test-ok": "La prova del script ha anat bé",
	"completion-script-test-fail": "La prova del script ha fallat",
	"completion-script-empty": "Indica una ordre primer",
	"completion-script-not-approved":
		"L’script de finalització no s’ha permès, així que no s’ha desat res.",
	"not-saved": "Preferències no desades",
	"not-saved-confirm":
		"Les preferències modificades es perdran, esteu segur que marxareu?",
	"saved-credentials": "Credencials desades",
	"saved-credentials-description":
		"Gestiona les credencials d'autenticació desades per a les descàrregues",
	"no-saved-credentials": "No hi ha credencials desades",
	"credential-host": "Amfitrió",
	"credential-label": "Etiqueta",
	"credential-protocol": "Protocol",
	"credential-last-used": "Darrer ús",
	"credential-delete": "Elimina la credencial",
	"credential-delete-confirm":
		"Estàs segur que vols eliminar aquesta credencial desada?",
	"vault-enabled": "Secrets desats al clauer del sistema",
	"vault-disabled":
		"Clauer del sistema no disponible: els secrets es desen en text pla",
	"credential-encrypted-badge": "Desat al clauer del sistema",
	"credential-vault-unavailable":
		"No es pot accedir al clauer del sistema; les credencials noves es desaran en text pla",
	"speed-limit-enabled": "Activa els límits de velocitat",
	"speed-limit-enabled-description":
		"Aplica els límits següents. Si està desactivat, les descàrregues van a tota velocitat; els valors es conserven.",
	"appearance-theme": "Tema",
	"appearance-typography": "Tipografia",
	"appearance-window": "Finestra",
	"task-list-style": "Llista de tasques",
	"task-list-style-description":
		"Com es mostren les descàrregues a la llista de tasques.",
	"task-list-style-compact": "Compacta",
	"task-list-style-card": "Targetes",
	"sidebar-collapsed": "Replega la barra lateral",
	"sidebar-collapsed-description":
		"Mostra només les icones a la barra lateral.",
};
