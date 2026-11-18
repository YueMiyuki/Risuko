export default {
	basic: "Alap",
	advanced: "Haladó",
	cloudSinks: "Felhőcélok",
	usenet: "Usenet",
	search: "Keresés",
	"search-placeholder": "Keresés az összes beállításban",
	"search-all-tabs": "Összes beállítás",
	"search-results": "{{count}} egyező beállítás",
	"search-no-results": "Nem található egyező beállítás",
	"search-clear": "Keresés törlése",
	"search-go-to-tab": "Beállítás megnyitása",
	"search-more-results": "Pontosítsa a keresést, hogy több találatot lásson",
	"usenet-tips":
		"NNTP-szolgáltatók és a biztonságos archívumtakarítás alapértelmezéseinek kezelése.",
	"usenet-profiles": "Szolgáltatói profilok",
	"usenet-profiles-tips":
		"A szolgáltatók prioritásának, biztonságának, hitelesítésének és kapcsolati korlátainak beállítása.",
	"usenet-add-profile": "Profil hozzáadása",
	"usenet-no-profiles": "Nincs beállított szolgáltatói profil.",
	"usenet-no-profiles-tips":
		"NZB-letöltés importálása előtt adjon hozzá legalább egy szolgáltatót.",
	"usenet-edit-profile": "Profil szerkesztése",
	"usenet-profile-dialog-tips":
		"A profilbeállítások szinkronizálódhatnak az eszközök között. A hitelesítő adatok ezen az eszközön maradnak.",
	"usenet-profile-dialog-tips-vault":
		"A profilbeállítások szinkronizálódhatnak az eszközök között. A hitelesítő adatok az eszköz biztonságos kulcstartójában maradnak.",
	"usenet-profile-name": "Profil neve",
	"usenet-profile-name-placeholder": "Elsődleges szolgáltató",
	"usenet-profile-host": "NNTP-gazdagép",
	"usenet-profile-port": "Port",
	"usenet-security": "Kapcsolat biztonsága",
	"usenet-tls": "Implicit TLS",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "Titkosítatlan NNTP",
	"usenet-authentication": "Hitelesítés",
	"usenet-auth-credentials": "Felhasználónév és jelszó",
	"usenet-auth-anonymous": "Anonim",
	"usenet-username": "Felhasználónév",
	"usenet-password": "Jelszó",
	"usenet-credentials-saved": "Hitelesítő adatok mentve",
	"usenet-credentials-unavailable":
		"A hitelesítő adatok állapota nem érhető el",
	"usenet-credentials-saved-tips":
		"A hitelesítő adatok már el vannak mentve. Hagyja üresen mindkét mezőt, ha változatlanul meg szeretné tartani őket.",
	"usenet-plain-warning-title": "Titkosítatlan kapcsolat",
	"usenet-plain-opt-in":
		"Megértettem, és engedélyezni szeretném a titkosítatlan NNTP-t ennél a profilnál.",
	"usenet-plain-warning":
		"A titkosítatlan NNTP nem titkosítja a hitelesítő adatokat és a letöltött adatokat. Csak akkor használja, ha a szolgáltatója kifejezetten megköveteli.",
	"usenet-enabled": "Engedélyezve",
	"usenet-enabled-tips":
		"A letiltott profilok megmaradnak, de nem használjuk őket letöltésekhez.",
	"usenet-disabled": "Letiltva",
	"usenet-priority": "Prioritás",
	"usenet-priority-tips":
		"Az alacsonyabb értékű profilok kerülnek sorra először. Az azonos prioritásúak megosztoznak a terhelésen.",
	"usenet-priority-value": "Prioritás: {{value}}",
	"usenet-max-connections": "Maximális kapcsolatok",
	"usenet-max-connections-tips":
		"A kapcsolatok felső korlátja ennél a szolgáltatói profilnál.",
	"usenet-connections-value": "{{value}} kapcsolat",
	"usenet-test": "Tesztelés",
	"usenet-edit": "Profil szerkesztése",
	"usenet-remove": "Eltávolítás",
	"usenet-remove-title": "Eltávolítja a szolgáltatói profilt?",
	"usenet-remove-confirm":
		"A(z) {{name}} többé nem lesz elérhető letöltésekhez. A mentett hitelesítő adatai is el lesznek távolítva erről az eszközről.",
	"usenet-profile-saved": "A szolgáltatói profil mentve.",
	"usenet-profile-removed": "A(z) {{name}} eltávolítva.",
	"usenet-test-success": "Csatlakozva a(z) {{name}} szolgáltatóhoz.",
	"usenet-test-failed": "A csatlakozás sikertelen: {{message}}",
	"usenet-validation-name": "Adjon meg egy profilnevet.",
	"usenet-validation-host": "Adjon meg egy NNTP-gazdagépet.",
	"usenet-validation-port": "A portnak 1 és 65535 között kell lennie.",
	"usenet-validation-plain":
		"Mentés előtt erősítse meg a titkosítatlan NNTP-re vonatkozó figyelmeztetést.",
	"usenet-validation-priority":
		"A prioritásnak nullának vagy annál nagyobbnak kell lennie.",
	"usenet-validation-connections":
		"A kapcsolatok számának 1 és 128 között kell lennie.",
	"usenet-validation-credentials":
		"Adjon meg felhasználónevet és jelszót is, vagy válassza az anonim hitelesítést.",
	"usenet-validation-credential-status":
		"A hitelesítő adatok állapota nem érhető el. Próbálja újra, mielőtt módosítja ezt a profilt.",
	"usenet-cleanup": "Takarítás siker esetén",
	"usenet-cleanup-tips":
		"A takarítás csak a teljes összeállítás és a sikeres kicsomagolás után fut le.",
	"usenet-cleanup-action": "Ellenőrzött siker után",
	"usenet-cleanup-keep": "Minden bemenet megtartása",
	"usenet-cleanup-keep-tips":
		"Megtartja a PAR2-fájlokat és az összes archívumkötetet.",
	"usenet-cleanup-par2": "PAR2-fájlok törlése",
	"usenet-cleanup-par2-tips":
		"Eltávolítja a paritásfájlokat, de megtartja a letöltött archívumköteteket.",
	"usenet-cleanup-all": "PAR2-fájlok és archívumkötetek törlése",
	"usenet-cleanup-all-tips":
		"A sikeres kicsomagolás után eltávolítja a paritásfájlokat és az archívumköteteket.",
	"usenet-archive-safety": "Archívumbiztonság",
	"usenet-limits-defaults":
		"A feladatszintű korlátok védik a tárhelyet és a feldolgozást az ellenőrzés és a kicsomagolás során.",
	"usenet-desktop-limits": "Asztali korlátok",
	"usenet-android-limits": "Androidos korlátok",
	"usenet-limits-summary":
		"{{entries}} elem · {{total}} teljes kibontás · {{perEntry}} elemenként",
	"usenet-limits-secondary-summary":
		"{{ratio}}:1 arány · {{depth}} beágyazási szint · {{reserve}} szabad hely tartaléka · {{hours}} aktív óra",
	"usenet-customize-limits": "Korlátok testreszabása",
	"usenet-hide-limits": "Korlátok elrejtése",
	"usenet-limits-adjusted": "A szinkronizált korlátok módosultak",
	"usenet-limits-adjusted-tips":
		"Egy vagy több érték meghaladta a platform szigorú felső határát, ezért csökkentettük.",
	"usenet-max-entries": "Elemek maximális száma",
	"usenet-max-expanded": "Teljes kibontott méret",
	"usenet-max-entry": "Egyetlen elem mérete",
	"usenet-max-depth": "Beágyazási mélység",
	"usenet-max-ratio": "Tömörítési arány",
	"usenet-free-space-reserve": "Szabad hely tartaléka",
	"usenet-max-active-time": "Aktív feldolgozási idő",
	"usenet-unit-entries": "elem",
	"usenet-unit-levels": "szint",
	"usenet-unit-hours": "óra",
	"usenet-limits-hard-cap":
		"Az értékek nem haladhatják meg a platform alapértékeinek négyszeresét. A feladatonkénti felülbírálatokat külön kell megerősíteni.",
	"usenet-restore-limits": "Platform-alapértékek visszaállítása",
	"usenet-limits-restored":
		"Az archívumkorlátok visszaálltak a platform alapértékeire.",
	save: "Mentés és alkalmazás",
	"save-success-message": "Mentés sikeres volt",
	"save-fail-message": "Mentés sikertelen volt",
	discard: "Elvetés",
	"history-directories": "Legutóbbi letöltési könyvtárak",
	"favorite-directory": "Kedvenc könyvtár",
	"unfavorite-directory": "Könyvtár eltávolítása a kedvencekből",
	"remove-history-directory": "Könyvtár eltávolítása az előzményekből",
	startup: "Indítás",
	"open-at-login": "Megnyitás bejelenkezéskor",
	"keep-window-state": "Ablak méretének megtartása kilépéskor",
	"auto-resume-all": "Automatikusan folytassa a félbehagyott feladatokat",
	"default-dir": "Alap mappa",
	"file-category-dirs": "Kategóriamappák",
	"file-category-dirs-tips":
		"Az alapértelmezett letöltési mappa felülbírálása adott fájltípusokhoz. Hagyja üresen az alapértelmezett mappa használatához.",
	"task-routing-rules": "Feladatirányítási szabályok",
	"task-routing-rules-tips":
		"A feladatokat fájlnévminták alapján mappákba irányítja. Az első egyező szabály érvényesül.",
	"task-routing-rule-pattern-placeholder": "Minta (pl. *.mkv)",
	"task-routing-rule-label-placeholder": "Címke",
	"task-routing-rule-dir-placeholder": "Mappa",
	"task-routing-rule-add": "Szabály hozzáadása",
	"file-category-music": "Zene",
	"file-category-video": "Videó",
	"file-category-image": "Kép",
	"file-category-document": "Dokumentum",
	"file-category-compressed": "Tömörített",
	"file-category-program": "Program",
	"file-category-rss": "RSS",
	"mas-default-dir-tips":
		"Az App Store korlátozása miatt ajánlott a ~/Downloads mappába tenni a letöltéseket",
	"transfer-settings": "Transmission",
	"transfer-speed-upload": "Feltöltés limit",
	"transfer-speed-download": "Letöltés limit",
	"transfer-speed-unlimited": "Végtelen",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Mágneses link mentése torrent fájlként",
	"bt-create-subfolder": "A többfájlos torrentek csoportosítása almappába",
	"bt-create-subfolder-tips":
		"Bekapcsolva a többfájlos BitTorrent- és magnet-letöltések a torrent nevét viselő mappába kerülnek. Kikapcsolva a fájlok közvetlenül a letöltési mappába íródnak.",
	"bt-upload-rate-limit": "BitTorrent feltöltési korlát",
	"bt-upload-rate-limit-tips":
		"A globális feltöltési korláton felül a BitTorrent-feltöltéseket is korlátozza. A 0 azt jelenti, hogy nincs külön korlát.",
	"bt-auto-download-content":
		"A mágnes és a torrent tartalma automatikus letöltése",
	"bt-force-encryption": "BT kötelező titkosítása",
	"keep-seeding": "Addig folytassa a vetéset, amíg manuálisan le nem állítja",
	"seed-ratio": "Magarány",
	"seed-time": "Vetési idő",
	"seed-time-unit": "percek",
	"task-manage": "Feladatok kezelése",
	"max-concurrent-downloads": "Maximum feladatok",
	"connections-per-task": "Kapcsolatok feladatonként",
	"max-connection-per-server": "Maximum csatlakozás szerverenként",
	"auto-retry": "Sikertelen feladatok automatikus újrapróbálása",
	"auto-retry-strategy": "Újrapróbálási időköz módja",
	"auto-retry-strategy-static": "Állandó",
	"auto-retry-strategy-exponential": "Exponenciális visszalépés",
	"auto-retry-interval": "Alap újrapróbálási időköz",
	"auto-retry-interval-unit": "másodperc",
	"auto-retry-max-attempts": "Újrapróbálások maximális száma (0 = korlátlan)",
	"worker-max-retries": "Max. újrapróbálás darabonként (munkaszál)",
	"auto-detect-low-speed-tasks": "Lassú feladatok automatikus észlelése",
	"auto-detect-low-speed-tasks-tips":
		"Ha egy futó feladat a sebességküszöb alatt marad, automatikusan szünetelteti és újraindítja.",
	"low-speed-threshold": "Alacsony sebesség küszöbe",
	"low-speed-threshold-unit": "KB/s",
	"http-reliability": "HTTP / hálózati megbízhatóság",
	"http-reliability-tips":
		"A csatlakozási időtúllépés és az elakadt átvitelek észlelésének hangolása HTTP/HTTPS/FTP letöltésekhez.",
	"connect-timeout": "Csatlakozási időtúllépés",
	"nzb-body-timeout": "NZB-törzs időtúllépése",
	"lowest-speed-limit": "Legkisebb sebességhatár",
	"lowest-speed-limit-timeout": "Elakadási időtúllépés",
	"lowest-speed-limit-help":
		"Ha a letöltés az elakadási időtúllépés ideje alatt a legkisebb sebességhatár alatt marad, a munkaszál leáll, és (ha az automatikus újrapróbálás be van kapcsolva) újraindul. A kikapcsoláshoz állítsa a határt 0-ra.",
	"uri-selector": "Tükör kiválasztása",
	"uri-selector-feedback": "Visszajelzés (a lassú/hibás tükrök elvetése)",
	"uri-selector-inorder": "Sorrendben (a tükrök balról jobbra)",
	"uri-selector-adaptive": "Adaptív (a leggyorsabb előnyben)",
	"unit-seconds": "másodperc",
	"unit-kib-per-sec": "KiB/s",
	storage: "Tárhely",
	"storage-tips": "Hogyan foglal le a motor lemezterületet letöltés előtt.",
	"file-allocation": "Fájlfoglalás",
	"file-allocation-falloc": "Előfoglalás (falloc)",
	"file-allocation-trunc": "Csonkolás (csak a hossz beállítása)",
	"file-allocation-none": "Nincs (igény szerint nő)",
	"file-allocation-tips":
		"Az előfoglalás valódi lemezblokkokat foglal le előre, hogy elkerülje a letöltés közbeni ENOSPC hibát és csökkentse a töredezettséget. Ha a fájlrendszer nem támogatja, csonkolásra vált.",
	"new-task-show-downloading":
		"Automatikusan mutassa meg a letöltéseket a feladat hozzáadása után",
	"no-confirm-before-delete-task": "Ne legyen megerősítés a törlésnél",
	"auto-file-renaming": "Ismétlődő fájlok automatikus átnevezése",
	"auto-file-renaming-tips":
		'Ha már létezik azonos nevű fájl, mentés "file.1.ext" néven a felülírás helyett',
	"use-remote-file-time":
		"A távoli fájl módosítási idejének használata mentéskor",
	"use-remote-file-time-tips":
		"A fájl módosítási idejét a távoli kiszolgálótól kapott időbélyegre állítja",
	continue: "Folytatás",
	"task-completed-notify": "Értesítés a feladat befejezésnél",
	"clipboard-watch": "A vágólap figyelése letöltési linkekért",
	"clipboard-watch-tips":
		"Amikor bárhol másol egy magnet- vagy fájllinket, a risuko felajánlja a letöltését.",
	"clipboard-watch-ext": "Figyelt fájlkiterjesztések",
	"clipboard-watch-ext-tips":
		"Csak az ezekre a kiterjesztésekre végződő http(s) linkek váltják ki a kérdést. A magnet-, ed2k- és torrentlinkek mindig. Szóközzel vagy vesszővel válassza el őket.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"A risuko mostantól figyeli a vágólapot letöltési linkek után. Kikapcsolás: Beállítások → Alap.",
	"prevent-sleep-while-downloading":
		"A rendszer alvó módjának megakadályozása letöltés közben",
	"prevent-sleep-while-downloading-tips":
		"Tartsa ébren a számítógépet, amíg aktív feladat fut",
	"shutdown-when-complete":
		"A számítógép leállítása az összes feladat befejezésekor",
	"shutdown-when-complete-tips":
		"Egyszeri: leállítja a számítógépet, amint a letöltési sor kiürül",
	"shutdown-countdown-title": "Leállás {{seconds}} mp múlva…",
	"shutdown-countdown-cancel": "Mégse",
	"shutdown-failed": "A leállítás nem sikerült. Ellenőrizze a jogosultságokat.",
	"auto-purge-record": "Automatikusan tisztítsa a feladatokat kilépéskor",
	"purge-record-on-start":
		"Letöltési rekordok törlése az alkalmazás indításakor",
	ui: "UI",
	appearance: "Személyre szabás",
	"theme-auto": "Automatikus",
	"theme-light": "Világos",
	"theme-dark": "Sötét",
	"font-family": "Betűtípus",
	"font-family-system": "Rendszer",
	"font-family-rounded": "Lekerekített",
	"font-family-serif": "Talpas",
	"font-family-mono": "Fix szélességű",
	"font-family-sample": "A gyors barna róka gyorsabban tölt le.",
	"font-size": "Méret",
	"font-size-small": "Kicsi",
	"font-size-small-short": "Kicsi",
	"font-size-default": "Alapértelmezett",
	"font-size-default-short": "Alapértelmezett",
	"font-size-large": "Nagy",
	"font-size-large-short": "Nagy",
	"font-size-extra-large": "Nagyon nagy",
	"font-size-extra-large-short": "Nagyon nagy",
	"auto-hide-window": "Automatikusan rejtse el az ablakot",
	"run-mode": "Indítás néven...",
	"run-mode-standard": "Alap alkalmazás",
	"run-mode-tray": "Tálcás alkalmazás",
	"run-mode-hide-tray": "Tálca alkalmazás elrejtése",
	"tray-speedometer": "Menu csik mutasa a letöltés és feltöltés sebbeséget",
	"show-progress-bar": "Letöltési előrehaladási sáv megjelenítése",
	language: "Nyelv",
	"change-language": "Nyelv váltás",
	"hide-app-menu": "Alkalmazás csík elrejtése (csak Windows és Linux)",
	proxy: "Proxy",
	"proxy-http-profile": "HTTP / FTP / Usenet proxy",
	"proxy-p2p-profile": "P2P proxy",
	"enable-proxy": "Proxy engedélyezése",
	"enable-p2p-proxy": "P2P proxy engedélyezése",
	"proxy-p2p-tcp-server": "TCP proxykiszolgáló",
	"proxy-p2p-tcp-bypass": "TCP proxy kivételek",
	"proxy-p2p-udp-server": "UDP proxykiszolgáló",
	"proxy-p2p-udp-server-placeholder":
		"Opcionális; hagyja üresen a TCP P2P proxy használatához",
	"proxy-p2p-udp-bypass": "UDP proxy kivételek",
	"proxy-bypass-input-tips":
		"Proxy beállítások elvetése ezek url és domain-nél (/sor)",
	"proxy-scope-label": "Erre használva",
	"proxy-scope-download": "Letöltés",
	"proxy-scope-download-desc":
		"Letöltési forgalom irányítása a proxyn keresztül",
	"proxy-scope-update-app": "Alkalmazás frissítése",
	"proxy-scope-update-app-desc":
		"Alkalmazásfrissítések keresése a proxyn keresztül",
	"proxy-scope-update-trackers": "Nyomkövetők frissítése",
	"proxy-scope-update-trackers-desc":
		"BT trackerlisták lekérése a proxyn keresztül",
	"proxy-tips": "Proxy kézikönyv megnyitása",
	"proxy-p2p-tips":
		"A DHT-hoz, az UDP trackerekhez és az uTP-hez SOCKS5 vagy SOCKS5H szükséges. A HTTP proxykat csak TCP P2P kapcsolatokhoz használja.",
	"proxy-p2p-udp-tips":
		"Hagyja üresen az UDP proxyt a TCP P2P útvonal újrahasználatához. Az UDP-proxyzáshoz SOCKS5 vagy SOCKS5H szükséges; a HTTP proxyk nem tudnak UDP-t továbbítani.",
	"proxy-p2p-restart-title": "Újraindítja a P2P letöltéseket?",
	"proxy-p2p-restart-confirm":
		"A P2P proxy módosítása újraindítja az aktív peer-to-peer letöltéseket. A HTTP, FTP és Usenet letöltések tovább futnak.",
	doh: "DNS over HTTPS",
	"doh-tips":
		"A gazdagépnevek feloldása titkosított DNS-végponton keresztül a rendszer feloldója helyett",
	"doh-enable": "DNS over HTTPS engedélyezése",
	"doh-provider": "Szolgáltató",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Egyéni",
	"doh-url": "Végpont URL-je",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "A DoH-végpontnak https://-sel kell kezdődnie",
	"doh-bootstrap": "Bootstrap IP-címek",
	"doh-bootstrap-placeholder":
		"A végpont eléréséhez használt IP-címek, vesszővel elválasztva (opcionális)",
	"doh-fallback": "Visszaesés a rendszer DNS-ére",
	"doh-fallback-tips":
		"Ha a DoH-lekérdezés sikertelen, a letöltés meghiúsítása helyett a rendszer feloldójával oldja fel a nevet",
	"bt-tracker": "Lekövető szerverek",
	"bt-tracker-input-tips": "Lekövető szerverek (/sor)",
	"bt-tracker-tips": "Ajánlás: ",
	"bt-max-peers-per-torrent": "Max. peer torrentenként",
	"bt-max-peers-per-torrent-tips":
		"A peer-kapcsolatok maximális egyidejű száma torrentenként. A magasabb értékek javítják a raj felderítését (a motor újraindítása szükséges)",
	"bt-max-outstanding-per-peer": "Max. függő kérés peerenként",
	"bt-max-outstanding-per-peer-tips":
		"Az egyes peerek felé párhuzamosan küldhető darabkérések maximális száma (felső korlát, legfeljebb 500). A 0 az adaptív 6–500 alapértéket használja; a torrentre vonatkozó kérelemkeretet az aktív peerek osztják meg egymás között. A motor újraindítása szükséges",
	"bt-max-connections": "Max. peer-kapcsolat",
	"bt-max-connections-tips":
		"A peer-kapcsolatok teljes száma az összes torrenten, 20 és 5000 között. A csökkentése sosem szakítja meg a már nyitott kapcsolatokat, csak az újakat tartja vissza. Linuxon, macOS-en és Androidon a tényleges értéket a rendszer fájlleíró-korlátjának fele is korlátozza",
	"bt-ban-corrupt-peers": "Sérült adatot küldő peerek tiltása",
	"bt-ban-corrupt-peers-tips":
		"Egy IP-cím blokkolása a munkamenet hátralévő részére, miután az általa küldött darabok ismételten megbuknak a hash-ellenőrzésen. Sok felhasználó osztozhat egy IP-címen (mobilszolgáltatók, megosztott hálózatok), ezért a tiltás ártatlan peereket is kizárhat (a motor újraindítása szükséges)",
	"bt-ban-corrupt-strikes": "Sikertelen darabok a tiltás előtt",
	"bt-ban-corrupt-strikes-tips":
		"Hány sikertelen darabhoz járulhat hozzá egy IP-cím a tiltása előtt. A magasabb értékek elviselik az alkalmankénti sérülést (a motor újraindítása szükséges)",
	"bt-enable-upnp": "UPnP / NAT-PMP portátirányítás",
	"bt-enable-upnp-tips":
		"A BitTorrent figyelőportjának automatikus megnyitása a routeren UPnP IGD, NAT-PMP vagy PCP segítségével (a motor újraindítása szükséges)",
	"bt-upnp-lease": "Portleképezés bérleti ideje (másodperc)",
	"bt-upnp-lease-tips":
		"A UPnP és NAT-PMP portleképezések bérleti ideje. A rövidebb bérlet gyorsabban áll helyre az elavult leképezésekből (a motor újraindítása szükséges)",
	"bt-enable-lsd": "Helyi szolgáltatásfelderítés",
	"bt-enable-lsd-tips":
		"Peerek felderítése a helyi hálózaton BEP-14 multicast segítségével (a motor újraindítása szükséges)",
	"bt-encryption-policy": "Protokolltitkosítás",
	"bt-encryption-policy-tips":
		"MSE/PE házirend a peer-kapcsolatokhoz. Az előnyben részesítés először titkosítást próbál, majd titkosítatlanra vált (a motor újraindítása szükséges)",
	"bt-encryption-plaintext": "Csak titkosítatlan",
	"bt-encryption-prefer": "Titkosítás előnyben részesítése",
	"bt-encryption-require": "Titkosítás megkövetelése",
	"bt-listen-v6": "Figyelés IPv6-on",
	"bt-listen-v6-tips":
		"További IPv6 TCP figyelő kötése ugyanazon a porton (a motor újraindítása szükséges)",
	"sync-tracker-tips": "Szinkronizálás",
	"auto-sync-tracker": "Lekövető szerverek listája frissítése naponta",
	"ed2k-server": "eDonkey-kiszolgálók",
	"ed2k-server-input-tips": "eDonkey-kiszolgálók, soronként egy (ip:port)",
	"ed2k-server-tips":
		"Üresen hagyva az alapértelmezett kiszolgálókat használja",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Forráskeresés engedélyezése a Kad segítségével",
	"ed2k-enable-kad-tips":
		"ED2K-források keresése az eMule Kad hálózaton keresztül (a motor újraindítása szükséges)",
	"ed2k-kad-port": "Kad UDP-port",
	"ed2k-kad-port-tips":
		"A Kad-felderítéshez használt UDP-port. Az érvényes értékek 1 és 65535 között vannak (a motor újraindítása szükséges)",
	"ed2k-kad-port-invalid":
		"A Kad UDP-portjának 1 és 65535 közötti egész számnak kell lennie",
	"listen-port-invalid":
		"A BT-port lehet 1 és 65535 közötti port, tartomány (pl. 6881-6889) vagy ezek vesszővel elválasztott listája",
	"dht-listen-port-invalid":
		"A DHT-port lehet 1 és 65535 közötti port, tartomány (pl. 6881-6889) vagy ezek vesszővel elválasztott listája",
	"ed2k-port-invalid":
		"Az ED2K-portnak 1 és 65535 közötti egész számnak kell lennie",
	"ed2k-enable-upload": "Fájlok megosztása ED2K-n",
	"ed2k-enable-upload-tips":
		"A kész fájlok és a folyamatban lévő ED2K-letöltések ellenőrzött részeinek feltöltése más peereknek. A Kad még legfeljebb 5 óráig listázhatja a már nem megosztott fájlt (a motor újraindítása szükséges)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Alapértelmezett hitelesítő adatok FTP-, FTPS- és SFTP-letöltésekhez",
	"ftp-username": "FTP-felhasználónév",
	"ftp-password": "FTP-jelszó",
	"sftp-private-key": "SSH privát kulcs elérési útja",
	"sftp-private-key-content": "SSH privát kulcs tartalma",
	"sftp-private-key-tips":
		"A PEM privát kulcsfájl elérési útja, pl. ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips": "Illessze be ide a PEM privát kulcs tartalmát",
	"sftp-key-passphrase": "Kulcs jelmondata",
	"sftp-key-passphrase-tips": "Hagyja üresen, ha a kulcsnak nincs jelmondata",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Kimeneti formátum",
	"media-settings": "Médiaoldalak (yt-dlp)",
	"media-settings-tips":
		"A médiaoldalakról (YouTube, Vimeo, Bilibili és mások) való letöltés alapértelmezett beállításai. A yt-dlp-nek elérhetőnek kell lennie a PATH-ban; a külön videó- és hangfolyamok egyesítéséhez ffmpeg szükséges.",
	"media-format": "Előnyben részesített formátum",
	"media-format-placeholder":
		"pl. bestvideo+bestaudio/best (yt-dlp formátumkifejezés)",
	"media-format-tips":
		"yt-dlp formátumválasztó. Hagyja üresen a legjobb minőséghez. Példák: bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "Figyelőportok",
	"bt-port": "BT figyelőport",
	"dht-port": "DHT figyelőport",
	"ed2k-port": "ed2k figyelőport",
	security: "Biztonság",
	rpc: "RPC",
	"rpc-host": "Kiszolgáló címe",
	"rpc-host-local": "Csak ez az eszköz",
	"rpc-host-all": "Minden hálózati interfész",
	"rpc-host-custom": "Egyéni ({{host}})",
	"rpc-host-exposed-tips":
		"A hálózat többi eszköze is eléri az RPC-portot, és ha engedélyezve van, a PeerBanHelper portját is. Aki ismeri a titkot, letöltéseket adhat hozzá és vezérelhet.",
	"rpc-listen-port": "RPC-hallgató-port",
	"rpc-secret": "RPC-titok",
	"rpc-secret-tips": "RPC-titok kézikönyv megnyitása",
	"rpc-allow-any-dir":
		"Bármely helyi elérési út engedélyezése az RPC-klienseknek",
	"rpc-allow-any-dir-tips":
		"Kikapcsolva az RPC-n át hozzáadott letöltések csak a letöltési mappába, annak almappáiba, valamint a kategória- és irányítási mappákba menthetők, és az RPC-kliensek nem késztethetik az alkalmazást cookie-, netrc- vagy SSH-kulcsfájl beolvasására.",
	pbh: "PeerBanHelper",
	"pbh-enable": "PeerBanHelper API engedélyezése",
	"pbh-enable-tips":
		"Aria2Next-kompatibilis JSON-RPC figyelőt indít, hogy a PeerBanHelper figyelhesse és tilthassa a BitTorrent peer-eket",
	"pbh-listen-port": "PeerBanHelper figyelőport",
	"pbh-rpc-secret": "PeerBanHelper RPC token",
	"generate-pbh-rpc-secret": "Új token generálása",
	"pbh-endpoint-hint":
		"A PeerBanHelperben adjon hozzá egy Aria2Next letöltőt a következő címre: http://127.0.0.1:{{port}}/jsonrpc",
	"randomize-port": "Véletlen port választása",
	"generate-rpc-secret": "Új RPC-titok létrehozása",
	"copy-rpc-url": "RPC URL másolása",
	"engine-overrides": "Motorfelülbírálások (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Haladó: tetszőleges motorbeállítás-kulcsokat adhatsz meg JSON-objektumként. A módosítások újraindítás után lépnek életbe.",
	"engine-overrides-invalid":
		"A motorfelülbírálásoknak érvényes JSON-objektumnak kell lenniük.",
	"engine-overrides-reserved-keys":
		"Figyelmen kívül hagyott fenntartott motorfelülbírálási kulcsok: {{keys}}",
	"engine-overrides-too-large":
		"A motorfelülbírálások túl nagyok: legfeljebb 64 kulcs, kulcsonként 64 karakter és értékenként 2048 karakter engedélyezett.",
	developer: "fejlesztő",
	"user-agent": "User-Agent",
	"mock-user-agent": "Hamis User-Agent",
	cookies: "Sütik",
	"cookies-tips":
		"Sütik betöltése Netscape/Mozilla formátumú cookies.txt fájlból (a curl/wget kimenetét is elfogadja).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Mentett domain-hitelesítő adatok",
	"saved-cookies-tips":
		"A böngészőből importált sütik és User-Agent a Cloudflare vagy más hitelesítéssel védett letöltések megkerüléséhez. A Risuko ezeket újra felhasználja az ugyanazon a domainen indított új letöltésekhez, amíg le nem járnak vagy működésképtelenné nem válnak.",
	"saved-cookies-empty": "Még nincsenek mentett hitelesítő adatok.",
	"saved-cookies-host": "Domain",
	"saved-cookies-browser": "Böngésző",
	"saved-cookies-count": "Sütik",
	"saved-cookies-imported": "Importálva",
	"saved-cookies-actions": "Műveletek",
	"saved-cookies-clear": "Összes törlése",
	"saved-cookies-confirm-clear":
		"Törli az összes mentett domain-hitelesítő adatot? Az érintett oldalakról a későbbi letöltések sikertelenek lehetnek az újraimportálásig.",
	"saved-cookies-delete": "Törlés",
	netrc: ".netrc hitelesítő adatok",
	"netrc-tips":
		"HTTP Basic hitelesítés beszúrása .netrc fájlból, ha az URL nem tartalmaz beágyazott hitelesítő adatokat. Alapértelmezett: ~/.netrc (Windowson %USERPROFILE%/_netrc).",
	"netrc-path-placeholder": ".netrc elérési útjának felülbírálása (opcionális)",
	"no-netrc": ".netrc-keresés letiltása",
	"no-netrc-tips":
		"A .netrc teljes kihagyása. Hasznos, ha a fájl nem letöltési hitelesítő adatokat tartalmaz.",
	"app-log-path": "Alkalmazásnapló helye",
	"log-dir-override-placeholder":
		"Egyéni naplómappa (hagyja üresen az alapértelmezetthez)",
	"log-dir-override-tips":
		"A naplók jelenleg ide íródnak: {{path}}. Egyéni mappa megadásával (pl. a Letöltések mappán belül) bármely fájlkezelőben láthatóvá válnak. A módosítások az alkalmazás következő újraindítása után lépnek érvénybe.",
	"download-session-path": "Letöltés folyamat helye",
	"session-reset": "Letöltési munkamenet visszaállítása",
	"session-reset-confirm": "Biztosan visszaállítja a letöltési munkamenetet?",
	"factory-reset": "Gyári visszaállítás",
	"factory-reset-confirm": "Biztos vézel gyári viszaálitas?",
	"download-protocol": "Protokoll",
	"protocols-default-client": "Alap kliens Protokoll",
	"protocols-magnet": "Magnet [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc:// adcs:// dchub:// nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella://, gnet:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "giFT démon integráció",
	"gift-enabled": "giFT delegálás engedélyezése",
	"gift-host": "giFT gazdagép",
	"gift-port": "giFT port",
	"gift-not-detected":
		"A giFT démon nem található a beállított gazdagép:port címen.",
	"browser-extensions": "Bővítmény",
	"baidu-exporter": "BaiduExporter",
	"browser-extensions-tips": "A közösség biztosítja, ",
	"baidu-exporter-help": "Kattints ide a használati naplóhoz",
	"auto-update": "Automatikus frissítés",
	"auto-check-update": "Automatikusan keressen frissítéseket",
	"last-check-update-time": "Legutolsó frissítés:",
	"completion-script": "Befejezési szkript",
	"completion-script-enabled": "Szkript futtatása a letöltés befejezésekor",
	"completion-script-tips":
		"A tauri-plugin-shell sandboxában fut. Feladatonként egyszer fut le a végén (siker vagy hiba).",
	"completion-script-command": "Parancs",
	"completion-script-command-placeholder":
		"pl. /usr/local/bin/notify.sh vagy C:\\Tools\\notify.exe",
	"completion-script-args": "Argumentumok",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Helyőrzők: {path}, {hash}, {status}. RISUKO_PATH, RISUKO_HASH, RISUKO_STATUS környezeti változóként is elérhetők.",
	"completion-script-timeout": "Időtúllépés (ms)",
	"completion-script-test": "Szkript tesztelése",
	"completion-script-test-ok": "A szkript tesztje sikeres",
	"completion-script-test-fail": "A szkript tesztje sikertelen",
	"completion-script-empty": "Előbb adj meg egy parancsot",
	"completion-script-not-approved":
		"A befejezési szkriptet nem engedélyezték, ezért semmi sem lett mentve.",
	"not-saved": "A beállítások nincsenek mentve",
	"not-saved-confirm": "A megváltozott beállítások elvesznek. Biztosan kilép?",
	"saved-credentials": "Mentett hitelesítő adatok",
	"saved-credentials-description":
		"A letöltésekhez mentett hitelesítő adatok kezelése",
	"no-saved-credentials": "Nincsenek mentett hitelesítő adatok",
	"credential-host": "Gazdagép",
	"credential-label": "Címke",
	"credential-protocol": "Protokoll",
	"credential-last-used": "Utoljára használva",
	"credential-delete": "Hitelesítő adat törlése",
	"credential-delete-confirm":
		"Biztosan törli ezt a mentett hitelesítő adatot?",
	"vault-enabled":
		"A titkos adatok az operációs rendszer kulcstartójában vannak tárolva",
	"vault-disabled":
		"Az operációs rendszer kulcstartója nem érhető el — a titkos adatok egyszerű szövegként vannak tárolva",
	"credential-encrypted-badge": "Az operációs rendszer kulcstartójában tárolva",
	"credential-vault-unavailable":
		"Az operációs rendszer kulcstartója nem érhető el; az új hitelesítő adatok egyszerű szövegként lesznek mentve",
	"speed-limit-enabled": "Sebességkorlátok engedélyezése",
	"speed-limit-enabled-description":
		"Az alábbi korlátok alkalmazása. Kikapcsolva a letöltések teljes sebességgel futnak; az értékek megmaradnak.",
	"appearance-theme": "Téma",
	"appearance-typography": "Tipográfia",
	"appearance-window": "Ablak",
	"task-list-style": "Feladatlista",
	"task-list-style-description":
		"A letöltések megjelenítésének módja a feladatlistában.",
	"task-list-style-compact": "Kompakt",
	"task-list-style-card": "Kártyák",
	"sidebar-collapsed": "Oldalsáv összecsukása",
	"sidebar-collapsed-description": "Csak ikonok megjelenítése az oldalsávban.",
};
