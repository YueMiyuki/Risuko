export default {
	basic: "Основа",
	advanced: "Разширени",
	cloudSinks: "Облачни дестинации",
	usenet: "Usenet",
	search: "Търсене",
	"search-placeholder": "Търсене във всички настройки",
	"search-all-tabs": "Всички настройки",
	"search-results": "Съвпадащи настройки: {{count}}",
	"search-no-results": "Не са намерени съвпадащи настройки",
	"search-clear": "Изчисти търсенето",
	"search-go-to-tab": "Отвори настройката",
	"search-more-results": "Уточнете търсенето, за да видите повече резултати",
	"usenet-tips":
		"Управление на NNTP доставчици и безопасни настройки по подразбиране за почистване на архиви.",
	"usenet-profiles": "Профили на доставчици",
	"usenet-profiles-tips":
		"Конфигурирайте приоритета, сигурността, удостоверяването и ограниченията за връзки на доставчика.",
	"usenet-add-profile": "Добави профил",
	"usenet-no-profiles": "Няма конфигурирани профили на доставчици.",
	"usenet-no-profiles-tips":
		"Добавете поне един доставчик, преди да импортирате NZB изтегляне.",
	"usenet-edit-profile": "Редактиране на профил",
	"usenet-profile-dialog-tips":
		"Настройките на профила могат да се синхронизират между устройствата. Данните за вход остават локални за това устройство.",
	"usenet-profile-dialog-tips-vault":
		"Настройките на профила могат да се синхронизират между устройствата. Данните за вход се пазят в защитеното хранилище за ключове на това устройство.",
	"usenet-profile-name": "Име на профила",
	"usenet-profile-name-placeholder": "Основен доставчик",
	"usenet-profile-host": "NNTP хост",
	"usenet-profile-port": "Порт",
	"usenet-security": "Сигурност на връзката",
	"usenet-tls": "Имплицитен TLS",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "Обикновен NNTP",
	"usenet-authentication": "Удостоверяване",
	"usenet-auth-credentials": "Потребителско име и парола",
	"usenet-auth-anonymous": "Анонимно",
	"usenet-username": "Потребителско име",
	"usenet-password": "Парола",
	"usenet-credentials-saved": "Данните за вход са запазени",
	"usenet-credentials-unavailable":
		"Състоянието на данните за вход не е налично",
	"usenet-credentials-saved-tips":
		"Данните за вход вече са запазени. Оставете и двете полета празни, за да ги запазите непроменени.",
	"usenet-plain-warning-title": "Нешифрована връзка",
	"usenet-plain-opt-in":
		"Разбирам и искам да разреша обикновен NNTP за този профил.",
	"usenet-plain-warning":
		"Обикновеният NNTP не шифрова данните за вход и изтегленото съдържание. Използвайте го само когато доставчикът ви изрично го изисква.",
	"usenet-enabled": "Включен",
	"usenet-enabled-tips":
		"Изключените профили се запазват, но не се използват за изтегляния.",
	"usenet-disabled": "Изключен",
	"usenet-priority": "Приоритет",
	"usenet-priority-tips":
		"По-ниските стойности се опитват първи. Равните приоритети си поделят натоварването.",
	"usenet-priority-value": "Приоритет {{value}}",
	"usenet-max-connections": "Максимален брой връзки",
	"usenet-max-connections-tips":
		"Ограничението на връзките за този профил на доставчик.",
	"usenet-connections-value": "Връзки: {{value}}",
	"usenet-test": "Тест",
	"usenet-edit": "Редактирай профила",
	"usenet-remove": "Премахни",
	"usenet-remove-title": "Да се премахне ли профилът на доставчика?",
	"usenet-remove-confirm":
		"{{name}} вече няма да е наличен за изтегляния. Запазените му данни за вход също ще бъдат премахнати от това устройство.",
	"usenet-profile-saved": "Профилът на доставчика е запазен.",
	"usenet-profile-removed": "{{name}} беше премахнат.",
	"usenet-test-success": "Свързано с {{name}}.",
	"usenet-test-failed": "Връзката е неуспешна: {{message}}",
	"usenet-validation-name": "Въведете име на профила.",
	"usenet-validation-host": "Въведете NNTP хост.",
	"usenet-validation-port": "Портът трябва да е между 1 и 65535.",
	"usenet-validation-plain":
		"Потвърдете предупреждението за обикновен NNTP, преди да запазите.",
	"usenet-validation-priority": "Приоритетът трябва да е нула или повече.",
	"usenet-validation-connections": "Връзките трябва да са между 1 и 128.",
	"usenet-validation-credentials":
		"Въведете потребителско име и парола или изберете анонимно удостоверяване.",
	"usenet-validation-credential-status":
		"Състоянието на данните за вход не е налично. Опитайте отново, преди да промените този профил.",
	"usenet-cleanup": "Почистване след успех",
	"usenet-cleanup-tips":
		"Почистването се изпълнява само след пълно сглобяване и успешно извличане.",
	"usenet-cleanup-action": "След потвърден успех",
	"usenet-cleanup-keep": "Запази всички входни файлове",
	"usenet-cleanup-keep-tips":
		"Запазва PAR2 файловете и всички томове на архива.",
	"usenet-cleanup-par2": "Изтрий PAR2 файловете",
	"usenet-cleanup-par2-tips":
		"Премахва файловете за паритет, но запазва изтеглените томове на архива.",
	"usenet-cleanup-all": "Изтрий PAR2 и томовете на архива",
	"usenet-cleanup-all-tips":
		"Премахва файловете за паритет и томовете на архива след успешно извличане.",
	"usenet-archive-safety": "Безопасност на архивите",
	"usenet-limits-defaults":
		"Ограниченията за цялата задача защитават хранилището и обработката по време на проверка и извличане.",
	"usenet-desktop-limits": "Ограничения за настолен компютър",
	"usenet-android-limits": "Ограничения за Android",
	"usenet-limits-summary":
		"Записи: {{entries}} · общо след разархивиране {{total}} · {{perEntry}} на запис",
	"usenet-limits-secondary-summary":
		"Съотношение {{ratio}}:1 · вложени нива: {{depth}} · резерв от свободно място {{reserve}} · активни часове: {{hours}}",
	"usenet-customize-limits": "Персонализирай ограниченията",
	"usenet-hide-limits": "Скрий ограниченията",
	"usenet-limits-adjusted": "Синхронизираните ограничения бяха коригирани",
	"usenet-limits-adjusted-tips":
		"Една или повече стойности надвишаваха твърдия таван на тази платформа и бяха намалени.",
	"usenet-max-entries": "Максимален брой записи",
	"usenet-max-expanded": "Общ размер след разархивиране",
	"usenet-max-entry": "Размер на отделен запис",
	"usenet-max-depth": "Дълбочина на вложеност",
	"usenet-max-ratio": "Степен на компресия",
	"usenet-free-space-reserve": "Резерв от свободно място",
	"usenet-max-active-time": "Активно време за обработка",
	"usenet-unit-entries": "записа",
	"usenet-unit-levels": "нива",
	"usenet-unit-hours": "часа",
	"usenet-limits-hard-cap":
		"Стойностите не могат да надвишават четири пъти стойностите по подразбиране за платформата. Замененията за отделна задача се потвърждават поотделно.",
	"usenet-restore-limits":
		"Възстанови стойностите по подразбиране за платформата",
	"usenet-limits-restored":
		"Ограниченията за архиви са възстановени до стойностите по подразбиране за платформата.",
	save: "Запазване и прилагане",
	"save-success-message": "настройките са запазени успешно",
	"save-fail-message": "грешка при запазване на настройките",
	discard: "Отказ",
	"history-directories": "Последни папки за изтегляне",
	"favorite-directory": "Любима папка",
	"unfavorite-directory": "Премахни папката от любимите",
	"remove-history-directory": "Премахни папката от историята",
	startup: "стартиране",
	"open-at-login":
		"стартиране на програмата заедно със стартирането на операционната система",
	"keep-window-state":
		"по време на затваряне на приложението, Запишете размера и позицията на прозореца",
	"auto-resume-all": "автоматично възобновяване на всички недовършени задачи",
	"default-dir": "път по подразбиране",
	"file-category-dirs": "Пътища по категории",
	"file-category-dirs-tips":
		"Заменете пътя за изтегляне по подразбиране за конкретни типове файлове. Оставете празно, за да се използва пътят по подразбиране.",
	"task-routing-rules": "Правила за маршрутизиране на задачи",
	"task-routing-rules-tips":
		"Насочва задачите към папки според шаблони в имената на файловете. Печели първото съвпадащо правило.",
	"task-routing-rule-pattern-placeholder": "Шаблон (напр. *.mkv)",
	"task-routing-rule-label-placeholder": "Етикет",
	"task-routing-rule-dir-placeholder": "Папка",
	"task-routing-rule-add": "Добави правило",
	"file-category-music": "Музика",
	"file-category-video": "Видео",
	"file-category-image": "Изображение",
	"file-category-document": "Документ",
	"file-category-compressed": "Компресирани",
	"file-category-program": "Програма",
	"mas-default-dir-tips":
		"поради ограничения в App Store, препоръчително е да зададете пътя по подразбиране като ~/Downloads",
	"transfer-settings": "Трансфер",
	"transfer-speed-upload": "лимит за качване",
	"transfer-speed-download": "лимит за изтегляне",
	"transfer-speed-unlimited": "Без ограничение",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Запазете магнитната връзка като торент файл",
	"bt-create-subfolder": "Групиране на торенти с много файлове в подпапка",
	"bt-create-subfolder-tips":
		"Когато е включено, BitTorrent и magnet изтеглянията с много файлове се поставят в папка с името на торента. Когато е изключено, файловете се записват директно в папката за изтегляне.",
	"bt-upload-rate-limit": "Лимит за качване при BitTorrent",
	"bt-upload-rate-limit-tips":
		"Ограничава качването през BitTorrent в допълнение към общия лимит за качване. 0 означава без отделно ограничение.",
	"bt-auto-download-content":
		"Автоматично изтегляне на магнит и торент съдържание",
	"bt-force-encryption": "BT задължително криптиране",
	"keep-seeding": "Продължавайте да засявате, докато не го спрете ръчно",
	"seed-ratio": "Съотношение при засяване",
	"seed-time": "Време на засяване",
	"seed-time-unit": "минути",
	"task-manage": "мениджър на задачи",
	"max-concurrent-downloads": "Максимум активни задачи",
	"connections-per-task": "Връзки на задача",
	"max-connection-per-server": "максимални връзки към сървъра",
	"auto-retry": "Автоматичен повторен опит за неуспешни задачи",
	"auto-retry-strategy": "Режим на интервала между опитите",
	"auto-retry-strategy-static": "Фиксиран",
	"auto-retry-strategy-exponential": "Експоненциално нарастване",
	"auto-retry-interval": "Базов интервал между опитите",
	"auto-retry-interval-unit": "секунди",
	"auto-retry-max-attempts":
		"Максимален брой повторни опити (0 = без ограничение)",
	"worker-max-retries": "Макс. повторни опити за парче на работник",
	"auto-detect-low-speed-tasks":
		"Автоматично откриване на задачи с ниска скорост",
	"auto-detect-low-speed-tasks-tips":
		"Ако работеща задача остане под прага на скоростта, тя се спира и рестартира автоматично.",
	"low-speed-threshold": "Праг на ниска скорост",
	"low-speed-threshold-unit": "KB/s",
	"http-reliability": "Надеждност на HTTP / мрежата",
	"http-reliability-tips":
		"Настройте времето за свързване и откриването на блокирал трансфер при HTTP/HTTPS/FTP изтегляния.",
	"connect-timeout": "Таймаут на свързване",
	"nzb-body-timeout": "Таймаут на тялото на NZB",
	"lowest-speed-limit": "Минимална скорост",
	"lowest-speed-limit-timeout": "Таймаут при блокиране",
	"lowest-speed-limit-help":
		"Ако изтеглянето остане под минималната скорост през времето на таймаута при блокиране, работникът се прекъсва и (ако автоматичният повторен опит е включен) се рестартира. Задайте 0, за да изключите.",
	"uri-selector": "Избор на огледало",
	"uri-selector-feedback":
		"Според резултата (отхвърля бавни/неработещи огледала)",
	"uri-selector-inorder": "По ред (огледалата се опитват отляво надясно)",
	"uri-selector-adaptive": "Адаптивно (предпочита най-бързото)",
	"unit-seconds": "секунди",
	"unit-kib-per-sec": "KiB/s",
	storage: "Съхранение",
	"storage-tips":
		"Как двигателят резервира дисково пространство преди изтеглянето.",
	"file-allocation": "Заделяне на файлове",
	"file-allocation-falloc": "Предварително заделяне (falloc)",
	"file-allocation-trunc": "Отрязване (само задаване на дължина)",
	"file-allocation-none": "Без (расте при нужда)",
	"file-allocation-tips":
		"Предварителното заделяне резервира реални дискови блокове авансово, за да се избегне ENOSPC по време на изтегляне и да се намали фрагментацията. При файлови системи без поддръжка се връща към отрязване.",
	"new-task-show-downloading": "автоматично показване на задача след добавяне",
	"no-confirm-before-delete-task":
		"Не се изисква потвърждение преди изтриване на задача",
	"auto-file-renaming": "Автоматично преименуване на дублиращи се файлове",
	"auto-file-renaming-tips":
		'Когато файл със същото име вече съществува, се запазва като "file.1.ext" вместо да бъде презаписан',
	"use-remote-file-time":
		"Използване на времето на промяна на отдалечения файл при запазване",
	"use-remote-file-time-tips":
		"Задава времето на промяна на файла според времевия печат от отдалечения сървър",
	continue: "Продължи",
	"task-completed-notify": "съобщение след изтеглянето",
	"clipboard-watch": "Следене на клипборда за връзки за изтегляне",
	"clipboard-watch-tips":
		"Когато копирате magnet връзка или връзка към файл където и да е, Risuko предлага да я изтегли.",
	"clipboard-watch-ext": "Следени файлови разширения",
	"clipboard-watch-ext-tips":
		"Само http(s) връзки, завършващи с тези разширения, задействат подканата. Magnet, ed2k и торент връзките винаги я задействат. Разделяйте с интервали или запетаи.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"Risuko вече следи клипборда ви за връзки за изтегляне. Изключете това от Настройки → Основа.",
	"prevent-sleep-while-downloading":
		"Предотвратяване на заспиване на системата по време на изтегляне",
	"prevent-sleep-while-downloading-tips":
		"Поддържайте компютъра буден, докато има активна задача",
	"shutdown-when-complete":
		"Изключване на компютъра, когато всички задачи приключат",
	"shutdown-when-complete-tips":
		"Еднократно: изключва компютъра, когато опашката за изтегляне се изпразни",
	"shutdown-countdown-title": "Изключване след {{seconds}} с…",
	"shutdown-countdown-cancel": "Отказ",
	"shutdown-failed": "Изключването е неуспешно. Проверете разрешенията.",
	"auto-purge-record":
		"автоматично почистване на записите за изтегляне след затваряне на приложението",
	"purge-record-on-start":
		"Изчистване на записите за изтегляния при стартиране на приложението",
	ui: "UI",
	appearance: "външен вид",
	"theme-auto": "автоматично",
	"theme-light": "Светло",
	"theme-dark": "Тъмно",
	"font-family": "Шрифт",
	"font-family-system": "Системен",
	"font-family-rounded": "Заоблен",
	"font-family-serif": "Серифен",
	"font-family-mono": "Моноширинен",
	"font-family-sample": "Бързата кафява лисица изтегля по-бързо.",
	"font-size": "Размер",
	"font-size-small": "Малък",
	"font-size-small-short": "Малък",
	"font-size-default": "По подразбиране",
	"font-size-default-short": "По подразбиране",
	"font-size-large": "Голям",
	"font-size-large-short": "Голям",
	"font-size-extra-large": "Много голям",
	"font-size-extra-large-short": "Много голям",
	"auto-hide-window": "Автоматично скриване на прозореца",
	"run-mode": "Стартиране като",
	"run-mode-standard": "стандартно приложение",
	"run-mode-tray": "Приложение в таванчето",
	"run-mode-hide-tray": "Скриване на приложение в таванчето",
	"tray-speedometer":
		"Таблата на лентата с менюта показва скорост в реално време",
	"show-progress-bar": "Показване на лента за напредък на изтеглянето",
	language: "Език",
	"change-language": "промяна на езика",
	"hide-app-menu":
		"Скриване на менюто на приложението (само за Windows и Linux)",
	proxy: "Proxy",
	"proxy-http-profile": "Прокси за HTTP / FTP / Usenet",
	"proxy-p2p-profile": "P2P прокси",
	"enable-proxy": "използване на Proxy",
	"enable-p2p-proxy": "Включване на P2P прокси",
	"proxy-p2p-tcp-server": "TCP прокси сървър",
	"proxy-p2p-tcp-bypass": "Изключения за TCP прокси",
	"proxy-p2p-udp-server": "UDP прокси сървър",
	"proxy-p2p-udp-server-placeholder":
		"По избор; оставете празно, за да се използва TCP P2P проксито",
	"proxy-p2p-udp-bypass": "Изключения за UDP прокси",
	"proxy-bypass-input-tips":
		"заобикаляне на настройките на прокси за тези хостове и домейни, един по един на ред",
	"proxy-scope-label": "Използва се за",
	"proxy-scope-download": "Изтегляне",
	"proxy-scope-download-desc":
		"Насочване на трафика за изтегляне през прокси сървъра",
	"proxy-scope-update-app": "Актуализиране на приложението",
	"proxy-scope-update-app-desc":
		"Проверка за актуализации на приложението през прокси сървъра",
	"proxy-scope-update-trackers": "Актуализиране на трекери",
	"proxy-scope-update-trackers-desc":
		"Изтегляне на списъци с BitTorrent тракери през прокси сървъра",
	"proxy-tips": "преглед на ръководството за прокси",
	"proxy-p2p-tips":
		"За DHT, UDP тракери и uTP се изисква SOCKS5 или SOCKS5H. HTTP проксита се използват само за TCP P2P връзки.",
	"proxy-p2p-udp-tips":
		"Оставете UDP проксито празно, за да се използва повторно маршрутът на TCP P2P. UDP проксирането изисква SOCKS5 или SOCKS5H; HTTP проксита не могат да пренасят UDP.",
	"proxy-p2p-restart-title": "Да се рестартират ли P2P изтеглянията?",
	"proxy-p2p-restart-confirm":
		"Промяната на P2P проксито рестартира активните peer-to-peer изтегляния. Изтеглянията през HTTP, FTP и Usenet продължават да работят.",
	doh: "DNS over HTTPS",
	"doh-tips":
		"Преобразуване на имена на хостове чрез шифрована DNS крайна точка вместо системния резолвър",
	"doh-enable": "Включване на DNS over HTTPS",
	"doh-provider": "Доставчик",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Персонализиран",
	"doh-url": "URL на крайната точка",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "DoH крайната точка трябва да започва с https://",
	"doh-bootstrap": "Bootstrap IP адреси",
	"doh-bootstrap-placeholder":
		"IP адреси за достъп до крайната точка, разделени със запетая (по избор)",
	"doh-fallback": "Връщане към системния DNS",
	"doh-fallback-tips":
		"Ако DoH заявка е неуспешна, имената се преобразуват чрез системния резолвър, вместо изтеглянето да се провали",
	"bt-tracker": "Tracker сървър",
	"bt-tracker-input-tips": "Tracker сървър, един на ред",
	"bt-tracker-tips": "препоръчително:",
	"bt-max-peers-per-torrent": "Макс. пиъри на торент",
	"bt-max-peers-per-torrent-tips":
		"Максимален брой едновременни връзки с пиъри на торент. По-високите стойности подобряват откриването в роя (изисква рестартиране на двигателя)",
	"bt-max-outstanding-per-peer": "Макс. чакащи заявки на пиър",
	"bt-max-outstanding-per-peer-tips":
		"Максимален брой едновременни заявки за блокове, изпращани конвейерно към всеки пиър (таван, до 500). 0 използва адаптивната стойност по подразбиране 6–500; бюджет на заявките за целия торент се споделя между активните пиъри. Изисква рестартиране на двигателя",
	"bt-max-connections": "Макс. връзки с пиъри",
	"bt-max-connections-tips":
		"Общ брой връзки с пиъри за всички торенти, от 20 до 5000. Намаляването му никога не прекъсва вече отворените връзки, а само задържа новите. В Linux, macOS и Android ефективната стойност е ограничена и до половината от лимита на системата за файлови дескриптори",
	"bt-ban-corrupt-peers": "Блокиране на пиъри, изпращащи повредени данни",
	"bt-ban-corrupt-peers-tips":
		"Блокира IP адрес до края на сесията, когато части, към чието изпращане е допринесъл, продължават да не издържат проверката на хеша. Много потребители могат да споделят един IP (мобилни оператори, споделени мрежи), така че блокирането може да отреже и невинни пиъри (изисква рестартиране на двигателя)",
	"bt-ban-corrupt-strikes": "Неуспешни части преди блокиране",
	"bt-ban-corrupt-strikes-tips":
		"С колко неуспешни части може да допринесе един IP адрес, преди да бъде блокиран. По-високите стойности допускат случайна повреда (изисква рестартиране на двигателя)",
	"bt-enable-upnp": "Пренасочване на портове чрез UPnP / NAT-PMP",
	"bt-enable-upnp-tips":
		"Автоматично отваряне на порта за слушане на BitTorrent на рутера чрез UPnP IGD, NAT-PMP или PCP (изисква рестартиране на двигателя)",
	"bt-upnp-lease": "Срок на съпоставяне на портове (секунди)",
	"bt-upnp-lease-tips":
		"Срок на валидност на съпоставянията на портове чрез UPnP и NAT-PMP. По-късите срокове се възстановяват по-бързо от остарели съпоставяния (изисква рестартиране на двигателя)",
	"bt-enable-lsd": "Локално откриване на услуги",
	"bt-enable-lsd-tips":
		"Откриване на пиъри в локалната мрежа чрез multicast по BEP-14 (изисква рестартиране на двигателя)",
	"bt-encryption-policy": "Шифроване на протокола",
	"bt-encryption-policy-tips":
		"Политика MSE/PE за връзките с пиъри. 'Предпочитай' опитва първо шифроване и се връща към обикновен текст (изисква рестартиране на двигателя)",
	"bt-encryption-plaintext": "Само обикновен текст",
	"bt-encryption-prefer": "Предпочитай шифроване",
	"bt-encryption-require": "Изисквай шифроване",
	"bt-listen-v6": "Слушане по IPv6",
	"bt-listen-v6-tips":
		"Свързване на допълнителен IPv6 TCP слушател на същия порт (изисква рестартиране на двигателя)",
	"sync-tracker-tips": "Синхронизация",
	"auto-sync-tracker":
		"актуализиране на списъка с тракери всеки ден автоматично",
	"ed2k-server": "eDonkey сървъри",
	"ed2k-server-input-tips": "eDonkey сървъри, по един на ред (ip:port)",
	"ed2k-server-tips": "Ако е празно, ще се използват сървърите по подразбиране",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Разрешаване на откриването на източници чрез Kad",
	"ed2k-enable-kad-tips":
		"Откриване на ED2K източници чрез мрежата eMule Kad (изисква рестартиране на двигателя)",
	"ed2k-kad-port": "UDP порт за Kad",
	"ed2k-kad-port-tips":
		"UDP порт, използван за откриване чрез Kad. Допустимите стойности са от 1 до 65535 (изисква рестартиране на двигателя)",
	"ed2k-kad-port-invalid":
		"UDP портът за Kad трябва да е цяло число от 1 до 65535",
	"listen-port-invalid":
		"BT портът трябва да е порт от 1 до 65535, диапазон като 6881-6889 или списък от такива, разделени със запетая",
	"dht-listen-port-invalid":
		"DHT портът трябва да е порт от 1 до 65535, диапазон като 6881-6889 или списък от такива, разделени със запетая",
	"ed2k-port-invalid": "ED2K портът трябва да е цяло число от 1 до 65535",
	"ed2k-enable-upload": "Споделяне на файлове чрез ED2K",
	"ed2k-enable-upload-tips":
		"Качване към други пиъри на завършени файлове и проверени части от текущи ED2K изтегляния. Kad може да показва файл, който вече не споделяте, още до 5 часа (изисква рестартиране на двигателя)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Данни за вход по подразбиране за изтегляния по FTP, FTPS и SFTP",
	"ftp-username": "Потребителско име за FTP",
	"ftp-password": "Парола за FTP",
	"sftp-private-key": "Път до частния SSH ключ",
	"sftp-private-key-content": "Съдържание на частния SSH ключ",
	"sftp-private-key-tips":
		"Път до файла с частен PEM ключ, напр. ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips":
		"Поставете тук съдържанието на частния PEM ключ",
	"sftp-key-passphrase": "Парола на ключа",
	"sftp-key-passphrase-tips": "Оставете празно, ако ключът няма парола",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Изходен формат",
	"media-settings": "Медийни сайтове (yt-dlp)",
	"media-settings-tips":
		"Настройки по подразбиране за изтегляния от медийни сайтове (YouTube, Vimeo, Bilibili и други). Изисква yt-dlp в PATH; ffmpeg е необходим за обединяване на отделни видео и аудио потоци.",
	"media-format": "Предпочитан формат",
	"media-format-placeholder":
		"напр. bestvideo+bestaudio/best (низ за формат на yt-dlp)",
	"media-format-tips":
		"Селектор на формат на yt-dlp. Оставете празно за най-добро качество. Примери: bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "портове за слушане",
	"bt-port": "Порт за слушане на BT",
	"dht-port": "DHT слушане Порт",
	"ed2k-port": "Порт за слушане на ed2k",
	security: "сигурност",
	rpc: "RPC",
	"rpc-host": "Адрес на сървъра",
	"rpc-host-local": "Само това устройство",
	"rpc-host-all": "Всички мрежови интерфейси",
	"rpc-host-custom": "Персонализиран ({{host}})",
	"rpc-host-exposed-tips":
		"Други устройства във вашата мрежа имат достъп до RPC порта и до порта на PeerBanHelper, когато е включен. Всеки, който знае секрета, може да добавя и управлява изтегляния.",
	"rpc-listen-port": "RPC слушащ порт",
	"rpc-secret": "RPC Secret",
	"rpc-secret-tips": "Гледайте инструкцията RPC Secret",
	"rpc-allow-any-dir":
		"Позволяване на RPC клиентите да ползват всеки локален път",
	"rpc-allow-any-dir-tips":
		"Когато е изключено, изтеглянията, добавени през RPC, могат да се запазват само в папката за изтегляне, нейните подпапки и вашите папки за категории и маршрутизиране, а RPC клиентите не могат да накарат приложението да чете файл с cookie, netrc или SSH ключ.",
	pbh: "PeerBanHelper",
	"pbh-enable": "Включване на PeerBanHelper API",
	"pbh-enable-tips":
		"Предоставя JSON-RPC слушател, съвместим с Aria2Next, за да може PeerBanHelper да следи и блокира BitTorrent пиъри",
	"pbh-listen-port": "Порт за слушане на PeerBanHelper",
	"pbh-rpc-secret": "RPC токен за PeerBanHelper",
	"generate-pbh-rpc-secret": "Генериране на нов токен",
	"pbh-endpoint-hint":
		"В PeerBanHelper добавете Aria2Next изтегляч, насочен към http://127.0.0.1:{{port}}/jsonrpc",
	"randomize-port": "Избери случаен порт",
	"generate-rpc-secret": "Генерирай нов RPC секрет",
	"copy-rpc-url": "Копирай RPC URL",
	"engine-overrides": "Заместващи настройки на двигателя (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Разширено: задайте произволни ключове за опциите на двигателя като JSON обект. Промените влизат в сила след рестарт.",
	"engine-overrides-invalid":
		"Заместващите настройки на двигателя трябва да са валиден JSON обект.",
	"engine-overrides-reserved-keys":
		"Игнорирани запазени ключове за заместващи настройки на двигателя: {{keys}}",
	"engine-overrides-too-large":
		"Заместващите настройки на двигателя са твърде големи: най-много 64 ключа, 64 знака на ключ и 2048 знака на стойност.",
	developer: "Разработчик",
	"user-agent": "User-Agent",
	"mock-user-agent": "оформление User-Agent",
	cookies: "Cookie",
	"cookies-tips":
		"Зареждане на cookie от файл cookies.txt във формат Netscape/Mozilla (приема и изход от curl/wget).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Запазени данни за домейни",
	"saved-cookies-tips":
		"Cookie и User-Agent, импортирани от браузър при заобикаляне на Cloudflare или други изтегляния със защитен достъп. Risuko ги използва повторно за нови изтегляния от същия домейн, докато изтекат или спрат да работят.",
	"saved-cookies-empty": "Още няма запазени данни.",
	"saved-cookies-host": "Домейн",
	"saved-cookies-browser": "Браузър",
	"saved-cookies-count": "Cookie",
	"saved-cookies-imported": "Импортирано",
	"saved-cookies-actions": "Действия",
	"saved-cookies-clear": "Изчисти всички",
	"saved-cookies-confirm-clear":
		"Да се изтрият ли всички запазени данни за домейни? Бъдещите изтегляния от тези сайтове може да се провалят, докато не импортирате отново.",
	"saved-cookies-delete": "Изтрий",
	netrc: "Данни за вход от .netrc",
	"netrc-tips":
		"Добавя HTTP Basic удостоверяване от файл .netrc, когато URL адресът няма вградени данни за вход. По подразбиране: ~/.netrc (или %USERPROFILE%/_netrc в Windows).",
	"netrc-path-placeholder": "Заместване на пътя до .netrc (по избор)",
	"no-netrc": "Изключване на търсенето в .netrc",
	"no-netrc-tips":
		"Пропуска .netrc изцяло. Полезно, когато файлът съдържа данни за вход, несвързани с изтегляния.",
	"app-log-path": "път към дневника на приложението",
	"log-dir-override-placeholder":
		"Собствена папка за журнали (оставете празно за подразбиране)",
	"log-dir-override-tips":
		"Журналите в момента се записват в {{path}}. Задаването на собствена папка (напр. в Изтегляния) ги прави видими във всеки файлов мениджър. Промените влизат в сила след следващото рестартиране на приложението.",
	"download-session-path": "Път на сесията за изтегляне",
	"session-reset": "Нулиране на сесията за изтегляне",
	"session-reset-confirm":
		"Сигурни ли сте, че искате да нулирате сесията за изтегляне?",
	"factory-reset": "Настройки по подразбиране",
	"factory-reset-confirm":
		"Сигурни ли сте, че искате да се върнете към настройките по подразбиране?",
	"download-protocol": "протоколи",
	"protocols-default-client":
		"Задаване като клиент по подразбиране за следните протоколи",
	"protocols-magnet": "Magnet [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc:// adcs:// dchub:// nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "Интеграция с демона giFT",
	"gift-enabled": "Активиране на делегирането към giFT",
	"gift-host": "Хост на giFT",
	"gift-port": "Порт на giFT",
	"gift-not-detected": "Демонът giFT не е открит на конфигурирания хост:порт.",
	"browser-extensions": "Разширения",
	"baidu-exporter": "BaiduExporter",
	"browser-extensions-tips": "предоставени от общността,",
	"baidu-exporter-help": "Кликнете тук, за да използвате",
	"auto-update": "автоматично обновяване",
	"auto-check-update": "автоматична проверка на актуализациите",
	"last-check-update-time": "последната актуализация е проверена",
	"completion-script": "Скрипт при завършване",
	"completion-script-enabled": "Изпълни скрипт при завършване на изтеглянето",
	"completion-script-tips":
		"Изпълнява се в изолирана среда чрез tauri-plugin-shell. Стартира веднъж при край на всяка задача (успех или грешка).",
	"completion-script-command": "Команда",
	"completion-script-command-placeholder":
		"напр. /usr/local/bin/notify.sh или C:\\Tools\\notify.exe",
	"completion-script-args": "Аргументи",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Плейсхолдери: {path}, {hash}, {status}. Достъпни са и като променливи на средата RISUKO_PATH, RISUKO_HASH, RISUKO_STATUS.",
	"completion-script-timeout": "Таймаут (ms)",
	"completion-script-test": "Тест на скрипта",
	"completion-script-test-ok": "Тестът на скрипта е успешен",
	"completion-script-test-fail": "Тестът на скрипта е неуспешен",
	"completion-script-empty": "Моля, задайте команда първо",
	"completion-script-not-approved":
		"Скриптът при завършване не беше разрешен, затова нищо не е запазено.",
	"not-saved": "Предпочитанията не са запазени",
	"not-saved-confirm":
		"Променените предпочитания ще бъдат загубени, сигурни ли сте, че ще напуснете?",
	"saved-credentials": "Запазени данни за вход",
	"saved-credentials-description":
		"Управление на запазените данни за удостоверяване за изтегляния",
	"no-saved-credentials": "Няма запазени данни за вход",
	"credential-host": "Хост",
	"credential-label": "Име",
	"credential-protocol": "Протокол",
	"credential-last-used": "Последно използвано",
	"credential-delete": "Изтрий данните",
	"credential-delete-confirm":
		"Сигурни ли сте, че искате да изтриете тези запазени данни?",
	"vault-enabled": "Тайните се съхраняват в хранилището за ключове на ОС",
	"vault-disabled":
		"Хранилището за ключове на ОС не е налично — тайните се съхраняват като обикновен текст",
	"credential-encrypted-badge": "Съхранено в хранилището за ключове на ОС",
	"credential-vault-unavailable":
		"Хранилището за ключове на ОС е недостъпно; новите данни за вход ще бъдат запазени като обикновен текст",
	"speed-limit-enabled": "Включване на ограниченията на скоростта",
	"speed-limit-enabled-description":
		"Прилага ограниченията по-долу. Когато е изключено, изтеглянията работят с пълна скорост; стойностите се запазват.",
	"appearance-theme": "Тема",
	"appearance-typography": "Типография",
	"appearance-window": "Прозорец",
	"task-list-style": "Списък със задачи",
	"task-list-style-description":
		"Как се показват изтеглянията в списъка със задачи.",
	"task-list-style-compact": "Компактен",
	"task-list-style-card": "Карти",
	"sidebar-collapsed": "Свиване на страничната лента",
	"sidebar-collapsed-description":
		"Показване само на икони в страничната лента.",
};
