export default {
	basic: "Основні",
	advanced: "Розширені",
	cloudSinks: "Хмарні призначення",
	usenet: "Usenet",
	search: "Пошук",
	"search-placeholder": "Шукати в усіх налаштуваннях",
	"search-all-tabs": "Усі налаштування",
	"search-results": "Відповідних налаштувань: {{count}}",
	"search-no-results": "Відповідних налаштувань не знайдено",
	"search-clear": "Очистити пошук",
	"search-go-to-tab": "Відкрити налаштування",
	"search-more-results": "Уточніть пошук, щоб побачити більше результатів",
	"usenet-tips":
		"Керуйте провайдерами NNTP і типовими параметрами безпечного очищення архівів.",
	"usenet-profiles": "Профілі провайдерів",
	"usenet-profiles-tips":
		"Налаштуйте пріоритет провайдера, безпеку, автентифікацію та обмеження кількості з'єднань.",
	"usenet-add-profile": "Додати профіль",
	"usenet-no-profiles": "Профілі провайдерів не налаштовано.",
	"usenet-no-profiles-tips":
		"Додайте принаймні одного провайдера, перш ніж імпортувати завантаження NZB.",
	"usenet-edit-profile": "Змінити профіль",
	"usenet-profile-dialog-tips":
		"Налаштування профілю можуть синхронізуватися між пристроями. Облікові дані залишаються лише на цьому пристрої.",
	"usenet-profile-dialog-tips-vault":
		"Налаштування профілю можуть синхронізуватися між пристроями. Облікові дані зберігаються у захищеній в'язці ключів цього пристрою.",
	"usenet-profile-name": "Назва профілю",
	"usenet-profile-name-placeholder": "Основний провайдер",
	"usenet-profile-host": "Хост NNTP",
	"usenet-profile-port": "Порт",
	"usenet-security": "Безпека з'єднання",
	"usenet-tls": "Неявний TLS",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "Звичайний NNTP",
	"usenet-authentication": "Автентифікація",
	"usenet-auth-credentials": "Ім'я користувача та пароль",
	"usenet-auth-anonymous": "Анонімно",
	"usenet-username": "Ім'я користувача",
	"usenet-password": "Пароль",
	"usenet-credentials-saved": "Облікові дані збережено",
	"usenet-credentials-unavailable": "Стан облікових даних недоступний",
	"usenet-credentials-saved-tips":
		"Облікові дані вже збережено. Залиште обидва поля порожніми, щоб не змінювати їх.",
	"usenet-plain-warning-title": "Незашифроване з'єднання",
	"usenet-plain-opt-in":
		"Я розумію ризики й хочу дозволити звичайний NNTP для цього профілю.",
	"usenet-plain-warning":
		"Звичайний NNTP не шифрує облікові дані та завантажені дані. Використовуйте його лише тоді, коли цього прямо вимагає ваш провайдер.",
	"usenet-enabled": "Увімкнено",
	"usenet-enabled-tips":
		"Вимкнені профілі зберігаються, але не використовуються для завантажень.",
	"usenet-disabled": "Вимкнено",
	"usenet-priority": "Пріоритет",
	"usenet-priority-tips":
		"Менші значення пробуються першими. Однакові пріоритети ділять навантаження.",
	"usenet-priority-value": "Пріоритет {{value}}",
	"usenet-max-connections": "Максимум з'єднань",
	"usenet-max-connections-tips":
		"Обмеження кількості з'єднань для цього профілю провайдера.",
	"usenet-connections-value": "З'єднань: {{value}}",
	"usenet-test": "Перевірити",
	"usenet-edit": "Змінити профіль",
	"usenet-remove": "Видалити",
	"usenet-remove-title": "Видалити профіль провайдера?",
	"usenet-remove-confirm":
		"{{name}} більше не буде доступний для завантажень. Його збережені облікові дані також буде видалено з цього пристрою.",
	"usenet-profile-saved": "Профіль провайдера збережено.",
	"usenet-profile-removed": "{{name}} видалено.",
	"usenet-test-success": "Підключено до {{name}}.",
	"usenet-test-failed": "Не вдалося підключитися: {{message}}",
	"usenet-validation-name": "Введіть назву профілю.",
	"usenet-validation-host": "Введіть хост NNTP.",
	"usenet-validation-port": "Порт має бути від 1 до 65535.",
	"usenet-validation-plain":
		"Перед збереженням підтвердьте попередження про звичайний NNTP.",
	"usenet-validation-priority": "Пріоритет має бути нулем або більше.",
	"usenet-validation-connections": "Кількість з'єднань має бути від 1 до 128.",
	"usenet-validation-credentials":
		"Введіть ім'я користувача й пароль або виберіть анонімну автентифікацію.",
	"usenet-validation-credential-status":
		"Стан облікових даних недоступний. Спробуйте ще раз, перш ніж змінювати цей профіль.",
	"usenet-cleanup": "Очищення після успіху",
	"usenet-cleanup-tips":
		"Очищення виконується лише після повного складання та успішного видобування.",
	"usenet-cleanup-action": "Після підтвердженого успіху",
	"usenet-cleanup-keep": "Залишити всі вхідні файли",
	"usenet-cleanup-keep-tips": "Зберігає файли PAR2 і всі томи архівів.",
	"usenet-cleanup-par2": "Видалити файли PAR2",
	"usenet-cleanup-par2-tips":
		"Видаляє файли парності, але зберігає завантажені томи архівів.",
	"usenet-cleanup-all": "Видалити PAR2 і томи архівів",
	"usenet-cleanup-all-tips":
		"Видаляє файли парності та томи архівів після успішного видобування.",
	"usenet-archive-safety": "Безпека архівів",
	"usenet-limits-defaults":
		"Обмеження на рівні завдання захищають сховище та ресурси під час перевірки й видобування.",
	"usenet-desktop-limits": "Обмеження для комп'ютера",
	"usenet-android-limits": "Обмеження для Android",
	"usenet-limits-summary":
		"Записів: {{entries}} · {{total}} загального розпакування · {{perEntry}} на запис",
	"usenet-limits-secondary-summary":
		"Коефіцієнт {{ratio}}:1 · рівнів вкладеності: {{depth}} · резерв вільного місця {{reserve}} · активних годин: {{hours}}",
	"usenet-customize-limits": "Налаштувати обмеження",
	"usenet-hide-limits": "Сховати обмеження",
	"usenet-limits-adjusted": "Синхронізовані обмеження було скориговано",
	"usenet-limits-adjusted-tips":
		"Одне чи кілька значень перевищували жорстку межу цієї платформи й були зменшені.",
	"usenet-max-entries": "Максимум записів",
	"usenet-max-expanded": "Загальний розпакований розмір",
	"usenet-max-entry": "Розмір окремого запису",
	"usenet-max-depth": "Глибина вкладеності",
	"usenet-max-ratio": "Коефіцієнт стиснення",
	"usenet-free-space-reserve": "Резерв вільного місця",
	"usenet-max-active-time": "Час активної обробки",
	"usenet-unit-entries": "записів",
	"usenet-unit-levels": "рівнів",
	"usenet-unit-hours": "годин",
	"usenet-limits-hard-cap":
		"Значення не можуть перевищувати чотирикратних типових значень платформи. Перевизначення для окремих завдань підтверджуються окремо.",
	"usenet-restore-limits": "Відновити типові значення платформи",
	"usenet-limits-restored":
		"Обмеження для архівів відновлено до типових значень платформи.",
	save: "Зберегти та Застосувати",
	"save-success-message": "Налаштування збережено успішно",
	"save-fail-message": "Помилка при зберіганні налаштувань",
	discard: "Відмінити",
	"history-directories": "Останні каталоги завантажень",
	"favorite-directory": "Улюблений каталог",
	"unfavorite-directory": "Видалити каталог з улюблених",
	"remove-history-directory": "Видалити каталог з історії",
	startup: "Запуск",
	"open-at-login": "Запускати програму разом з запуском операційної системи",
	"keep-window-state":
		"Під час закриття додатка, зберігати розмір і положення вікна",
	"auto-resume-all": "Автоматично поновлювати всі невиконані завдання",
	"default-dir": "Шлях за замовчуванням",
	"file-category-dirs": "Шляхи за категоріями",
	"file-category-dirs-tips":
		"Перевизначте шлях завантаження за замовчуванням для певних типів файлів. Залиште порожнім, щоб використовувати шлях за замовчуванням.",
	"task-routing-rules": "Правила маршрутизації завдань",
	"task-routing-rules-tips":
		"Спрямовує завдання до каталогів за шаблонами імен файлів. Перше відповідне правило має пріоритет.",
	"task-routing-rule-pattern-placeholder": "Шаблон (напр. *.mkv)",
	"task-routing-rule-label-placeholder": "Тег",
	"task-routing-rule-dir-placeholder": "Каталог",
	"task-routing-rule-add": "Додати правило",
	"file-category-music": "Музика",
	"file-category-video": "Відео",
	"file-category-image": "Зображення",
	"file-category-document": "Документ",
	"file-category-compressed": "Архіви",
	"file-category-program": "Програма",
	"mas-default-dir-tips":
		"Через обмеження в App Store, рекомендовано встановити шлях за замовчуванням ~/Downloads",
	"transfer-settings": "Передача",
	"transfer-speed-upload": "Ліміт вивантаження",
	"transfer-speed-download": "Ліміт завантаження",
	"transfer-speed-unlimited": "Безлімітно",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Зберегти магнітне посилання як торрент-файл",
	"bt-create-subfolder": "Групувати багатофайлові торренти в підтеку",
	"bt-create-subfolder-tips":
		"Якщо ввімкнено, багатофайлові завантаження BitTorrent і magnet розміщуються в теці з назвою торрента. Якщо вимкнено, файли записуються безпосередньо в каталог завантаження.",
	"bt-upload-rate-limit": "Ліміт вивантаження BitTorrent",
	"bt-upload-rate-limit-tips":
		"Обмежує вивантаження BitTorrent додатково до загального ліміту вивантаження. 0 означає окремого обмеження немає.",
	"bt-auto-download-content":
		"Автоматично завантажуйте вміст магніту та торрент",
	"bt-force-encryption": "Обов'язкова криптографія BT",
	"keep-seeding": "Продовжувати роздачу, поки не зупинено вручну",
	"seed-ratio": "Співвідношення роздачі",
	"seed-time": "Час роздачі",
	"seed-time-unit": "хвилин",
	"task-manage": "Менеджер завдань",
	"max-concurrent-downloads": "Максимум активних завдань",
	"connections-per-task": "З'єднань на завдання",
	"max-connection-per-server": "Максимум з'єднання на сервер",
	"auto-retry": "Автоматично повторювати невдалі завдання",
	"auto-retry-strategy": "Режим інтервалу повторів",
	"auto-retry-strategy-static": "Фіксований",
	"auto-retry-strategy-exponential": "Експоненційна затримка",
	"auto-retry-interval": "Базовий інтервал повторів",
	"auto-retry-interval-unit": "секунд",
	"auto-retry-max-attempts": "Максимум повторних спроб (0 = без обмеження)",
	"worker-max-retries": "Макс. повторів на шматок для одного потоку",
	"auto-detect-low-speed-tasks": "Автоматично виявляти повільні завдання",
	"auto-detect-low-speed-tasks-tips":
		"Якщо активне завдання залишається нижче порогу швидкості, воно автоматично призупиняється й перезапускається.",
	"low-speed-threshold": "Поріг низької швидкості",
	"low-speed-threshold-unit": "КБ/с",
	"http-reliability": "Надійність HTTP / мережі",
	"http-reliability-tips":
		"Налаштуйте тайм-аут підключення та виявлення зупинених передавань для завантажень HTTP/HTTPS/FTP.",
	"connect-timeout": "Тайм-аут підключення",
	"nzb-body-timeout": "Тайм-аут тіла NZB",
	"lowest-speed-limit": "Мінімальна швидкість",
	"lowest-speed-limit-timeout": "Тайм-аут зупинки",
	"lowest-speed-limit-help":
		"Якщо швидкість завантаження залишається нижче мінімальної протягом тайм-ауту зупинки, потік переривається та (якщо ввімкнено автоповтор) перезапускається. Задайте 0, щоб вимкнути.",
	"uri-selector": "Вибір дзеркала",
	"uri-selector-feedback":
		"Зворотний зв'язок (відкидати повільні/збійні дзеркала)",
	"uri-selector-inorder": "За порядком (дзеркала зліва направо)",
	"uri-selector-adaptive": "Адаптивно (надавати перевагу найшвидшому)",
	"unit-seconds": "секунд",
	"unit-kib-per-sec": "КіБ/с",
	storage: "Сховище",
	"storage-tips": "Як двигун резервує місце на диску перед завантаженням.",
	"file-allocation": "Виділення місця під файл",
	"file-allocation-falloc": "Попереднє виділення (falloc)",
	"file-allocation-trunc": "Обрізання (лише задати довжину)",
	"file-allocation-none": "Без виділення (зростає за потреби)",
	"file-allocation-tips":
		"Попереднє виділення одразу резервує реальні блоки диска, щоб уникнути ENOSPC посеред завантаження та зменшити фрагментацію. На файлових системах без підтримки використовується обрізання.",
	"new-task-show-downloading":
		"Автоматично відображати завантаження після додавання завдання",
	"no-confirm-before-delete-task":
		"Перед видаленням завдання не потрібно підтверджувати",
	"auto-file-renaming": "Автоматично перейменовувати дублікати файлів",
	"auto-file-renaming-tips":
		'Якщо файл з такою самою назвою вже існує, зберегти як "file.1.ext" замість перезапису',
	"use-remote-file-time":
		"Використовувати час зміни віддаленого файлу під час збереження",
	"use-remote-file-time-tips":
		"Задати час зміни файлу за позначкою часу з віддаленого сервера",
	continue: "Продовжити",
	"task-completed-notify": "Повідомлення після завершення завантаження",
	"clipboard-watch":
		"Стежити за буфером обміну на наявність посилань для завантаження",
	"clipboard-watch-tips":
		"Коли ви копіюєте magnet-посилання або посилання на файл будь-де, Risuko пропонує його завантажити.",
	"clipboard-watch-ext": "Відстежувані розширення файлів",
	"clipboard-watch-ext-tips":
		"Запит викликають лише посилання http(s), що закінчуються цими розширеннями. Посилання magnet, ed2k і torrent викликають його завжди. Розділяйте пробілами або комами.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"Тепер Risuko стежить за буфером обміну на наявність посилань для завантаження. Вимкніть це в розділі Налаштування → Основні.",
	"prevent-sleep-while-downloading":
		"Запобігати переходу системи в сплячий режим під час завантаження",
	"prevent-sleep-while-downloading-tips":
		"Тримати комп’ютер пробудженим, поки триває активне завдання",
	"shutdown-when-complete": "Вимкнути комп'ютер після завершення всіх завдань",
	"shutdown-when-complete-tips":
		"Одноразово: вимикає комп'ютер, коли черга завантажень спорожніє",
	"shutdown-countdown-title": "Вимкнення через {{seconds}} с…",
	"shutdown-countdown-cancel": "Скасувати",
	"shutdown-failed": "Не вдалося вимкнути. Перевірте дозволи.",
	"auto-purge-record":
		"Автоматично чистити записи про завантаження перед закриттям додатка",
	"purge-record-on-start":
		"Очищати записи про завантаження під час запуску застосунку",
	ui: "UI",
	appearance: "Зовнішній вигляд",
	"theme-auto": "Автоматично",
	"theme-light": "Світлий",
	"theme-dark": "Темний",
	"font-family": "Шрифт",
	"font-family-system": "Системний",
	"font-family-rounded": "Округлений",
	"font-family-serif": "Із засічками",
	"font-family-mono": "Моноширинний",
	"font-family-sample": "Швидка бура лисиця завантажує швидше.",
	"font-size": "Розмір",
	"font-size-small": "Малий",
	"font-size-small-short": "Малий",
	"font-size-default": "За замовчуванням",
	"font-size-default-short": "За замовчуванням",
	"font-size-large": "Великий",
	"font-size-large-short": "Великий",
	"font-size-extra-large": "Дуже великий",
	"font-size-extra-large-short": "Дуже великий",
	"auto-hide-window": "Автоприховування вікна",
	"run-mode": "Виконати як",
	"run-mode-standard": "Стандартне застосування",
	"run-mode-tray": "Додаток у лотку",
	"run-mode-hide-tray": "Приховати програму лотка",
	"tray-speedometer":
		"Лоток панелі меню показує швидкість у режимі реального часу",
	"show-progress-bar": "Показати панель завантаження",
	language: "Мова",
	"change-language": "Змінити мову",
	"hide-app-menu": "Сховати меню додатка (Тільки для Windows та Linux)",
	proxy: "Proxy",
	"proxy-http-profile": "Проксі для HTTP / FTP / Usenet",
	"proxy-p2p-profile": "Проксі для P2P",
	"enable-proxy": "Застосувати Proxy",
	"enable-p2p-proxy": "Увімкнути проксі для P2P",
	"proxy-p2p-tcp-server": "Сервер TCP-проксі",
	"proxy-p2p-tcp-bypass": "Обхід TCP-проксі",
	"proxy-p2p-udp-server": "Сервер UDP-проксі",
	"proxy-p2p-udp-server-placeholder":
		"Необов'язково; залиште порожнім, щоб використовувати TCP-проксі для P2P",
	"proxy-p2p-udp-bypass": "Обхід UDP-проксі",
	"proxy-bypass-input-tips":
		"Обхід налаштувань проксі для цих хостів та доменів, по одному на рядок",
	"proxy-scope-label": "Використовується для",
	"proxy-scope-download": "Завантажити",
	"proxy-scope-download-desc": "Спрямовувати трафік завантажень через проксі",
	"proxy-scope-update-app": "Оновити додаток",
	"proxy-scope-update-app-desc": "Перевіряти оновлення застосунку через проксі",
	"proxy-scope-update-trackers": "Оновити трекери",
	"proxy-scope-update-trackers-desc":
		"Отримувати списки BT-трекерів через проксі",
	"proxy-tips": "Перегляньте посібник з проксі",
	"proxy-p2p-tips":
		"Для DHT, UDP-трекерів і uTP потрібен SOCKS5 або SOCKS5H. HTTP-проксі використовуються лише для TCP-з'єднань P2P.",
	"proxy-p2p-udp-tips":
		"Залиште UDP-проксі порожнім, щоб повторно використати маршрут TCP для P2P. Для UDP-проксі потрібен SOCKS5 або SOCKS5H; HTTP-проксі не можуть передавати UDP.",
	"proxy-p2p-restart-title": "Перезапустити завантаження P2P?",
	"proxy-p2p-restart-confirm":
		"Зміна проксі для P2P перезапускає активні завантаження між пірами. Завантаження HTTP, FTP і Usenet продовжують працювати.",
	doh: "DNS через HTTPS",
	"doh-tips":
		"Визначати імена хостів через зашифровану точку DNS замість системного резолвера",
	"doh-enable": "Увімкнути DNS через HTTPS",
	"doh-provider": "Провайдер",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Власний",
	"doh-url": "URL кінцевої точки",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "Кінцева точка DoH має починатися з https://",
	"doh-bootstrap": "Початкові IP",
	"doh-bootstrap-placeholder":
		"IP для доступу до кінцевої точки, через кому (необов'язково)",
	"doh-fallback": "Переходити на системний DNS",
	"doh-fallback-tips":
		"Якщо запит DoH не вдався, визначати ім'я через системний резолвер замість збою завантаження",
	"bt-tracker": "Tracker Сервер",
	"bt-tracker-input-tips": "Tracker сервера, один в рядок",
	"bt-tracker-tips": "Рекомендовано: ",
	"bt-max-peers-per-torrent": "Макс. пірів на торрент",
	"bt-max-peers-per-torrent-tips":
		"Максимальна кількість одночасних з'єднань із пірами на торрент. Вищі значення покращують пошук у рої (потрібен перезапуск двигуна)",
	"bt-max-outstanding-per-peer": "Макс. невиконаних запитів на пір",
	"bt-max-outstanding-per-peer-tips":
		"Максимальна кількість одночасних запитів фрагментів, що надсилаються конвеєром кожному піру (обмеження, до 500). 0 використовує адаптивне значення за замовчуванням 6–500; загальний бюджет запитів торрента розподіляється між активними пірами. Потрібен перезапуск двигуна",
	"bt-max-connections": "Макс. з'єднань із пірами",
	"bt-max-connections-tips":
		"Загальна кількість з'єднань із пірами для всіх торрентів, від 20 до 5000. Зменшення ніколи не розриває вже відкриті з'єднання, а лише стримує нові. У Linux, macOS і Android фактичне значення також обмежується половиною системного ліміту файлових дескрипторів",
	"bt-ban-corrupt-peers": "Блокувати пірів, що надсилають пошкоджені дані",
	"bt-ban-corrupt-peers-tips":
		"Блокувати IP-адресу до кінця сеансу, коли шматки, до надсилання яких вона долучилася, постійно не проходять перевірку хешу. Одну IP-адресу можуть ділити багато користувачів (мобільні оператори, спільні мережі), тож блокування може відсікти й невинних пірів (потрібен перезапуск двигуна)",
	"bt-ban-corrupt-strikes": "Невдалих шматків до блокування",
	"bt-ban-corrupt-strikes-tips":
		"Скільком невдалим шматкам може сприяти IP-адреса, перш ніж її буде заблоковано. Вищі значення допускають випадкові пошкодження (потрібен перезапуск двигуна)",
	"bt-enable-upnp": "Переадресація портів UPnP / NAT-PMP",
	"bt-enable-upnp-tips":
		"Автоматично відкривати порт прослуховування BitTorrent на маршрутизаторі через UPnP IGD, NAT-PMP або PCP (потрібен перезапуск двигуна)",
	"bt-upnp-lease": "Строк оренди відображення портів (секунд)",
	"bt-upnp-lease-tips":
		"Тривалість оренди відображень портів UPnP і NAT-PMP. Коротша оренда швидше відновлюється після застарілих відображень (потрібен перезапуск двигуна)",
	"bt-enable-lsd": "Виявлення локальних служб",
	"bt-enable-lsd-tips":
		"Виявляти пірів у локальній мережі через мультикаст BEP-14 (потрібен перезапуск двигуна)",
	"bt-encryption-policy": "Шифрування протоколу",
	"bt-encryption-policy-tips":
		"Політика MSE/PE для з'єднань із пірами. Варіант «Надавати перевагу» спочатку пробує шифрування, а потім переходить на відкритий текст (потрібен перезапуск двигуна)",
	"bt-encryption-plaintext": "Лише відкритий текст",
	"bt-encryption-prefer": "Надавати перевагу шифруванню",
	"bt-encryption-require": "Вимагати шифрування",
	"bt-listen-v6": "Прослуховувати IPv6",
	"bt-listen-v6-tips":
		"Прив'язати додатковий TCP-слухач IPv6 до того самого порту (потрібен перезапуск двигуна)",
	"sync-tracker-tips": "Синхронізуватись",
	"auto-sync-tracker": "Щодня оновлюйте список трекерів автоматично",
	"ed2k-server": "Сервери eDonkey",
	"ed2k-server-input-tips": "Сервери eDonkey, по одному в рядку (ip:port)",
	"ed2k-server-tips":
		"Якщо порожньо, використовуватимуться сервери за замовчуванням",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Увімкнути пошук джерел через Kad",
	"ed2k-enable-kad-tips":
		"Пошук джерел ED2K через мережу eMule Kad (потрібен перезапуск двигуна)",
	"ed2k-kad-port": "UDP-порт Kad",
	"ed2k-kad-port-tips":
		"UDP-порт для пошуку через Kad. Допустимі значення: 1-65535 (потрібен перезапуск двигуна)",
	"ed2k-kad-port-invalid": "UDP-порт Kad має бути цілим числом від 1 до 65535",
	"listen-port-invalid":
		"Порт BT має бути портом від 1 до 65535, діапазоном на кшталт 6881-6889 або списком таких значень через кому",
	"dht-listen-port-invalid":
		"Порт DHT має бути портом від 1 до 65535, діапазоном на кшталт 6881-6889 або списком таких значень через кому",
	"ed2k-port-invalid": "Порт ED2K має бути цілим числом від 1 до 65535",
	"ed2k-enable-upload": "Роздавати файли через ED2K",
	"ed2k-enable-upload-tips":
		"Віддавати іншим пірам завершені файли та перевірені частини поточних завантажень ED2K. Kad може показувати файл, який ви перестали роздавати, ще до 5 годин (потрібен перезапуск двигуна)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Облікові дані за замовчуванням для завантажень FTP, FTPS і SFTP",
	"ftp-username": "Ім'я користувача FTP",
	"ftp-password": "Пароль FTP",
	"sftp-private-key": "Шлях до закритого ключа SSH",
	"sftp-private-key-content": "Вміст закритого ключа SSH",
	"sftp-private-key-tips":
		"Шлях до файлу закритого ключа PEM, напр. ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips": "Вставте сюди вміст закритого ключа PEM",
	"sftp-key-passphrase": "Парольна фраза ключа",
	"sftp-key-passphrase-tips":
		"Залиште порожнім, якщо ключ не має парольної фрази",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Формат виводу",
	"media-settings": "Медіасайти (yt-dlp)",
	"media-settings-tips":
		"Налаштування за замовчуванням для завантажень із медіасайтів (YouTube, Vimeo, Bilibili та інші). Потрібен yt-dlp у PATH; для об'єднання окремих відео- й аудіопотоків потрібен ffmpeg.",
	"media-format": "Бажаний формат",
	"media-format-placeholder":
		"напр. bestvideo+bestaudio/best (рядок формату yt-dlp)",
	"media-format-tips":
		"Селектор формату yt-dlp. Залиште порожнім для найкращої якості. Приклади: bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "Слухайте порти",
	"bt-port": "Порт прослуховування BT",
	"dht-port": "Порт прослуховування DHT",
	"ed2k-port": "Порт прослуховування ed2k",
	security: "Безпека",
	rpc: "RPC",
	"rpc-host": "Адреса сервера",
	"rpc-host-local": "Лише цей пристрій",
	"rpc-host-all": "Усі мережеві інтерфейси",
	"rpc-host-custom": "Власна ({{host}})",
	"rpc-host-exposed-tips":
		"Інші пристрої у вашій мережі можуть підключатися до порту RPC, а також до порту PeerBanHelper, якщо його ввімкнено. Будь-хто, хто знає секрет, може додавати завантаження й керувати ними.",
	"rpc-listen-port": "Порт прослуховування RPC",
	"rpc-secret": "RPC Secret",
	"rpc-secret-tips": "Дивитись інструкцію RPC Secret",
	"rpc-allow-any-dir":
		"Дозволити клієнтам RPC використовувати будь-який локальний шлях",
	"rpc-allow-any-dir-tips":
		"Якщо вимкнено, завантаження, додані через RPC, можна зберігати лише в каталозі завантаження, його підтеках і ваших каталогах категорій та маршрутизації, а клієнти RPC не можуть змусити застосунок прочитати файл cookie, netrc або ключа SSH.",
	pbh: "PeerBanHelper",
	"pbh-enable": "Увімкнути API PeerBanHelper",
	"pbh-enable-tips":
		"Відкриває JSON-RPC слухач, сумісний з Aria2Next, щоб PeerBanHelper міг стежити за пірами BitTorrent і блокувати їх",
	"pbh-listen-port": "Порт прослуховування PeerBanHelper",
	"pbh-rpc-secret": "RPC-токен PeerBanHelper",
	"generate-pbh-rpc-secret": "Згенерувати новий токен",
	"pbh-endpoint-hint":
		"У PeerBanHelper додайте завантажувач Aria2Next, спрямований на http://127.0.0.1:{{port}}/jsonrpc",
	"randomize-port": "Вибрати випадковий порт",
	"generate-rpc-secret": "Створити новий секрет RPC",
	"copy-rpc-url": "Копіювати URL RPC",
	"engine-overrides": "Перевизначення двигуна (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Додатково: задайте довільні ключі параметрів двигуна як JSON-об'єкт. Зміни набудуть чинності після перезапуску.",
	"engine-overrides-invalid":
		"Перевизначення двигуна мають бути коректним JSON-об'єктом.",
	"engine-overrides-reserved-keys":
		"Проігноровано зарезервовані ключі перевизначення двигуна: {{keys}}",
	"engine-overrides-too-large":
		"Перевизначення двигуна завеликі: щонайбільше 64 ключі, 64 символи на ключ і 2048 символів на значення.",
	developer: "Розробник",
	"user-agent": "User-Agent",
	"mock-user-agent": "Макет User-Agent",
	cookies: "Cookies",
	"cookies-tips":
		"Завантажити cookie з файлу cookies.txt у форматі Netscape/Mozilla (також приймається вивід curl/wget).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Збережені облікові дані доменів",
	"saved-cookies-tips":
		"Cookie та User-Agent, імпортовані з браузера для обходу Cloudflare або інших завантажень із захищеною автентифікацією. Risuko повторно використовує їх для нових завантажень з того самого домену, доки вони не втратять чинність або не перестануть працювати.",
	"saved-cookies-empty": "Збережених облікових даних ще немає.",
	"saved-cookies-host": "Домен",
	"saved-cookies-browser": "Браузер",
	"saved-cookies-count": "Cookies",
	"saved-cookies-imported": "Імпортовано",
	"saved-cookies-actions": "Дії",
	"saved-cookies-clear": "Очистити все",
	"saved-cookies-confirm-clear":
		"Видалити всі збережені облікові дані доменів? Майбутні завантаження з цих сайтів можуть не вдаватися, доки ви не імпортуєте їх знову.",
	"saved-cookies-delete": "Видалити",
	netrc: "Облікові дані .netrc",
	"netrc-tips":
		"Підставляти HTTP Basic-автентифікацію з файлу .netrc, якщо в URL немає вбудованих облікових даних. За замовчуванням: ~/.netrc (або %USERPROFILE%/_netrc у Windows).",
	"netrc-path-placeholder": "Перевизначити шлях до .netrc (необов'язково)",
	"no-netrc": "Вимкнути пошук .netrc",
	"no-netrc-tips":
		"Повністю пропускати .netrc. Корисно, коли файл містить облікові дані не для завантажень.",
	"app-log-path": "Шлях до журналу додатка",
	"log-dir-override-placeholder":
		"Власний каталог журналів (залиште порожнім для типового)",
	"log-dir-override-tips":
		"Зараз журнали записуються в {{path}}. Власний каталог (наприклад, у Downloads) робить їх видимими для будь-якого файлового менеджера. Зміни набудуть чинності після наступного перезапуску застосунку.",
	"download-session-path": "Шлях до сеансу завантаження",
	"session-reset": "Скинути сеанс завантаження",
	"session-reset-confirm": "Ви впевнені, що хочете скинути сеанс завантаження?",
	"factory-reset": "Налаштування за замовчуванням",
	"factory-reset-confirm":
		"Ви впевнені, що бажаєте повернутись до налаштувань за замовчуванням?",
	"download-protocol": "Протоколи",
	"protocols-default-client":
		"Встановіть як клієнта за замовчуванням для таких протоколів",
	"protocols-magnet": "Magnet [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc:// adcs:// dchub:// nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella://, gnet:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "Інтеграція з демоном giFT",
	"gift-enabled": "Увімкнути делегування giFT",
	"gift-host": "Хост giFT",
	"gift-port": "Порт giFT",
	"gift-not-detected": "Демон giFT не виявлено на налаштованому хості:порту.",
	"browser-extensions": "Розширення",
	"baidu-exporter": "BaiduExporter",
	"browser-extensions-tips": "Надається спільнотою, ",
	"baidu-exporter-help": "Натисніть тут для використання",
	"auto-update": "Автоматичне оновлення",
	"auto-check-update": "Автоматично перевіряти оновлення",
	"last-check-update-time": "Остання перевірка оновлень",
	"completion-script": "Скрипт після завершення",
	"completion-script-enabled": "Запускати скрипт після завершення завантаження",
	"completion-script-tips":
		"Виконується в пісочниці через tauri-plugin-shell. Запускається один раз наприкінці кожного завдання (успіх або помилка).",
	"completion-script-command": "Команда",
	"completion-script-command-placeholder":
		"напр. /usr/local/bin/notify.sh або C:\\Tools\\notify.exe",
	"completion-script-args": "Аргументи",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Заповнювачі: {path}, {hash}, {status}. Також доступні як змінні середовища RISUKO_PATH, RISUKO_HASH, RISUKO_STATUS.",
	"completion-script-timeout": "Таймаут (мс)",
	"completion-script-test": "Перевірити скрипт",
	"completion-script-test-ok": "Перевірку скрипта завершено успішно",
	"completion-script-test-fail": "Перевірка скрипта не вдалася",
	"completion-script-empty": "Спочатку вкажіть команду",
	"completion-script-not-approved":
		"Скрипт завершення не було дозволено, тому нічого не збережено.",
	"not-saved": "Налаштування не збережено",
	"not-saved-confirm":
		"Змінені параметри буде втрачено. Ви впевнені, що залишите?",
	"saved-credentials": "Збережені облікові дані",
	"saved-credentials-description":
		"Керуйте збереженими обліковими даними автентифікації для завантажень",
	"no-saved-credentials": "Збережених облікових даних немає",
	"credential-host": "Хост",
	"credential-label": "Назва",
	"credential-protocol": "Протокол",
	"credential-last-used": "Востаннє використано",
	"credential-delete": "Видалити облікові дані",
	"credential-delete-confirm":
		"Ви впевнені, що хочете видалити ці збережені облікові дані?",
	"vault-enabled": "Секрети зберігаються в сховищі ключів ОС",
	"vault-disabled":
		"Сховище ключів ОС недоступне — секрети зберігаються у відкритому вигляді",
	"credential-encrypted-badge": "Зберігається в сховищі ключів ОС",
	"credential-vault-unavailable":
		"Сховище ключів ОС недоступне; нові облікові дані буде збережено у відкритому вигляді",
	"speed-limit-enabled": "Увімкнути обмеження швидкості",
	"speed-limit-enabled-description":
		"Застосувати наведені нижче обмеження. Якщо вимкнено, завантаження працюють на повній швидкості; значення зберігаються.",
	"appearance-theme": "Тема",
	"appearance-typography": "Типографіка",
	"appearance-window": "Вікно",
	"task-list-style": "Список завдань",
	"task-list-style-description":
		"Як завантаження відображаються у списку завдань.",
	"task-list-style-compact": "Компактний",
	"task-list-style-card": "Картки",
	"sidebar-collapsed": "Згорнути бічну панель",
	"sidebar-collapsed-description": "Показувати в бічній панелі лише значки.",
};
