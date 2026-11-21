export default {
	basic: "Básico",
	advanced: "Avanzado",
	cloudSinks: "Destinos en la nube",
	usenet: "Usenet",
	search: "Buscar",
	"search-placeholder": "Buscar en todas las preferencias",
	"search-all-tabs": "Todas las preferencias",
	"search-results": "{{count}} ajustes coincidentes",
	"search-no-results": "No se encontraron ajustes coincidentes",
	"search-clear": "Borrar búsqueda",
	"search-go-to-tab": "Abrir ajuste",
	"search-more-results": "Refina la búsqueda para ver más resultados",
	"usenet-tips":
		"Gestiona los proveedores NNTP y los valores predeterminados de limpieza segura de archivos comprimidos.",
	"usenet-profiles": "Perfiles de proveedor",
	"usenet-profiles-tips":
		"Configura la prioridad, la seguridad, la autenticación y los límites de conexión de los proveedores.",
	"usenet-add-profile": "Añadir perfil",
	"usenet-no-profiles": "No hay perfiles de proveedor configurados.",
	"usenet-no-profiles-tips":
		"Añade al menos un proveedor antes de importar una descarga NZB.",
	"usenet-edit-profile": "Editar perfil",
	"usenet-profile-dialog-tips":
		"Los ajustes del perfil pueden sincronizarse entre dispositivos. Las credenciales permanecen solo en este dispositivo.",
	"usenet-profile-dialog-tips-vault":
		"Los ajustes del perfil pueden sincronizarse entre dispositivos. Las credenciales permanecen en el llavero seguro de este dispositivo.",
	"usenet-profile-name": "Nombre del perfil",
	"usenet-profile-name-placeholder": "Proveedor principal",
	"usenet-profile-host": "Host NNTP",
	"usenet-profile-port": "Puerto",
	"usenet-security": "Seguridad de la conexión",
	"usenet-tls": "TLS implícito",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "NNTP sin encriptar",
	"usenet-authentication": "Autenticación",
	"usenet-auth-credentials": "Usuario y contraseña",
	"usenet-auth-anonymous": "Anónimo",
	"usenet-username": "Usuario",
	"usenet-password": "Contraseña",
	"usenet-credentials-saved": "Credenciales guardadas",
	"usenet-credentials-unavailable": "Estado de las credenciales no disponible",
	"usenet-credentials-saved-tips":
		"Las credenciales ya están guardadas. Deja ambos campos vacíos para mantenerlas sin cambios.",
	"usenet-plain-warning-title": "Conexión sin encriptar",
	"usenet-plain-opt-in":
		"Lo entiendo y quiero permitir NNTP sin encriptar para este perfil.",
	"usenet-plain-warning":
		"NNTP sin encriptar no protege las credenciales ni los datos descargados. Úsalo solo cuando tu proveedor lo exija expresamente.",
	"usenet-enabled": "Activado",
	"usenet-enabled-tips":
		"Los perfiles desactivados se conservan, pero no se usan para las descargas.",
	"usenet-disabled": "Desactivado",
	"usenet-priority": "Prioridad",
	"usenet-priority-tips":
		"Los valores más bajos se prueban primero. Las prioridades iguales se reparten la carga.",
	"usenet-priority-value": "Prioridad {{value}}",
	"usenet-max-connections": "Máximo de conexiones",
	"usenet-max-connections-tips":
		"El límite de conexiones de este perfil de proveedor.",
	"usenet-connections-value": "{{value}} conexiones",
	"usenet-test": "Probar",
	"usenet-edit": "Editar perfil",
	"usenet-remove": "Quitar",
	"usenet-remove-title": "¿Quitar el perfil de proveedor?",
	"usenet-remove-confirm":
		"{{name}} dejará de estar disponible para las descargas. Sus credenciales guardadas también se eliminarán de este dispositivo.",
	"usenet-profile-saved": "Perfil de proveedor guardado.",
	"usenet-profile-removed": "Se quitó {{name}}.",
	"usenet-test-success": "Conectado a {{name}}.",
	"usenet-test-failed": "Error de conexión: {{message}}",
	"usenet-validation-name": "Introduce un nombre de perfil.",
	"usenet-validation-host": "Introduce un host NNTP.",
	"usenet-validation-port": "El puerto debe estar entre 1 y 65535.",
	"usenet-validation-plain":
		"Confirma la advertencia de NNTP sin encriptar antes de guardar.",
	"usenet-validation-priority": "La prioridad debe ser cero o mayor.",
	"usenet-validation-connections": "Las conexiones deben estar entre 1 y 128.",
	"usenet-validation-credentials":
		"Introduce usuario y contraseña, o elige la autenticación anónima.",
	"usenet-validation-credential-status":
		"El estado de las credenciales no está disponible. Inténtalo de nuevo antes de modificar este perfil.",
	"usenet-cleanup": "Limpieza tras un resultado correcto",
	"usenet-cleanup-tips":
		"La limpieza solo se ejecuta tras el ensamblado completo y una extracción correcta.",
	"usenet-cleanup-action": "Tras un éxito verificado",
	"usenet-cleanup-keep": "Conservar todos los archivos de origen",
	"usenet-cleanup-keep-tips":
		"Conserva los archivos PAR2 y todos los volúmenes comprimidos.",
	"usenet-cleanup-par2": "Eliminar archivos PAR2",
	"usenet-cleanup-par2-tips":
		"Elimina los archivos de paridad, pero conserva los volúmenes comprimidos descargados.",
	"usenet-cleanup-all": "Eliminar PAR2 y volúmenes comprimidos",
	"usenet-cleanup-all-tips":
		"Elimina los archivos de paridad y los volúmenes comprimidos cuando la extracción termina correctamente.",
	"usenet-archive-safety": "Seguridad de los archivos comprimidos",
	"usenet-limits-defaults":
		"Los límites aplicados a toda la tarea protegen el almacenamiento y el procesamiento durante la verificación y la extracción.",
	"usenet-desktop-limits": "Límites en escritorio",
	"usenet-android-limits": "Límites en Android",
	"usenet-limits-summary":
		"{{entries}} entradas · {{total}} de expansión total · {{perEntry}} por entrada",
	"usenet-limits-secondary-summary":
		"relación {{ratio}}:1 · {{depth}} niveles de anidamiento · {{reserve}} de espacio libre reservado · {{hours}} horas activas",
	"usenet-customize-limits": "Personalizar límites",
	"usenet-hide-limits": "Ocultar límites",
	"usenet-limits-adjusted": "Se ajustaron los límites sincronizados",
	"usenet-limits-adjusted-tips":
		"Uno o más valores superaban el máximo absoluto de esta plataforma y se redujeron.",
	"usenet-max-entries": "Máximo de entradas",
	"usenet-max-expanded": "Tamaño total expandido",
	"usenet-max-entry": "Tamaño de una sola entrada",
	"usenet-max-depth": "Profundidad de anidamiento",
	"usenet-max-ratio": "Relación de compresión",
	"usenet-free-space-reserve": "Reserva de espacio libre",
	"usenet-max-active-time": "Tiempo de procesamiento activo",
	"usenet-unit-entries": "entradas",
	"usenet-unit-levels": "niveles",
	"usenet-unit-hours": "horas",
	"usenet-limits-hard-cap":
		"Los valores no pueden superar el cuádruple de los predeterminados de la plataforma. Los ajustes por tarea se confirman por separado.",
	"usenet-restore-limits":
		"Restaurar los valores predeterminados de la plataforma",
	"usenet-limits-restored":
		"Límites de archivos comprimidos restaurados a los valores predeterminados de la plataforma.",
	save: "Guardar y Aplicar",
	"save-success-message": "Preferencias guardadas exitosamente",
	"save-fail-message": "Hubo un error al guardar tus preferencias",
	discard: "Descartar",
	"history-directories": "Directorios de descarga recientes",
	"favorite-directory": "Directorio favorito",
	"unfavorite-directory": "Quitar directorio de favoritos",
	"remove-history-directory": "Quitar directorio del historial",
	startup: "Inicio",
	"open-at-login": "Abrir al iniciar sesión",
	"keep-window-state":
		"Mantener el tamaño y la posición de la ventana al salir",
	"auto-resume-all": "Reanudar automáticamente todas las tareas sin finalizar",
	"default-dir": "Ruta por defecto",
	"file-category-dirs": "Rutas por categoría",
	"file-category-dirs-tips":
		"Sustituye la ruta de descarga predeterminada para tipos de archivo concretos. Déjalo vacío para usar la ruta predeterminada.",
	"task-routing-rules": "Reglas de enrutamiento de tareas",
	"task-routing-rules-tips":
		"Dirige las tareas a directorios según patrones de nombre de archivo. Se aplica la primera regla que coincida.",
	"task-routing-rule-pattern-placeholder": "Patrón (p. ej. *.mkv)",
	"task-routing-rule-label-placeholder": "Etiqueta",
	"task-routing-rule-dir-placeholder": "Directorio",
	"task-routing-rule-add": "Añadir regla",
	"file-category-music": "Música",
	"file-category-video": "Vídeo",
	"file-category-image": "Imagen",
	"file-category-document": "Documento",
	"file-category-compressed": "Comprimido",
	"file-category-program": "Programa",
	"mas-default-dir-tips":
		"Debido a las restricciones de la tienda de aplicaciones, la ruta por defecto se recomienda que sea ~/Downloads",
	"transfer-settings": "Transferencia",
	"transfer-speed-upload": "Límite de subida",
	"transfer-speed-download": "Límite de bajada",
	"transfer-speed-unlimited": "Ilimitado",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Guardar enlace magnet como archivo torrent",
	"bt-create-subfolder":
		"Agrupar los torrents de varios archivos en una subcarpeta",
	"bt-create-subfolder-tips":
		"Si está activado, las descargas BitTorrent y magnet de varios archivos se colocan dentro de una carpeta con el nombre del torrent. Si está desactivado, los archivos se escriben directamente en el directorio de descarga.",
	"bt-upload-rate-limit": "Límite de subida de BitTorrent",
	"bt-upload-rate-limit-tips":
		"Limita las subidas de BitTorrent además del límite de subida global. 0 significa sin límite aparte.",
	"bt-auto-download-content":
		"Descargar automáticamente el contenido de magnet y torrent",
	"bt-force-encryption": "Forzar encriptación BT",
	"keep-seeding": "Siga sembrando hasta detenerlo manualmente",
	"seed-ratio": "Proporción de semillas",
	"seed-time": "Tiempo de semilla",
	"seed-time-unit": "minutos",
	"task-manage": "Gestión de tareas",
	"max-concurrent-downloads": "Tareas máximas activas",
	"connections-per-task": "Conexiones por tarea",
	"max-connection-per-server": "Conexiones máximas por servidor",
	"auto-retry": "Reintentar automáticamente las tareas fallidas",
	"auto-retry-strategy": "Modo de intervalo de reintento",
	"auto-retry-strategy-static": "Fijo",
	"auto-retry-strategy-exponential": "Retroceso exponencial",
	"auto-retry-interval": "Intervalo base de reintento",
	"auto-retry-interval-unit": "segundos",
	"auto-retry-max-attempts": "Máximo de reintentos (0 = ilimitado)",
	"worker-max-retries": "Máx. de reintentos por pieza del worker",
	"auto-detect-low-speed-tasks": "Detectar automáticamente las tareas lentas",
	"auto-detect-low-speed-tasks-tips":
		"Si una tarea en ejecución se mantiene por debajo del umbral de velocidad, se pausa y se reinicia automáticamente.",
	"low-speed-threshold": "Umbral de baja velocidad",
	"low-speed-threshold-unit": "KB/s",
	"http-reliability": "Fiabilidad de HTTP / red",
	"http-reliability-tips":
		"Ajusta el tiempo de espera de conexión y la detección de transferencias estancadas en las descargas HTTP/HTTPS/FTP.",
	"connect-timeout": "Tiempo de espera de conexión",
	"nzb-body-timeout": "Tiempo de espera del cuerpo NZB",
	"lowest-speed-limit": "Límite de velocidad mínima",
	"lowest-speed-limit-timeout": "Tiempo de espera de estancamiento",
	"lowest-speed-limit-help":
		"Si una descarga se mantiene por debajo del límite de velocidad mínima durante el tiempo de espera de estancamiento, el worker se aborta y (si el reintento automático está activado) se reinicia. Pon el límite en 0 para desactivarlo.",
	"uri-selector": "Selección de réplicas",
	"uri-selector-feedback":
		"Con retroalimentación (descarta réplicas lentas o fallidas)",
	"uri-selector-inorder":
		"En orden (prueba las réplicas de izquierda a derecha)",
	"uri-selector-adaptive": "Adaptativa (prefiere la más rápida)",
	"unit-seconds": "segundos",
	"unit-kib-per-sec": "KiB/s",
	storage: "Almacenamiento",
	"storage-tips":
		"Cómo reserva el motor el espacio en disco antes de descargar.",
	"file-allocation": "Asignación de archivos",
	"file-allocation-falloc": "Preasignar (falloc)",
	"file-allocation-trunc": "Truncar (solo fija la longitud)",
	"file-allocation-none": "Ninguna (crece según se necesita)",
	"file-allocation-tips":
		"Preasignar reserva bloques de disco reales por adelantado para evitar ENOSPC a mitad de la descarga y reducir la fragmentación. Recurre a truncar en los sistemas de archivos que no lo admiten.",
	"new-task-show-downloading":
		"Mostrar automáticamente la descarga después de añadir una tarea",
	"no-confirm-before-delete-task":
		"No se requiere confirmación antes de eliminar la tarea",
	"auto-file-renaming": "Renombrar automáticamente los archivos duplicados",
	"auto-file-renaming-tips":
		'Cuando ya existe un archivo con el mismo nombre, se guarda como "file.1.ext" en lugar de sobrescribirlo',
	"use-remote-file-time": "Usar la fecha de modificación remota al guardar",
	"use-remote-file-time-tips":
		"Define la fecha de modificación del archivo con la marca de tiempo del servidor remoto",
	continue: "Continuar",
	"task-completed-notify": "Notificar después de que la descarga se complete",
	"clipboard-watch": "Vigilar el portapapeles en busca de enlaces de descarga",
	"clipboard-watch-tips":
		"Cuando copias un enlace magnet o de archivo en cualquier lugar, Risuko te ofrece descargarlo.",
	"clipboard-watch-ext": "Extensiones vigiladas",
	"clipboard-watch-ext-tips":
		"Solo los enlaces http(s) que terminan en estas extensiones muestran el aviso. Los enlaces magnet, ed2k y torrent siempre lo hacen. Sepáralas con espacios o comas.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"Risuko ahora vigila tu portapapeles en busca de enlaces de descarga. Puedes desactivarlo en Preferencias → Básico.",
	"prevent-sleep-while-downloading":
		"Evitar que el sistema entre en suspensión durante la descarga",
	"prevent-sleep-while-downloading-tips":
		"Mantener el equipo despierto siempre que haya una tarea activa en curso",
	"shutdown-when-complete": "Apagar el equipo cuando terminen todas las tareas",
	"shutdown-when-complete-tips":
		"Una sola vez: apaga el equipo cuando la cola de descargas esté vacía",
	"shutdown-countdown-title": "Apagando en {{seconds}} s…",
	"shutdown-countdown-cancel": "Cancelar",
	"shutdown-failed": "Error al apagar. Comprueba los permisos.",
	"auto-purge-record":
		"Eliminar automáticamente el registro de descargas al salir",
	"purge-record-on-start":
		"Purgar los registros de descargas al iniciar la aplicación",
	ui: "UI",
	appearance: "Apariencia",
	"theme-auto": "Auto",
	"theme-light": "Claro",
	"theme-dark": "Oscuro",
	"font-family": "Fuente",
	"font-family-system": "Sistema",
	"font-family-rounded": "Redondeada",
	"font-family-serif": "Con serifa",
	"font-family-mono": "Monoespaciada",
	"font-family-sample": "El rápido zorro marrón descarga más rápido.",
	"font-size": "Tamaño",
	"font-size-small": "Pequeño",
	"font-size-small-short": "Pequeño",
	"font-size-default": "Predeterminado",
	"font-size-default-short": "Predeterminado",
	"font-size-large": "Grande",
	"font-size-large-short": "Grande",
	"font-size-extra-large": "Muy grande",
	"font-size-extra-large-short": "Muy grande",
	"auto-hide-window": "Ocultar automáticamente ventanas",
	"run-mode": "Correr como",
	"run-mode-standard": "Aplicación estándar",
	"run-mode-tray": "Aplicación de bandeja",
	"run-mode-hide-tray": "Ocultar aplicación de bandeja",
	"tray-speedometer":
		"La bandeja de la barra de menú muestra la velocidad en tiempo real",
	"show-progress-bar": "Mostrar la barra de progreso de descarga",
	language: "Idioma",
	"change-language": "Cambiar Idioma",
	"hide-app-menu": "Ocultar el menú (Solo Windows y Linux)",
	proxy: "Proxy",
	"proxy-http-profile": "Proxy HTTP / FTP / Usenet",
	"proxy-p2p-profile": "Proxy P2P",
	"enable-proxy": "Activar Proxy",
	"enable-p2p-proxy": "Activar proxy P2P",
	"proxy-p2p-tcp-server": "Servidor proxy TCP",
	"proxy-p2p-tcp-bypass": "Omitir proxy TCP",
	"proxy-p2p-udp-server": "Servidor proxy UDP",
	"proxy-p2p-udp-server-placeholder":
		"Opcional; déjalo en blanco para usar el proxy P2P TCP",
	"proxy-p2p-udp-bypass": "Omitir proxy UDP",
	"proxy-bypass-input-tips":
		"Omitir la configuración del proxy para estos hosts y dominios, uno por línea",
	"proxy-scope-label": "Se utiliza para",
	"proxy-scope-download": "Descargar",
	"proxy-scope-download-desc":
		"Enrutar el tráfico de descarga a través del proxy",
	"proxy-scope-update-app": "Actualizar aplicación",
	"proxy-scope-update-app-desc":
		"Buscar actualizaciones de la aplicación a través del proxy",
	"proxy-scope-update-trackers": "Actualizar Trackers",
	"proxy-scope-update-trackers-desc":
		"Obtener listas de rastreadores BT a través del proxy",
	"proxy-tips": "Ver manual para Proxy",
	"proxy-p2p-tips":
		"DHT, los trackers UDP y uTP requieren SOCKS5 o SOCKS5H. Los proxies HTTP solo se usan en las conexiones P2P por TCP.",
	"proxy-p2p-udp-tips":
		"Deja el proxy UDP en blanco para reutilizar la ruta P2P por TCP. El proxy UDP requiere SOCKS5 o SOCKS5H; los proxies HTTP no pueden transportar UDP.",
	"proxy-p2p-restart-title": "¿Reiniciar las descargas P2P?",
	"proxy-p2p-restart-confirm":
		"Cambiar el proxy P2P reinicia las descargas peer-to-peer activas. Las descargas HTTP, FTP y Usenet siguen en ejecución.",
	doh: "DNS sobre HTTPS",
	"doh-tips":
		"Resuelve los nombres de host mediante un endpoint DNS encriptado en lugar del resolvedor del sistema",
	"doh-enable": "Activar DNS sobre HTTPS",
	"doh-provider": "Proveedor",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Personalizado",
	"doh-url": "URL del endpoint",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "El endpoint DoH debe empezar por https://",
	"doh-bootstrap": "IP de arranque",
	"doh-bootstrap-placeholder":
		"IP para llegar al endpoint, separadas por comas (opcional)",
	"doh-fallback": "Recurrir al DNS del sistema",
	"doh-fallback-tips":
		"Si falla una consulta DoH, se resuelve con el resolvedor del sistema en lugar de hacer fallar la descarga",
	"bt-tracker": "Servidores de rastreadores",
	"bt-tracker-input-tips": "Servidores de rastreadores, uno por línea",
	"bt-tracker-tips": "Recomendado: ",
	"bt-max-peers-per-torrent": "Máx. de pares por torrent",
	"bt-max-peers-per-torrent-tips":
		"Máximo de conexiones simultáneas con pares por torrent. Los valores más altos mejoran el descubrimiento del enjambre (requiere reiniciar el motor)",
	"bt-max-outstanding-per-peer": "Máx. de solicitudes pendientes por par",
	"bt-max-outstanding-per-peer-tips":
		"Máximo de solicitudes de fragmentos simultáneas en pipeline hacia cada par (límite, hasta 500). 0 usa el valor adaptativo predeterminado de 6–500; un presupuesto de solicitudes para todo el torrent se reparte entre los pares activos. Requiere reiniciar el motor",
	"bt-max-connections": "Máx. de conexiones con pares",
	"bt-max-connections-tips":
		"Total de conexiones con pares entre todos los torrents, de 20 a 5000. Reducirlo nunca cierra las conexiones ya abiertas, solo retiene las nuevas. En Linux, macOS y Android el valor efectivo también está limitado a la mitad del límite de descriptores de archivo del sistema",
	"bt-ban-corrupt-peers": "Bloquear pares que envían datos corruptos",
	"bt-ban-corrupt-peers-tips":
		"Bloquea una dirección IP durante el resto de la sesión cuando las piezas que ayudó a enviar siguen fallando la comprobación de hash. Muchos usuarios pueden compartir una misma IP (operadores móviles, redes compartidas), por lo que un bloqueo puede dejar fuera también a pares inocentes (requiere reiniciar el motor)",
	"bt-ban-corrupt-strikes": "Piezas fallidas antes del bloqueo",
	"bt-ban-corrupt-strikes-tips":
		"Cuántas piezas fallidas puede aportar una dirección IP antes de ser bloqueada. Los valores más altos toleran corrupción ocasional (requiere reiniciar el motor)",
	"bt-enable-upnp": "Reenvío de puertos UPnP / NAT-PMP",
	"bt-enable-upnp-tips":
		"Abre automáticamente el puerto de escucha de BitTorrent en el router mediante UPnP IGD, NAT-PMP o PCP (requiere reiniciar el motor)",
	"bt-upnp-lease": "Duración de la asignación de puertos (segundos)",
	"bt-upnp-lease-tips":
		"Duración de las asignaciones de puertos UPnP y NAT-PMP. Las duraciones más cortas permiten recuperarse antes de asignaciones obsoletas (requiere reiniciar el motor)",
	"bt-enable-lsd": "Descubrimiento de servicio local",
	"bt-enable-lsd-tips":
		"Descubre pares en la red local mediante multicast BEP-14 (requiere reiniciar el motor)",
	"bt-encryption-policy": "Encriptación de protocolo",
	"bt-encryption-policy-tips":
		"Política MSE/PE para las conexiones con pares. 'Preferir' prueba primero la encriptación y recurre al texto plano (requiere reiniciar el motor)",
	"bt-encryption-plaintext": "Solo texto plano",
	"bt-encryption-prefer": "Preferir encriptación",
	"bt-encryption-require": "Exigir encriptación",
	"bt-listen-v6": "Escuchar en IPv6",
	"bt-listen-v6-tips":
		"Enlaza un receptor TCP IPv6 adicional en el mismo puerto (requiere reiniciar el motor)",
	"sync-tracker-tips": "Sincronizar",
	"auto-sync-tracker":
		"Actualice la lista de rastreadores todos los días automáticamente",
	"ed2k-server": "Servidores eDonkey",
	"ed2k-server-input-tips": "Servidores eDonkey, uno por línea (ip:puerto)",
	"ed2k-server-tips": "Si está vacío, se usarán los servidores predeterminados",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Activar el descubrimiento de fuentes mediante Kad",
	"ed2k-enable-kad-tips":
		"Descubre fuentes ED2K mediante la red eMule Kad (requiere reiniciar el motor)",
	"ed2k-kad-port": "Puerto UDP de Kad",
	"ed2k-kad-port-tips":
		"Puerto UDP utilizado para el descubrimiento mediante Kad. Los valores válidos están entre 1 y 65535 (requiere reiniciar el motor)",
	"ed2k-kad-port-invalid":
		"El puerto UDP de Kad debe ser un número entero entre 1 y 65535",
	"listen-port-invalid":
		"El puerto BT debe ser un puerto de 1 a 65535, un intervalo como 6881-6889 o una lista de ellos separada por comas",
	"dht-listen-port-invalid":
		"El puerto DHT debe ser un puerto de 1 a 65535, un intervalo como 6881-6889 o una lista de ellos separada por comas",
	"ed2k-port-invalid": "El puerto ED2K debe ser un entero de 1 a 65535",
	"ed2k-enable-upload": "Compartir archivos por ED2K",
	"ed2k-enable-upload-tips":
		"Sube a otros pares los archivos terminados y las partes verificadas de las descargas ED2K en curso. Kad puede seguir listando un archivo que dejaste de compartir hasta 5 horas (requiere reiniciar el motor)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Credenciales predeterminadas para las descargas FTP, FTPS y SFTP",
	"ftp-username": "Usuario FTP",
	"ftp-password": "Contraseña FTP",
	"sftp-private-key": "Ruta de la clave privada SSH",
	"sftp-private-key-content": "Contenido de la clave privada SSH",
	"sftp-private-key-tips":
		"Ruta del archivo de clave privada PEM, p. ej. ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips":
		"Pega aquí el contenido de la clave privada PEM",
	"sftp-key-passphrase": "Frase de contraseña de la clave",
	"sftp-key-passphrase-tips":
		"Déjalo vacío si la clave no tiene frase de contraseña",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Formato de salida",
	"media-settings": "Sitios multimedia (yt-dlp)",
	"media-settings-tips":
		"Ajustes predeterminados para las descargas de sitios multimedia (YouTube, Vimeo, Bilibili y más). Requiere yt-dlp en PATH; se necesita ffmpeg para combinar flujos de vídeo y audio separados.",
	"media-format": "Formato preferido",
	"media-format-placeholder":
		"p. ej. bestvideo+bestaudio/best (cadena de formato de yt-dlp)",
	"media-format-tips":
		"Selector de formato de yt-dlp. Déjalo vacío para obtener la mejor calidad. Ejemplos: bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "Puertos de Escucha",
	"bt-port": "Puerto de escucha BT",
	"dht-port": "Puerto de escucha DHT",
	"ed2k-port": "Puerto de escucha ed2k",
	security: "Seguridad",
	rpc: "RPC",
	"rpc-host": "Dirección del servidor",
	"rpc-host-local": "Solo este dispositivo",
	"rpc-host-all": "Todas las interfaces de red",
	"rpc-host-custom": "Personalizada ({{host}})",
	"rpc-host-exposed-tips":
		"Otros dispositivos de tu red pueden acceder al puerto RPC y, si está activado, al puerto de PeerBanHelper. Cualquiera que conozca la clave puede añadir y controlar descargas.",
	"rpc-listen-port": "Puerto de Escucha RPC",
	"rpc-secret": "Clave RPC",
	"rpc-secret-tips": "Ver manual de la clave RPC",
	"rpc-allow-any-dir":
		"Permitir que los clientes RPC usen cualquier ruta local",
	"rpc-allow-any-dir-tips":
		"Si está desactivado, las descargas añadidas por RPC solo pueden guardarse en la carpeta de descarga, sus subcarpetas y tus carpetas de categorías y de enrutamiento, y los clientes RPC no pueden hacer que la aplicación lea un archivo de cookies, netrc o clave SSH.",
	pbh: "PeerBanHelper",
	"pbh-enable": "Activar la API de PeerBanHelper",
	"pbh-enable-tips":
		"Expone un receptor JSON-RPC compatible con Aria2Next para que PeerBanHelper pueda supervisar y bloquear pares de BitTorrent",
	"pbh-listen-port": "Puerto de escucha de PeerBanHelper",
	"pbh-rpc-secret": "Token RPC de PeerBanHelper",
	"generate-pbh-rpc-secret": "Generar un token nuevo",
	"pbh-endpoint-hint":
		"En PeerBanHelper, añade un descargador Aria2Next apuntando a http://127.0.0.1:{{port}}/jsonrpc",
	"randomize-port": "Elegir un puerto aleatorio",
	"generate-rpc-secret": "Generar un nuevo secreto RPC",
	"copy-rpc-url": "Copiar URL de RPC",
	"engine-overrides": "Sobrescrituras del motor (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Avanzado: define claves arbitrarias de opciones del motor como un objeto JSON. Los cambios se aplican después de reiniciar.",
	"engine-overrides-invalid":
		"Las sobrescrituras del motor deben ser un objeto JSON válido.",
	"engine-overrides-reserved-keys":
		"Se ignoraron las claves reservadas de sobrescritura del motor: {{keys}}",
	"engine-overrides-too-large":
		"Las sobrescrituras del motor son demasiado grandes: como máximo 64 claves, 64 caracteres por clave y 2048 caracteres por valor.",
	developer: "Desarrollador",
	"user-agent": "User-Agent",
	"mock-user-agent": "Falsear Agente de Usuario",
	cookies: "Cookies",
	"cookies-tips":
		"Carga cookies desde un archivo cookies.txt en formato Netscape/Mozilla (también acepta la salida de curl/wget).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Credenciales de dominio guardadas",
	"saved-cookies-tips":
		"Cookies y User-Agent importados de un navegador al sortear Cloudflare u otras descargas protegidas por autenticación. Risuko los reutiliza en las descargas nuevas del mismo dominio hasta que caducan o dejan de funcionar.",
	"saved-cookies-empty": "Aún no hay credenciales guardadas.",
	"saved-cookies-host": "Dominio",
	"saved-cookies-browser": "Navegador",
	"saved-cookies-count": "Cookies",
	"saved-cookies-imported": "Importado",
	"saved-cookies-actions": "Acciones",
	"saved-cookies-clear": "Borrar todo",
	"saved-cookies-confirm-clear":
		"¿Eliminar todas las credenciales de dominio guardadas? Las descargas futuras de esos sitios pueden fallar hasta que vuelvas a importarlas.",
	"saved-cookies-delete": "Eliminar",
	netrc: "Credenciales .netrc",
	"netrc-tips":
		"Inserta la autenticación básica HTTP desde un archivo .netrc cuando la URL no incluye credenciales. Valor predeterminado: ~/.netrc (o %USERPROFILE%/_netrc en Windows).",
	"netrc-path-placeholder": "Sustituir la ruta de .netrc (opcional)",
	"no-netrc": "Desactivar la búsqueda de .netrc",
	"no-netrc-tips":
		"Omite .netrc por completo. Útil cuando el archivo contiene credenciales que no son de descargas.",
	"app-log-path": "Ruta del registro",
	"log-dir-override-placeholder":
		"Directorio de registros personalizado (déjalo en blanco para el predeterminado)",
	"log-dir-override-tips":
		"Los registros se escriben actualmente en {{path}}. Si defines un directorio personalizado (p. ej. dentro de Descargas), serán visibles desde cualquier gestor de archivos. Los cambios surten efecto tras el próximo reinicio de la aplicación.",
	"download-session-path": "Ruta de descarga de la sesión",
	"session-reset": "Restablecer la sesión de descargas",
	"session-reset-confirm":
		"¿Seguro que quieres restablecer la sesión de descargas?",
	"factory-reset": "Restauración de fábrica",
	"factory-reset-confirm": "¿Estás seguro que quieres restaurar de fábrica?",
	"download-protocol": "Protocolos",
	"protocols-default-client":
		"Establecer como cliente por defecto de los siguientes protocolos",
	"protocols-magnet": "Magnet [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc:// adcs:// dchub:// nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella://, gnet:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "Integración del demonio giFT",
	"gift-enabled": "Activar la delegación a giFT",
	"gift-host": "Host de giFT",
	"gift-port": "Puerto de giFT",
	"gift-not-detected":
		"No se detectó el demonio giFT en el host:puerto configurados.",
	"browser-extensions": "Extensiones",
	"baidu-exporter": "BaiduExporter",
	"browser-extensions-tips": "Proporcionados por la comunidad, ",
	"baidu-exporter-help": "Presiona aquí para ver el uso",
	"auto-update": "Auto-actualizar",
	"auto-check-update": "Revisar automáticamente por actualizaciones",
	"last-check-update-time": "Última revisión de actualizaciones",
	"completion-script": "Script al completar",
	"completion-script-enabled": "Ejecutar un script al completar la descarga",
	"completion-script-tips":
		"Se ejecuta en sandbox mediante tauri-plugin-shell. Se ejecuta una vez al finalizar cada tarea (éxito o error).",
	"completion-script-command": "Comando",
	"completion-script-command-placeholder":
		"p. ej. /usr/local/bin/notify.sh o C:\\Tools\\notify.exe",
	"completion-script-args": "Argumentos",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Marcadores: {path}, {hash}, {status}. También disponibles como variables de entorno RISUKO_PATH, RISUKO_HASH, RISUKO_STATUS.",
	"completion-script-timeout": "Tiempo de espera (ms)",
	"completion-script-test": "Probar script",
	"completion-script-test-ok": "La prueba del script fue correcta",
	"completion-script-test-fail": "La prueba del script falló",
	"completion-script-empty": "Especifica primero un comando",
	"completion-script-not-approved":
		"No se permitió el script al completar, así que no se guardó nada.",
	"not-saved": "Preferencias no guardadas",
	"not-saved-confirm":
		"Las preferencias cambiadas se perderán, ¿está seguro de irse?",
	"saved-credentials": "Credenciales guardadas",
	"saved-credentials-description":
		"Gestiona las credenciales de autenticación guardadas para las descargas",
	"no-saved-credentials": "No hay credenciales guardadas",
	"credential-host": "Host",
	"credential-label": "Etiqueta",
	"credential-protocol": "Protocolo",
	"credential-last-used": "Último uso",
	"credential-delete": "Eliminar credencial",
	"credential-delete-confirm":
		"¿Seguro que quieres eliminar esta credencial guardada?",
	"vault-enabled": "Secretos almacenados en el llavero del sistema",
	"vault-disabled":
		"Llavero del sistema no disponible: los secretos se almacenan en texto plano",
	"credential-encrypted-badge": "Almacenada en el llavero del sistema",
	"credential-vault-unavailable":
		"No se puede acceder al llavero del sistema; las credenciales nuevas se guardarán en texto plano",
	"speed-limit-enabled": "Activar límites de velocidad",
	"speed-limit-enabled-description":
		"Aplica los límites de abajo. Si está desactivado, las descargas van a máxima velocidad; los valores se conservan.",
	"appearance-theme": "Tema",
	"appearance-typography": "Tipografía",
	"appearance-window": "Ventana",
	"task-list-style": "Lista de tareas",
	"task-list-style-description":
		"Cómo se muestran las descargas en la lista de tareas.",
	"task-list-style-compact": "Compacta",
	"task-list-style-card": "Tarjetas",
	"sidebar-collapsed": "Contraer barra lateral",
	"sidebar-collapsed-description":
		"Muestra solo los iconos en la barra lateral.",
};
