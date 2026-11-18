export default {
	basic: "Basique",
	advanced: "Avancé",
	cloudSinks: "Destinations cloud",
	usenet: "Usenet",
	search: "Rechercher",
	"search-placeholder": "Rechercher dans toutes les préférences",
	"search-all-tabs": "Toutes les préférences",
	"search-results": "{{count}} paramètres correspondants",
	"search-no-results": "Aucun paramètre correspondant trouvé",
	"search-clear": "Effacer la recherche",
	"search-go-to-tab": "Ouvrir le paramètre",
	"search-more-results": "Affinez votre recherche pour voir plus de résultats",
	"usenet-tips":
		"Gérez les fournisseurs NNTP et les réglages de nettoyage sûr des archives.",
	"usenet-profiles": "Profils de fournisseur",
	"usenet-profiles-tips":
		"Configurez la priorité, la sécurité, l’authentification et les limites de connexion des fournisseurs.",
	"usenet-add-profile": "Ajouter un profil",
	"usenet-no-profiles": "Aucun profil de fournisseur configuré.",
	"usenet-no-profiles-tips":
		"Ajoutez au moins un fournisseur avant d’importer un téléchargement NZB.",
	"usenet-edit-profile": "Modifier le profil",
	"usenet-profile-dialog-tips":
		"Les paramètres du profil peuvent être synchronisés entre appareils. Les identifiants restent locaux à cet appareil.",
	"usenet-profile-dialog-tips-vault":
		"Les paramètres du profil peuvent être synchronisés entre appareils. Les identifiants restent dans le trousseau sécurisé de cet appareil.",
	"usenet-profile-name": "Nom du profil",
	"usenet-profile-name-placeholder": "Fournisseur principal",
	"usenet-profile-host": "Hôte NNTP",
	"usenet-profile-port": "Port",
	"usenet-security": "Sécurité de la connexion",
	"usenet-tls": "TLS implicite",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "NNTP non chiffré",
	"usenet-authentication": "Authentification",
	"usenet-auth-credentials": "Nom d’utilisateur et mot de passe",
	"usenet-auth-anonymous": "Anonyme",
	"usenet-username": "Nom d’utilisateur",
	"usenet-password": "Mot de passe",
	"usenet-credentials-saved": "Identifiants enregistrés",
	"usenet-credentials-unavailable": "État des identifiants indisponible",
	"usenet-credentials-saved-tips":
		"Les identifiants sont déjà enregistrés. Laissez les deux champs vides pour les conserver tels quels.",
	"usenet-plain-warning-title": "Connexion non chiffrée",
	"usenet-plain-opt-in":
		"Je comprends et je souhaite autoriser NNTP non chiffré pour ce profil.",
	"usenet-plain-warning":
		"NNTP non chiffré ne protège ni les identifiants ni les données téléchargées. Ne l’utilisez que si votre fournisseur l’exige explicitement.",
	"usenet-enabled": "Activé",
	"usenet-enabled-tips":
		"Les profils désactivés sont conservés mais ne servent pas aux téléchargements.",
	"usenet-disabled": "Désactivé",
	"usenet-priority": "Priorité",
	"usenet-priority-tips":
		"Les valeurs les plus basses sont essayées en premier. Les priorités égales se partagent la charge.",
	"usenet-priority-value": "Priorité {{value}}",
	"usenet-max-connections": "Nombre maximal de connexions",
	"usenet-max-connections-tips":
		"Le plafond de connexions pour ce profil de fournisseur.",
	"usenet-connections-value": "{{value}} connexions",
	"usenet-test": "Tester",
	"usenet-edit": "Modifier le profil",
	"usenet-remove": "Retirer",
	"usenet-remove-title": "Retirer le profil de fournisseur ?",
	"usenet-remove-confirm":
		"{{name}} ne sera plus disponible pour les téléchargements. Ses identifiants enregistrés seront aussi supprimés de cet appareil.",
	"usenet-profile-saved": "Profil de fournisseur enregistré.",
	"usenet-profile-removed": "{{name}} a été retiré.",
	"usenet-test-success": "Connecté à {{name}}.",
	"usenet-test-failed": "Échec de la connexion : {{message}}",
	"usenet-validation-name": "Saisissez un nom de profil.",
	"usenet-validation-host": "Saisissez un hôte NNTP.",
	"usenet-validation-port": "Le port doit être compris entre 1 et 65535.",
	"usenet-validation-plain":
		"Confirmez l’avertissement sur NNTP non chiffré avant d’enregistrer.",
	"usenet-validation-priority":
		"La priorité doit être supérieure ou égale à zéro.",
	"usenet-validation-connections":
		"Le nombre de connexions doit être compris entre 1 et 128.",
	"usenet-validation-credentials":
		"Saisissez un nom d’utilisateur et un mot de passe, ou choisissez l’authentification anonyme.",
	"usenet-validation-credential-status":
		"L’état des identifiants est indisponible. Réessayez avant de modifier ce profil.",
	"usenet-cleanup": "Nettoyage après réussite",
	"usenet-cleanup-tips":
		"Le nettoyage ne s’exécute qu’après un assemblage complet et une extraction réussie.",
	"usenet-cleanup-action": "Après une réussite vérifiée",
	"usenet-cleanup-keep": "Conserver tous les fichiers sources",
	"usenet-cleanup-keep-tips":
		"Conserve les fichiers PAR2 et tous les volumes d’archive.",
	"usenet-cleanup-par2": "Supprimer les fichiers PAR2",
	"usenet-cleanup-par2-tips":
		"Supprime les fichiers de parité mais conserve les volumes d’archive téléchargés.",
	"usenet-cleanup-all": "Supprimer les fichiers PAR2 et les volumes d’archive",
	"usenet-cleanup-all-tips":
		"Supprime les fichiers de parité et les volumes d’archive une fois l’extraction réussie.",
	"usenet-archive-safety": "Sécurité des archives",
	"usenet-limits-defaults":
		"Les limites par tâche protègent le stockage et le traitement pendant la vérification et l’extraction.",
	"usenet-desktop-limits": "Limites sur ordinateur",
	"usenet-android-limits": "Limites sur Android",
	"usenet-limits-summary":
		"{{entries}} entrées · {{total}} d’expansion totale · {{perEntry}} par entrée",
	"usenet-limits-secondary-summary":
		"ratio {{ratio}}:1 · {{depth}} niveaux d’imbrication · {{reserve}} d’espace libre réservé · {{hours}} h d’activité",
	"usenet-customize-limits": "Personnaliser les limites",
	"usenet-hide-limits": "Masquer les limites",
	"usenet-limits-adjusted": "Les limites synchronisées ont été ajustées",
	"usenet-limits-adjusted-tips":
		"Une ou plusieurs valeurs dépassaient le plafond strict de cette plateforme et ont été réduites.",
	"usenet-max-entries": "Nombre maximal d’entrées",
	"usenet-max-expanded": "Taille totale décompressée",
	"usenet-max-entry": "Taille d’une entrée",
	"usenet-max-depth": "Profondeur d’imbrication",
	"usenet-max-ratio": "Taux de compression",
	"usenet-free-space-reserve": "Espace libre réservé",
	"usenet-max-active-time": "Durée de traitement active",
	"usenet-unit-entries": "entrées",
	"usenet-unit-levels": "niveaux",
	"usenet-unit-hours": "heures",
	"usenet-limits-hard-cap":
		"Les valeurs ne peuvent pas dépasser quatre fois les valeurs par défaut de la plateforme. Les remplacements par tâche restent confirmés séparément.",
	"usenet-restore-limits": "Restaurer les valeurs par défaut de la plateforme",
	"usenet-limits-restored":
		"Limites d’archive restaurées aux valeurs par défaut de la plateforme.",
	save: "Sauver et appliquer",
	"save-success-message": "Enregistrer les préférences avec succès",
	"save-fail-message": "La sauvegarde des préférences a échoué",
	discard: "Annuler les changements",
	"history-directories": "Répertoires de téléchargement récents",
	"favorite-directory": "Répertoire favori",
	"unfavorite-directory": "Retirer le répertoire des favoris",
	"remove-history-directory": "Retirer le répertoire de l’historique",
	startup: "Démarrage",
	"open-at-login": "Ouvrir à la connexion",
	"keep-window-state": "Restaurez la taille et la position de la fenêtre",
	"auto-resume-all": "Reprendre les tâches non terminées",
	"default-dir": "Répertoire par défaut",
	"file-category-dirs": "Chemins par catégorie",
	"file-category-dirs-tips":
		"Remplacez le chemin de téléchargement par défaut pour certains types de fichiers. Laissez vide pour utiliser le chemin par défaut.",
	"task-routing-rules": "Règles de routage des tâches",
	"task-routing-rules-tips":
		"Dirige les tâches vers des répertoires selon des motifs de nom de fichier. La première règle correspondante l’emporte.",
	"task-routing-rule-pattern-placeholder": "Motif (ex. *.mkv)",
	"task-routing-rule-label-placeholder": "Étiquette",
	"task-routing-rule-dir-placeholder": "Répertoire",
	"task-routing-rule-add": "Ajouter une règle",
	"file-category-music": "Musique",
	"file-category-video": "Vidéo",
	"file-category-image": "Image",
	"file-category-document": "Document",
	"file-category-compressed": "Archive",
	"file-category-program": "Programme",
	"file-category-rss": "RSS",
	"mas-default-dir-tips":
		"En raison des restrictions d'autorisations du bac à sable de l'App Store, il est recommandé de définir le répertoire de téléchargement par défaut sur le répertoire Téléchargements.",
	"transfer-settings": "Transmission",
	"transfer-speed-upload": "Limite de téléversement",
	"transfer-speed-download": "Limite de téléchargement",
	"transfer-speed-unlimited": "Illimité",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Enregistrer le lien magnet en tant que fichier torrent",
	"bt-create-subfolder":
		"Regrouper les torrents multi-fichiers dans un sous-dossier",
	"bt-create-subfolder-tips":
		"Si activé, les téléchargements BitTorrent et magnet de plusieurs fichiers sont placés dans un dossier portant le nom du torrent. Si désactivé, les fichiers sont écrits directement dans le répertoire de téléchargement.",
	"bt-upload-rate-limit": "Limite de téléversement BitTorrent",
	"bt-upload-rate-limit-tips":
		"Plafonne le téléversement BitTorrent en plus de la limite de téléversement globale. 0 signifie aucun plafond distinct.",
	"bt-auto-download-content":
		"Télécharger automatiquement le contenu des magnets et des torrents",
	"bt-force-encryption": "Forcer le chiffrement de BT",
	"keep-seeding":
		"Continuez à semer jusqu'à ce que vous l'arrêtiez manuellement",
	"seed-ratio": "Ratio de semences",
	"seed-time": "Temps de semence",
	"seed-time-unit": "minutes",
	"task-manage": "Tâches",
	"max-concurrent-downloads": "Nombre de tâches actives au maximum",
	"connections-per-task": "Connexions par tâche",
	"max-connection-per-server": "Nombre maximum de connexions par serveur",
	"auto-retry": "Réessayer automatiquement les tâches en échec",
	"auto-retry-strategy": "Mode d’intervalle de nouvelle tentative",
	"auto-retry-strategy-static": "Fixe",
	"auto-retry-strategy-exponential": "Attente exponentielle",
	"auto-retry-interval": "Intervalle de base entre les tentatives",
	"auto-retry-interval-unit": "secondes",
	"auto-retry-max-attempts": "Nombre maximal de tentatives (0 = illimité)",
	"worker-max-retries": "Nombre max. de tentatives par pièce de worker",
	"auto-detect-low-speed-tasks": "Détecter automatiquement les tâches lentes",
	"auto-detect-low-speed-tasks-tips":
		"Si une tâche en cours reste sous le seuil de vitesse, elle est mise en pause puis relancée automatiquement.",
	"low-speed-threshold": "Seuil de faible vitesse",
	"low-speed-threshold-unit": "KB/s",
	"http-reliability": "Fiabilité HTTP / réseau",
	"http-reliability-tips":
		"Réglez le délai de connexion et la détection des transferts bloqués pour les téléchargements HTTP/HTTPS/FTP.",
	"connect-timeout": "Délai de connexion",
	"nzb-body-timeout": "Délai du corps NZB",
	"lowest-speed-limit": "Vitesse minimale",
	"lowest-speed-limit-timeout": "Délai de blocage",
	"lowest-speed-limit-help":
		"Si un téléchargement reste sous la vitesse minimale pendant le délai de blocage, le worker est interrompu puis (si les nouvelles tentatives automatiques sont activées) relancé. Mettez la limite à 0 pour désactiver.",
	"uri-selector": "Sélection du miroir",
	"uri-selector-feedback":
		"Rétroaction (abandonner les miroirs lents ou défaillants)",
	"uri-selector-inorder":
		"Dans l’ordre (essayer les miroirs de gauche à droite)",
	"uri-selector-adaptive": "Adaptatif (préférer le plus rapide)",
	"unit-seconds": "secondes",
	"unit-kib-per-sec": "KiB/s",
	storage: "Stockage",
	"storage-tips":
		"Comment le moteur réserve l’espace disque avant le téléchargement.",
	"file-allocation": "Allocation des fichiers",
	"file-allocation-falloc": "Préallocation (falloc)",
	"file-allocation-trunc": "Troncature (définir la longueur seulement)",
	"file-allocation-none": "Aucune (croissance à la demande)",
	"file-allocation-tips":
		"La préallocation réserve de vrais blocs de disque dès le départ pour éviter un ENOSPC en cours de téléchargement et réduire la fragmentation. Bascule sur la troncature sur les systèmes de fichiers qui ne la prennent pas en charge.",
	"new-task-show-downloading":
		"Montrer automatiquement les téléchargements après l'ajout d'une tâche",
	"no-confirm-before-delete-task":
		"Aucune confirmation n'est requise avant de supprimer la tâche",
	"auto-file-renaming": "Renommer automatiquement les fichiers en double",
	"auto-file-renaming-tips":
		"Si un fichier du même nom existe déjà, l’enregistrer sous « file.1.ext » au lieu de l’écraser",
	"use-remote-file-time":
		"Utiliser la date de modification du fichier distant à l’enregistrement",
	"use-remote-file-time-tips":
		"Définit la date de modification du fichier d’après l’horodatage du serveur distant",
	continue: "Continuer",
	"task-completed-notify": "Notifier à la fin d'un téléchargement",
	"clipboard-watch":
		"Surveiller le presse-papiers pour détecter les liens de téléchargement",
	"clipboard-watch-tips":
		"Lorsque vous copiez un lien magnet ou un lien de fichier n’importe où, risuko propose de le télécharger.",
	"clipboard-watch-ext": "Extensions de fichier surveillées",
	"clipboard-watch-ext-tips":
		"Seuls les liens http(s) se terminant par ces extensions déclenchent la proposition. Les liens magnet, ed2k et torrent la déclenchent toujours. Séparez-les par des espaces ou des virgules.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"risuko surveille désormais votre presse-papiers pour détecter les liens de téléchargement. Désactivez-le dans Paramètres → Basique.",
	"prevent-sleep-while-downloading":
		"Empêcher la mise en veille du système pendant le téléchargement",
	"prevent-sleep-while-downloading-tips":
		"Garder l'ordinateur éveillé tant qu'une tâche active est en cours",
	"shutdown-when-complete":
		"Éteindre l’ordinateur lorsque toutes les tâches sont terminées",
	"shutdown-when-complete-tips":
		"Une seule fois : éteint l’ordinateur dès que la file de téléchargement est vide",
	"shutdown-countdown-title": "Arrêt dans {{seconds}} s…",
	"shutdown-countdown-cancel": "Annuler",
	"shutdown-failed": "Échec de l’arrêt. Vérifiez les autorisations.",
	"auto-purge-record":
		"Purger l'historique de téléchargement lorsque vous quittez l'application",
	"purge-record-on-start":
		"Purger les enregistrements de téléchargement au démarrage de l’application",
	ui: "UI",
	appearance: "Mode d'apparence",
	"theme-auto": "Automatique",
	"theme-light": "Clair",
	"theme-dark": "Sombre",
	"font-family": "Police",
	"font-family-system": "Système",
	"font-family-rounded": "Arrondie",
	"font-family-serif": "Sérif",
	"font-family-mono": "Chasse fixe",
	"font-family-sample": "Le rapide renard brun télécharge plus vite.",
	"font-size": "Taille",
	"font-size-small": "Petite",
	"font-size-small-short": "Petite",
	"font-size-default": "Par défaut",
	"font-size-default-short": "Par défaut",
	"font-size-large": "Grande",
	"font-size-large-short": "Grande",
	"font-size-extra-large": "Très grande",
	"font-size-extra-large-short": "Très grande",
	"auto-hide-window": "Masquer automatiquement les fenêtres",
	"run-mode": "Exécuter en tant que",
	"run-mode-standard": "Application standard",
	"run-mode-tray": "Application de la zone de notification",
	"run-mode-hide-tray": "Masquer l'application de la barre d'état système",
	"tray-speedometer": "La barre de menus affiche la vitesse en temps réel",
	"show-progress-bar": "Afficher la barre de progression du téléchargement",
	language: "Langues",
	"change-language": "Changer la langue",
	"hide-app-menu":
		"Cacher le menu de l'application (Windows & Linux uniquement)",
	proxy: "Proxy",
	"proxy-http-profile": "Proxy HTTP / FTP / Usenet",
	"proxy-p2p-profile": "Proxy P2P",
	"enable-proxy": "Activer le Proxy",
	"enable-p2p-proxy": "Activer le proxy P2P",
	"proxy-p2p-tcp-server": "Serveur proxy TCP",
	"proxy-p2p-tcp-bypass": "Contournement du proxy TCP",
	"proxy-p2p-udp-server": "Serveur proxy UDP",
	"proxy-p2p-udp-server-placeholder":
		"Facultatif ; laissez vide pour utiliser le proxy P2P TCP",
	"proxy-p2p-udp-bypass": "Contournement du proxy UDP",
	"proxy-bypass-input-tips":
		"Contourner les paramètres de proxy pour ces hôtes et domaines, un par ligne",
	"proxy-scope-label": "Utilisé pour",
	"proxy-scope-download": "Télécharger",
	"proxy-scope-download-desc":
		"Faire passer le trafic de téléchargement par le proxy",
	"proxy-scope-update-app": "Mettre à jour l'application",
	"proxy-scope-update-app-desc":
		"Rechercher les mises à jour de l'application via le proxy",
	"proxy-scope-update-trackers": "Mettre à jour les trackers",
	"proxy-scope-update-trackers-desc":
		"Récupérer les listes de trackers BT via le proxy",
	"proxy-tips": "Afficher le manuel du proxy",
	"proxy-p2p-tips":
		"SOCKS5 ou SOCKS5H est requis pour DHT, les trackers UDP et uTP. Les proxys HTTP ne servent que pour les connexions P2P TCP.",
	"proxy-p2p-udp-tips":
		"Laissez le proxy UDP vide pour réutiliser la route P2P TCP. Le proxy UDP nécessite SOCKS5 ou SOCKS5H ; les proxys HTTP ne peuvent pas transporter l’UDP.",
	"proxy-p2p-restart-title": "Redémarrer les téléchargements P2P ?",
	"proxy-p2p-restart-confirm":
		"Modifier le proxy P2P redémarre les téléchargements pair à pair actifs. Les téléchargements HTTP, FTP et Usenet continuent.",
	doh: "DNS over HTTPS",
	"doh-tips":
		"Résoudre les noms d’hôte via un point de terminaison DNS chiffré plutôt que le résolveur de votre système",
	"doh-enable": "Activer DNS over HTTPS",
	"doh-provider": "Fournisseur",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Personnalisé",
	"doh-url": "URL du point de terminaison",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "Le point de terminaison DoH doit commencer par https://",
	"doh-bootstrap": "IP d’amorçage",
	"doh-bootstrap-placeholder":
		"IP pour joindre le point de terminaison, séparées par des virgules (facultatif)",
	"doh-fallback": "Se rabattre sur le DNS du système",
	"doh-fallback-tips":
		"Si une requête DoH échoue, résoudre via le résolveur du système plutôt que de faire échouer le téléchargement",
	"bt-tracker": "Serveurs Tracker",
	"bt-tracker-input-tips": "Serveur de suivi, un par ligne",
	"bt-tracker-tips": "Recommandé:",
	"bt-max-peers-per-torrent": "Nombre max. de pairs par torrent",
	"bt-max-peers-per-torrent-tips":
		"Nombre maximal de connexions de pairs simultanées par torrent. Des valeurs plus élevées améliorent la découverte de l’essaim (nécessite un redémarrage du moteur)",
	"bt-max-outstanding-per-peer": "Nombre max. de requêtes en attente par pair",
	"bt-max-outstanding-per-peer-tips":
		"Nombre maximal de requêtes de blocs envoyées en parallèle à chaque pair (plafond, jusqu’à 500). 0 utilise la valeur adaptative par défaut de 6 à 500 ; un budget de requêtes global au torrent est partagé entre les pairs actifs. Nécessite un redémarrage du moteur",
	"bt-max-connections": "Nombre max. de connexions de pairs",
	"bt-max-connections-tips":
		"Nombre total de connexions de pairs pour tous les torrents, de 20 à 5000. Le diminuer ne coupe jamais les connexions déjà ouvertes, cela retient seulement les nouvelles. Sous Linux, macOS et Android, la valeur effective est aussi plafonnée à la moitié de la limite de descripteurs de fichiers du système",
	"bt-ban-corrupt-peers":
		"Bannir les pairs qui envoient des données corrompues",
	"bt-ban-corrupt-peers-tips":
		"Bloque une adresse IP jusqu’à la fin de la session lorsque les pièces qu’elle a aidé à envoyer échouent de façon répétée à la vérification du hash. Plusieurs utilisateurs peuvent partager une même IP (opérateurs mobiles, réseaux partagés), un bannissement peut donc aussi couper des pairs innocents (nécessite un redémarrage du moteur)",
	"bt-ban-corrupt-strikes": "Pièces en échec avant bannissement",
	"bt-ban-corrupt-strikes-tips":
		"Nombre de pièces en échec auxquelles une adresse IP peut contribuer avant d’être bannie. Des valeurs plus élevées tolèrent une corruption occasionnelle (nécessite un redémarrage du moteur)",
	"bt-enable-upnp": "Redirection de port UPnP / NAT-PMP",
	"bt-enable-upnp-tips":
		"Ouvre automatiquement le port d’écoute BitTorrent sur le routeur via UPnP IGD, NAT-PMP ou PCP (nécessite un redémarrage du moteur)",
	"bt-upnp-lease": "Durée de bail du mappage de port (secondes)",
	"bt-upnp-lease-tips":
		"Durée de bail des mappages de port UPnP et NAT-PMP. Des baux plus courts permettent de se remettre plus vite d’un mappage périmé (nécessite un redémarrage du moteur)",
	"bt-enable-lsd": "Découverte de services locaux",
	"bt-enable-lsd-tips":
		"Découvre les pairs sur le réseau local via le multicast BEP-14 (nécessite un redémarrage du moteur)",
	"bt-encryption-policy": "Chiffrement du protocole",
	"bt-encryption-policy-tips":
		"Politique MSE/PE pour les connexions de pairs. « Préférer » essaie d’abord le chiffrement puis se rabat sur le texte en clair (nécessite un redémarrage du moteur)",
	"bt-encryption-plaintext": "Texte en clair uniquement",
	"bt-encryption-prefer": "Préférer le chiffrement",
	"bt-encryption-require": "Exiger le chiffrement",
	"bt-listen-v6": "Écouter sur IPv6",
	"bt-listen-v6-tips":
		"Ouvre un écouteur TCP IPv6 supplémentaire sur le même port (nécessite un redémarrage du moteur)",
	"sync-tracker-tips": "Sync",
	"auto-sync-tracker":
		"Mettre à jour automatiquement la liste des trackers chaque jour",
	"ed2k-server": "Serveurs eDonkey",
	"ed2k-server-input-tips": "Serveurs eDonkey, un par ligne (ip:port)",
	"ed2k-server-tips": "Les serveurs par défaut seront utilisés si vide",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Activer la découverte de sources via Kad",
	"ed2k-enable-kad-tips":
		"Découvrir des sources ED2K via le réseau eMule Kad (nécessite un redémarrage du moteur)",
	"ed2k-kad-port": "Port UDP de Kad",
	"ed2k-kad-port-tips":
		"Port UDP utilisé pour la découverte via Kad. Les valeurs valides vont de 1 à 65535 (nécessite un redémarrage du moteur)",
	"ed2k-kad-port-invalid":
		"Le port UDP de Kad doit être un entier compris entre 1 et 65535",
	"listen-port-invalid":
		"Le port BT doit être un port de 1 à 65535, une plage telle que 6881-6889, ou une liste de ceux-ci séparés par des virgules",
	"dht-listen-port-invalid":
		"Le port DHT doit être un port de 1 à 65535, une plage telle que 6881-6889, ou une liste de ceux-ci séparés par des virgules",
	"ed2k-port-invalid": "Le port ED2K doit être un entier de 1 à 65535",
	"ed2k-enable-upload": "Partager des fichiers via ED2K",
	"ed2k-enable-upload-tips":
		"Envoyer aux autres pairs les fichiers terminés et les parties vérifiées des téléchargements ED2K en cours. Kad peut encore répertorier un fichier que vous ne partagez plus pendant 5 heures au maximum (nécessite un redémarrage du moteur)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Identifiants par défaut pour les téléchargements FTP, FTPS et SFTP",
	"ftp-username": "Nom d’utilisateur FTP",
	"ftp-password": "Mot de passe FTP",
	"sftp-private-key": "Chemin de la clé privée SSH",
	"sftp-private-key-content": "Contenu de la clé privée SSH",
	"sftp-private-key-tips":
		"Chemin du fichier de clé privée PEM, p. ex. ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips": "Collez ici le contenu de la clé privée PEM",
	"sftp-key-passphrase": "Phrase secrète de la clé",
	"sftp-key-passphrase-tips":
		"Laissez vide si la clé n’a pas de phrase secrète",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Format de sortie",
	"media-settings": "Sites de médias (yt-dlp)",
	"media-settings-tips":
		"Paramètres par défaut des téléchargements depuis les sites de médias (YouTube, Vimeo, Bilibili, etc.). Nécessite yt-dlp dans le PATH ; ffmpeg est requis pour fusionner des flux vidéo et audio séparés.",
	"media-format": "Format préféré",
	"media-format-placeholder":
		"ex. bestvideo+bestaudio/best (chaîne de format yt-dlp)",
	"media-format-tips":
		"Sélecteur de format yt-dlp. Laissez vide pour la meilleure qualité. Exemples : bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "Ports d'écoute",
	"bt-port": "Ports d'écoute BT",
	"dht-port": "Ports d'écoute DHT",
	"ed2k-port": "Port d’écoute ed2k",
	security: "Sécurité",
	rpc: "RPC",
	"rpc-host": "Adresse du serveur",
	"rpc-host-local": "Cet appareil uniquement",
	"rpc-host-all": "Toutes les interfaces réseau",
	"rpc-host-custom": "Personnalisé ({{host}})",
	"rpc-host-exposed-tips":
		"Les autres appareils de votre réseau peuvent accéder au port RPC, ainsi qu’au port PeerBanHelper lorsqu’il est activé. Toute personne connaissant le secret peut ajouter et contrôler des téléchargements.",
	"rpc-listen-port": "Port d'écoute RPC",
	"rpc-secret": "RPC Secret",
	"rpc-secret-tips": "Voir le manuel secret RPC",
	"rpc-allow-any-dir":
		"Autoriser les clients RPC à utiliser n’importe quel chemin local",
	"rpc-allow-any-dir-tips":
		"Si désactivé, les téléchargements ajoutés via RPC ne peuvent être enregistrés que dans le dossier de téléchargement, ses sous-dossiers et vos dossiers de catégories et de routage, et les clients RPC ne peuvent pas faire lire à l’application un fichier de cookies, netrc ou de clé SSH.",
	pbh: "PeerBanHelper",
	"pbh-enable": "Activer l’API PeerBanHelper",
	"pbh-enable-tips":
		"Expose un écouteur JSON-RPC compatible Aria2Next pour que PeerBanHelper puisse surveiller et bannir les pairs BitTorrent",
	"pbh-listen-port": "Port d’écoute PeerBanHelper",
	"pbh-rpc-secret": "Jeton RPC PeerBanHelper",
	"generate-pbh-rpc-secret": "Générer un nouveau jeton",
	"pbh-endpoint-hint":
		"Dans PeerBanHelper, ajoutez un téléchargeur Aria2Next pointant vers http://127.0.0.1:{{port}}/jsonrpc",
	"randomize-port": "Choisir un port aléatoire",
	"generate-rpc-secret": "Générer un nouveau secret RPC",
	"copy-rpc-url": "Copier l’URL RPC",
	"engine-overrides": "Surcharges du moteur (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Avancé : définissez des clés d'options du moteur arbitraires sous forme d'objet JSON. Les changements prennent effet après redémarrage.",
	"engine-overrides-invalid":
		"Les surcharges du moteur doivent être un objet JSON valide.",
	"engine-overrides-reserved-keys":
		"Clés réservées de surcharge du moteur ignorées : {{keys}}",
	"engine-overrides-too-large":
		"Les surcharges du moteur sont trop volumineuses : au maximum 64 clés, 64 caractères par clé et 2048 caractères par valeur.",
	developer: "Développeur",
	"user-agent": "User-Agent",
	"mock-user-agent": "Mock User-Agent",
	cookies: "Cookies",
	"cookies-tips":
		"Charge les cookies depuis un fichier cookies.txt au format Netscape/Mozilla (accepte aussi la sortie de curl/wget).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Identifiants de domaine enregistrés",
	"saved-cookies-tips":
		"Cookies et User-Agent importés depuis un navigateur pour contourner Cloudflare ou d’autres téléchargements protégés par authentification. Risuko les réutilise pour les nouveaux téléchargements du même domaine jusqu’à leur expiration ou leur invalidation.",
	"saved-cookies-empty": "Aucun identifiant enregistré pour le moment.",
	"saved-cookies-host": "Domaine",
	"saved-cookies-browser": "Navigateur",
	"saved-cookies-count": "Cookies",
	"saved-cookies-imported": "Importé",
	"saved-cookies-actions": "Actions",
	"saved-cookies-clear": "Tout effacer",
	"saved-cookies-confirm-clear":
		"Supprimer tous les identifiants de domaine enregistrés ? Les prochains téléchargements sur ces sites pourraient échouer tant que vous ne les aurez pas réimportés.",
	"saved-cookies-delete": "Supprimer",
	netrc: "Identifiants .netrc",
	"netrc-tips":
		"Injecte l’authentification HTTP Basic d’un fichier .netrc lorsque l’URL ne contient pas d’identifiants. Par défaut : ~/.netrc (ou %USERPROFILE%/_netrc sous Windows).",
	"netrc-path-placeholder": "Remplacer le chemin de .netrc (facultatif)",
	"no-netrc": "Désactiver la recherche .netrc",
	"no-netrc-tips":
		"Ignore complètement .netrc. Utile lorsque le fichier contient des identifiants sans rapport avec les téléchargements.",
	"app-log-path": "Chemin des logs",
	"log-dir-override-placeholder":
		"Répertoire de journaux personnalisé (laisser vide pour la valeur par défaut)",
	"log-dir-override-tips":
		"Les journaux sont actuellement écrits dans {{path}}. Définir un répertoire personnalisé (p. ex. dans Téléchargements) les rend visibles depuis n’importe quel gestionnaire de fichiers. Les changements prennent effet au prochain redémarrage de l’application.",
	"download-session-path": "Chemin de la session de téléchargement",
	"session-reset": "Réinitialiser la session de téléchargement",
	"session-reset-confirm":
		"Voulez-vous vraiment réinitialiser la session de téléchargement ?",
	"factory-reset": "Réinitialisation",
	"factory-reset-confirm":
		"Êtes vous sûr de vouloir réinitialiser les paramètres",
	"download-protocol": "Protocole",
	"protocols-default-client":
		"Définir comme client par défaut pour les protocoles suivants",
	"protocols-magnet": "Magnet [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc:// adcs:// dchub:// nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "Intégration du démon giFT",
	"gift-enabled": "Activer la délégation giFT",
	"gift-host": "Hôte giFT",
	"gift-port": "Port giFT",
	"gift-not-detected": "Démon giFT introuvable à l'hôte:port configuré.",
	"browser-extensions": "Extensions",
	"baidu-exporter": "BaiduExporter",
	"browser-extensions-tips": "Fourni par la communauté, ",
	"baidu-exporter-help": "Cliquez ici pour voir l'utilisation",
	"auto-update": "Mettre à jour",
	"auto-check-update": "Mise à jour automatique",
	"last-check-update-time": "Dernière vérification des mises à jour",
	"completion-script": "Script à la fin",
	"completion-script-enabled": "Exécuter un script à la fin du téléchargement",
	"completion-script-tips":
		"Exécuté en sandbox via tauri-plugin-shell. S’exécute une fois à la fin de chaque tâche (succès ou erreur).",
	"completion-script-command": "Commande",
	"completion-script-command-placeholder":
		"ex. /usr/local/bin/notify.sh ou C:\\Tools\\notify.exe",
	"completion-script-args": "Paramètres",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Variables: {path}, {hash}, {status}. Également exposées via les variables d’environnement RISUKO_PATH, RISUKO_HASH, RISUKO_STATUS.",
	"completion-script-timeout": "Délai d’attente (ms)",
	"completion-script-test": "Tester le script",
	"completion-script-test-ok": "Test du script réussi",
	"completion-script-test-fail": "Échec du test du script",
	"completion-script-empty": "Veuillez d’abord définir une commande",
	"completion-script-not-approved":
		"Le script de fin n’a pas été autorisé, donc rien n’a été enregistré.",
	"not-saved": "Préférences non enregistrées",
	"not-saved-confirm":
		"Les préférences modifiées seront perdues, êtes-vous sûr de partir ?",
	"saved-credentials": "Identifiants enregistrés",
	"saved-credentials-description":
		"Gérez les identifiants d’authentification enregistrés pour les téléchargements",
	"no-saved-credentials": "Aucun identifiant enregistré",
	"credential-host": "Hôte",
	"credential-label": "Nom",
	"credential-protocol": "Protocole",
	"credential-last-used": "Dernière utilisation",
	"credential-delete": "Supprimer l’identifiant",
	"credential-delete-confirm":
		"Voulez-vous vraiment supprimer cet identifiant enregistré ?",
	"vault-enabled": "Secrets stockés dans le trousseau du système",
	"vault-disabled":
		"Trousseau du système indisponible — secrets stockés en clair",
	"credential-encrypted-badge": "Stocké dans le trousseau du système",
	"credential-vault-unavailable":
		"Trousseau du système inaccessible ; les nouveaux identifiants seront enregistrés en clair",
	"speed-limit-enabled": "Activer les limites de vitesse",
	"speed-limit-enabled-description":
		"Applique les limites ci-dessous. Si désactivé, les téléchargements s’exécutent à pleine vitesse ; les valeurs sont conservées.",
	"appearance-theme": "Thème",
	"appearance-typography": "Typographie",
	"appearance-window": "Fenêtre",
	"task-list-style": "Liste des tâches",
	"task-list-style-description":
		"Comment les téléchargements sont affichés dans la liste des tâches.",
	"task-list-style-compact": "Compacte",
	"task-list-style-card": "Cartes",
	"sidebar-collapsed": "Réduire la barre latérale",
	"sidebar-collapsed-description":
		"N’afficher que les icônes dans la barre latérale.",
};
