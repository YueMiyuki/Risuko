export default {
	basic: "Básico",
	advanced: "Avançado",
	cloudSinks: "Destinos na nuvem",
	usenet: "Usenet",
	search: "Pesquisar",
	"search-placeholder": "Pesquisar em todas as preferências",
	"search-all-tabs": "Todas as preferências",
	"search-results": "{{count}} configurações correspondentes",
	"search-no-results": "Nenhuma configuração correspondente encontrada",
	"search-clear": "Limpar pesquisa",
	"search-go-to-tab": "Abrir configuração",
	"search-more-results": "Refine a pesquisa para ver mais resultados",
	"usenet-tips":
		"Gerencie provedores NNTP e os padrões de limpeza segura de arquivos compactados.",
	"usenet-profiles": "Perfis de provedor",
	"usenet-profiles-tips":
		"Configure a prioridade, a segurança, a autenticação e os limites de conexão dos provedores.",
	"usenet-add-profile": "Adicionar perfil",
	"usenet-no-profiles": "Nenhum perfil de provedor configurado.",
	"usenet-no-profiles-tips":
		"Adicione pelo menos um provedor antes de importar um download NZB.",
	"usenet-edit-profile": "Editar perfil",
	"usenet-profile-dialog-tips":
		"As configurações do perfil podem ser sincronizadas entre dispositivos. As credenciais permanecem locais neste dispositivo.",
	"usenet-profile-dialog-tips-vault":
		"As configurações do perfil podem ser sincronizadas entre dispositivos. As credenciais ficam no chaveiro seguro deste dispositivo.",
	"usenet-profile-name": "Nome do perfil",
	"usenet-profile-name-placeholder": "Provedor principal",
	"usenet-profile-host": "Host NNTP",
	"usenet-profile-port": "Porta",
	"usenet-security": "Segurança da conexão",
	"usenet-tls": "TLS implícito",
	"usenet-tls-short": "TLS",
	"usenet-starttls": "STARTTLS",
	"usenet-plain": "NNTP simples",
	"usenet-authentication": "Autenticação",
	"usenet-auth-credentials": "Usuário e senha",
	"usenet-auth-anonymous": "Anônima",
	"usenet-username": "Usuário",
	"usenet-password": "Senha",
	"usenet-credentials-saved": "Credenciais salvas",
	"usenet-credentials-unavailable": "Status das credenciais indisponível",
	"usenet-credentials-saved-tips":
		"As credenciais já estão salvas. Deixe os dois campos vazios para mantê-las inalteradas.",
	"usenet-plain-warning-title": "Conexão sem criptografia",
	"usenet-plain-opt-in":
		"Entendo e quero permitir NNTP simples para este perfil.",
	"usenet-plain-warning":
		"O NNTP simples não criptografa as credenciais nem os dados baixados. Use-o apenas quando o provedor exigir explicitamente.",
	"usenet-enabled": "Ativado",
	"usenet-enabled-tips":
		"Perfis desativados são mantidos, mas não são usados nos downloads.",
	"usenet-disabled": "Desativado",
	"usenet-priority": "Prioridade",
	"usenet-priority-tips":
		"Valores menores são tentados primeiro. Prioridades iguais dividem a carga.",
	"usenet-priority-value": "Prioridade {{value}}",
	"usenet-max-connections": "Máximo de conexões",
	"usenet-max-connections-tips":
		"O limite de conexões deste perfil de provedor.",
	"usenet-connections-value": "{{value}} conexões",
	"usenet-test": "Testar",
	"usenet-edit": "Editar perfil",
	"usenet-remove": "Remover",
	"usenet-remove-title": "Remover perfil de provedor?",
	"usenet-remove-confirm":
		"{{name}} não estará mais disponível para downloads. As credenciais salvas também serão removidas deste dispositivo.",
	"usenet-profile-saved": "Perfil de provedor salvo.",
	"usenet-profile-removed": "{{name}} foi removido.",
	"usenet-test-success": "Conectado a {{name}}.",
	"usenet-test-failed": "Falha na conexão: {{message}}",
	"usenet-validation-name": "Informe um nome de perfil.",
	"usenet-validation-host": "Informe um host NNTP.",
	"usenet-validation-port": "A porta deve estar entre 1 e 65535.",
	"usenet-validation-plain":
		"Confirme o aviso de NNTP simples antes de salvar.",
	"usenet-validation-priority": "A prioridade deve ser zero ou maior.",
	"usenet-validation-connections": "As conexões devem estar entre 1 e 128.",
	"usenet-validation-credentials":
		"Informe usuário e senha, ou escolha a autenticação anônima.",
	"usenet-validation-credential-status":
		"O status das credenciais está indisponível. Tente novamente antes de alterar este perfil.",
	"usenet-cleanup": "Limpeza após sucesso",
	"usenet-cleanup-tips":
		"A limpeza só é executada após a montagem completa e a extração bem-sucedida.",
	"usenet-cleanup-action": "Após o sucesso verificado",
	"usenet-cleanup-keep": "Manter todos os arquivos",
	"usenet-cleanup-keep-tips":
		"Mantém os arquivos PAR2 e todos os volumes do arquivo compactado.",
	"usenet-cleanup-par2": "Excluir arquivos PAR2",
	"usenet-cleanup-par2-tips":
		"Remove os arquivos de paridade, mas mantém os volumes baixados do arquivo compactado.",
	"usenet-cleanup-all": "Excluir PAR2 e volumes do arquivo compactado",
	"usenet-cleanup-all-tips":
		"Remove os arquivos de paridade e os volumes do arquivo compactado após a extração bem-sucedida.",
	"usenet-archive-safety": "Segurança de arquivos compactados",
	"usenet-limits-defaults":
		"Os limites aplicados a toda a tarefa protegem o armazenamento e o processamento durante a verificação e a extração.",
	"usenet-desktop-limits": "Limites no desktop",
	"usenet-android-limits": "Limites no Android",
	"usenet-limits-summary":
		"{{entries}} entradas · {{total}} de expansão total · {{perEntry}} por entrada",
	"usenet-limits-secondary-summary":
		"proporção de {{ratio}}:1 · {{depth}} níveis de aninhamento · reserva de {{reserve}} de espaço livre · {{hours}} horas ativas",
	"usenet-customize-limits": "Personalizar limites",
	"usenet-hide-limits": "Ocultar limites",
	"usenet-limits-adjusted": "Os limites sincronizados foram ajustados",
	"usenet-limits-adjusted-tips":
		"Um ou mais valores excederam o limite máximo desta plataforma e foram reduzidos.",
	"usenet-max-entries": "Máximo de entradas",
	"usenet-max-expanded": "Tamanho total expandido",
	"usenet-max-entry": "Tamanho de uma entrada",
	"usenet-max-depth": "Profundidade de aninhamento",
	"usenet-max-ratio": "Taxa de compressão",
	"usenet-free-space-reserve": "Reserva de espaço livre",
	"usenet-max-active-time": "Tempo de processamento ativo",
	"usenet-unit-entries": "entradas",
	"usenet-unit-levels": "níveis",
	"usenet-unit-hours": "horas",
	"usenet-limits-hard-cap":
		"Os valores não podem exceder quatro vezes os padrões da plataforma. As substituições por tarefa continuam sendo confirmadas separadamente.",
	"usenet-restore-limits": "Restaurar padrões da plataforma",
	"usenet-limits-restored":
		"Limites de arquivos compactados restaurados para os padrões da plataforma.",
	save: "Salvar & Aplicar",
	"save-success-message": "Salvar preferências com sucesso",
	"save-fail-message": "Salvar preferências falhadas",
	discard: "Descartar",
	"history-directories": "Diretórios de download recentes",
	"favorite-directory": "Diretório favorito",
	"unfavorite-directory": "Remover diretório dos favoritos",
	"remove-history-directory": "Remover diretório do histórico",
	startup: "Inicialização",
	"open-at-login": "Abra no login",
	"keep-window-state": "Restaurar tamanho e posição da janela",
	"auto-resume-all": "Retomar automaticamente todas as tarefas não finalizadas",
	"default-dir": "Diretório Padrão",
	"file-category-dirs": "Caminhos por categoria",
	"file-category-dirs-tips":
		"Substitua o caminho de download padrão para tipos de arquivo específicos. Deixe vazio para usar o caminho padrão.",
	"task-routing-rules": "Regras de roteamento de tarefas",
	"task-routing-rules-tips":
		"Direciona tarefas para diretórios com base em padrões de nome de arquivo. A primeira regra correspondente prevalece.",
	"task-routing-rule-pattern-placeholder": "Padrão (ex.: *.mkv)",
	"task-routing-rule-label-placeholder": "Etiqueta",
	"task-routing-rule-dir-placeholder": "Diretório",
	"task-routing-rule-add": "Adicionar regra",
	"file-category-music": "Música",
	"file-category-video": "Vídeo",
	"file-category-image": "Imagem",
	"file-category-document": "Documento",
	"file-category-compressed": "Compactado",
	"file-category-program": "Programa",
	"mas-default-dir-tips":
		"Devido às restrições de permissões do sandbox da App Store, recomenda-se que o diretório de download padrão seja definido como o diretório de downloads",
	"transfer-settings": "Transmissão",
	"transfer-speed-upload": "limite de envio",
	"transfer-speed-download": "limite de transferência",
	"transfer-speed-unlimited": "Ilimitado",
	"bt-settings": "BitTorrent",
	"bt-save-metadata": "Salvar link magnético como arquivo torrent",
	"bt-create-subfolder": "Agrupar torrents de vários arquivos em uma subpasta",
	"bt-create-subfolder-tips":
		"Quando ativado, downloads de BitTorrent e de links magnéticos com vários arquivos são colocados em uma pasta com o nome do torrent. Quando desativado, os arquivos são gravados diretamente no diretório de download.",
	"bt-upload-rate-limit": "Limite de envio do BitTorrent",
	"bt-upload-rate-limit-tips":
		"Limita os envios do BitTorrent além do limite global de envio. 0 significa nenhum limite separado.",
	"bt-auto-download-content":
		"Baixar automaticamente o conteúdo de links magnéticos e de torrents",
	"bt-force-encryption": "Forçar criptografia BT",
	"keep-seeding": "Continue semeando até pará-lo manualmente",
	"seed-ratio": "Proporção de sementes",
	"seed-time": "Hora da Semente",
	"seed-time-unit": "minutos",
	"task-manage": "Gerenciador de Tarefas",
	"max-concurrent-downloads": "Máximo de tarefas ativas",
	"connections-per-task": "Conexões por tarefa",
	"max-connection-per-server": "Máximo de conexões por servidor",
	"auto-retry": "Tentar novamente as tarefas com falha automaticamente",
	"auto-retry-strategy": "Modo de intervalo entre tentativas",
	"auto-retry-strategy-static": "Fixo",
	"auto-retry-strategy-exponential": "Recuo exponencial",
	"auto-retry-interval": "Intervalo base entre tentativas",
	"auto-retry-interval-unit": "segundos",
	"auto-retry-max-attempts": "Máximo de tentativas (0 = ilimitado)",
	"worker-max-retries": "Máximo de tentativas por peça em cada worker",
	"auto-detect-low-speed-tasks": "Detectar tarefas lentas automaticamente",
	"auto-detect-low-speed-tasks-tips":
		"Se uma tarefa em execução permanecer abaixo do limite de velocidade, ela será pausada e reiniciada automaticamente.",
	"low-speed-threshold": "Limite de baixa velocidade",
	"low-speed-threshold-unit": "KB/s",
	"http-reliability": "Confiabilidade de HTTP / rede",
	"http-reliability-tips":
		"Ajuste o tempo limite de conexão e a detecção de transferências paralisadas em downloads HTTP/HTTPS/FTP.",
	"connect-timeout": "Tempo limite de conexão",
	"nzb-body-timeout": "Tempo limite do corpo do NZB",
	"lowest-speed-limit": "Limite mínimo de velocidade",
	"lowest-speed-limit-timeout": "Tempo limite de paralisação",
	"lowest-speed-limit-help":
		"Se um download permanecer abaixo do limite mínimo de velocidade durante o tempo limite de paralisação, o worker é interrompido e (se a repetição automática estiver ativada) reiniciado. Defina o limite como 0 para desativar.",
	"uri-selector": "Seleção de espelho",
	"uri-selector-feedback": "Feedback (descarta espelhos lentos ou com falha)",
	"uri-selector-inorder":
		"Em ordem (tenta os espelhos da esquerda para a direita)",
	"uri-selector-adaptive": "Adaptativo (prefere o mais rápido)",
	"unit-seconds": "segundos",
	"unit-kib-per-sec": "KiB/s",
	storage: "Armazenamento",
	"storage-tips": "Como o motor reserva espaço em disco antes de baixar.",
	"file-allocation": "Alocação de arquivo",
	"file-allocation-falloc": "Pré-alocar (falloc)",
	"file-allocation-trunc": "Truncar (apenas define o tamanho)",
	"file-allocation-none": "Nenhuma (cresce conforme necessário)",
	"file-allocation-tips":
		"A pré-alocação reserva blocos de disco reais antecipadamente para evitar ENOSPC durante o download e reduzir a fragmentação. Recorre ao truncamento em sistemas de arquivos sem suporte.",
	"new-task-show-downloading":
		"Auto exibir progresso depois de adicionar uma tarefa",
	"no-confirm-before-delete-task":
		"Nenhuma confirmação é necessária antes de excluir a tarefa",
	"auto-file-renaming": "Renomear arquivos duplicados automaticamente",
	"auto-file-renaming-tips":
		'Quando já existir um arquivo com o mesmo nome, salva como "arquivo.1.ext" em vez de sobrescrevê-lo',
	"use-remote-file-time":
		"Usar a data de modificação do arquivo remoto ao salvar",
	"use-remote-file-time-tips":
		"Define a data de modificação do arquivo com o carimbo de data/hora do servidor remoto",
	continue: "Continuar",
	"task-completed-notify": "Notificação após o download ser completado",
	"clipboard-watch":
		"Monitorar a área de transferência em busca de links de download",
	"clipboard-watch-tips":
		"Ao copiar um link magnético ou de arquivo em qualquer lugar, o risuko oferece baixá-lo.",
	"clipboard-watch-ext": "Extensões de arquivo monitoradas",
	"clipboard-watch-ext-tips":
		"Apenas links http(s) terminados nessas extensões acionam o aviso. Links magnet, ed2k e torrent sempre acionam. Separe com espaços ou vírgulas.",
	"clipboard-watch-ext-placeholder": "zip  iso  mp4  pdf",
	"clipboard-watch-notice":
		"O risuko agora monitora a área de transferência em busca de links de download. Desative em Preferências → Básico.",
	"prevent-sleep-while-downloading":
		"Impedir que o sistema entre em suspensão durante o download",
	"prevent-sleep-while-downloading-tips":
		"Manter o computador ativo enquanto houver uma tarefa em andamento",
	"shutdown-when-complete":
		"Desligar o computador quando todas as tarefas forem concluídas",
	"shutdown-when-complete-tips":
		"Uma única vez: desliga o computador quando a fila de downloads estiver vazia",
	"shutdown-countdown-title": "Desligando em {{seconds}}s…",
	"shutdown-countdown-cancel": "Cancelar",
	"shutdown-failed": "Falha ao desligar. Verifique as permissões.",
	"auto-purge-record":
		"Auto remover registro de download quando o app for finalizado",
	"purge-record-on-start": "Limpar os registros de download ao iniciar o app",
	ui: "UI",
	appearance: "Aparência",
	"theme-auto": "Automático",
	"theme-light": "Clara",
	"theme-dark": "Escura",
	"font-family": "Fonte",
	"font-family-system": "Sistema",
	"font-family-rounded": "Arredondada",
	"font-family-serif": "Serifada",
	"font-family-mono": "Monoespaçada",
	"font-family-sample": "A rápida raposa marrom baixa mais rápido.",
	"font-size": "Tamanho",
	"font-size-small": "Pequeno",
	"font-size-small-short": "Pequeno",
	"font-size-default": "Padrão",
	"font-size-default-short": "Padrão",
	"font-size-large": "Grande",
	"font-size-large-short": "Grande",
	"font-size-extra-large": "Muito grande",
	"font-size-extra-large-short": "Muito grande",
	"auto-hide-window": "Ocultar janelas automaticamente",
	"run-mode": "Correr como",
	"run-mode-standard": "Aplicação padrão",
	"run-mode-tray": "Aplicativo de bandeja",
	"run-mode-hide-tray": "Ocultar aplicativo da bandeja",
	"tray-speedometer":
		"A bandeja da barra de menus mostra a velocidade em tempo real",
	"show-progress-bar": "Mostrar barra de progresso de download",
	language: "Idioma",
	"change-language": "Mudar o Idioma",
	"hide-app-menu": "Ocultar o Menu do App (Windows & Linux apenas)",
	proxy: "Proxy",
	"proxy-http-profile": "Proxy de HTTP / FTP / Usenet",
	"proxy-p2p-profile": "Proxy P2P",
	"enable-proxy": "Habilitar Proxy",
	"enable-p2p-proxy": "Ativar proxy P2P",
	"proxy-p2p-tcp-server": "Servidor proxy TCP",
	"proxy-p2p-tcp-bypass": "Exceções do proxy TCP",
	"proxy-p2p-udp-server": "Servidor proxy UDP",
	"proxy-p2p-udp-server-placeholder":
		"Opcional; deixe em branco para usar o proxy P2P TCP",
	"proxy-p2p-udp-bypass": "Exceções do proxy UDP",
	"proxy-bypass-input-tips":
		"Ignorar configurações de proxy para esses hosts e domínios, um por linha",
	"proxy-scope-label": "Usado para",
	"proxy-scope-download": "Download",
	"proxy-scope-download-desc": "Roteia o tráfego de download pelo proxy",
	"proxy-scope-update-app": "Atualizar Aplicativo",
	"proxy-scope-update-app-desc":
		"Verifica atualizações do aplicativo pelo proxy",
	"proxy-scope-update-trackers": "Atualizar rastreadores",
	"proxy-scope-update-trackers-desc":
		"Busca listas de rastreadores BT pelo proxy",
	"proxy-tips": "Exibir manual do proxy",
	"proxy-p2p-tips":
		"SOCKS5 ou SOCKS5H é necessário para DHT, rastreadores UDP e uTP. Proxies HTTP são usados apenas em conexões P2P TCP.",
	"proxy-p2p-udp-tips":
		"Deixe o proxy UDP em branco para reutilizar a rota P2P TCP. O proxy UDP exige SOCKS5 ou SOCKS5H; proxies HTTP não transportam UDP.",
	"proxy-p2p-restart-title": "Reiniciar os downloads P2P?",
	"proxy-p2p-restart-confirm":
		"Alterar o proxy P2P reinicia os downloads ponto a ponto ativos. Os downloads HTTP, FTP e Usenet continuam em execução.",
	doh: "DNS sobre HTTPS",
	"doh-tips":
		"Resolve nomes de host por um endpoint DNS criptografado em vez do resolvedor do sistema",
	"doh-enable": "Ativar DNS sobre HTTPS",
	"doh-provider": "Provedor",
	"doh-provider-cloudflare": "Cloudflare",
	"doh-provider-google": "Google",
	"doh-provider-quad9": "Quad9",
	"doh-provider-custom": "Personalizado",
	"doh-url": "URL do endpoint",
	"doh-url-placeholder": "https://example.com/dns-query",
	"doh-url-invalid": "O endpoint DoH deve começar com https://",
	"doh-bootstrap": "IPs de bootstrap",
	"doh-bootstrap-placeholder":
		"IPs para alcançar o endpoint, separados por vírgula (opcional)",
	"doh-fallback": "Recorrer ao DNS do sistema",
	"doh-fallback-tips":
		"Se uma consulta DoH falhar, resolve pelo resolvedor do sistema em vez de fazer o download falhar",
	"bt-tracker": "Rastreadores",
	"bt-tracker-input-tips": "servidor rastreador, um por linha",
	"bt-tracker-tips": "Recomendado:",
	"bt-max-peers-per-torrent": "Máximo de pares por torrent",
	"bt-max-peers-per-torrent-tips":
		"Máximo de conexões simultâneas com pares por torrent. Valores maiores melhoram a descoberta do swarm (requer reinicialização do motor)",
	"bt-max-outstanding-per-peer": "Máximo de requisições pendentes por par",
	"bt-max-outstanding-per-peer-tips":
		"Máximo de requisições de blocos simultâneas em pipeline para cada par (limite de até 500). 0 usa o padrão adaptativo de 6–500; um orçamento de requisições por torrent é compartilhado entre os pares ativos. Requer reinicialização do motor",
	"bt-max-connections": "Máximo de conexões com pares",
	"bt-max-connections-tips":
		"Total de conexões com pares em todos os torrents, de 20 a 5000. Reduzir o valor nunca derruba conexões já abertas, apenas impede novas. No Linux, macOS e Android, o valor efetivo também é limitado à metade do limite de descritores de arquivo do sistema",
	"bt-ban-corrupt-peers": "Banir pares que enviam dados corrompidos",
	"bt-ban-corrupt-peers-tips":
		"Bloqueia um endereço IP pelo resto da sessão quando as peças que ele ajudou a enviar continuam falhando na verificação de hash. Muitos usuários podem compartilhar um mesmo IP (operadoras de celular, redes compartilhadas), então um banimento também pode isolar pares inocentes (requer reinicialização do motor)",
	"bt-ban-corrupt-strikes": "Peças com falha antes do banimento",
	"bt-ban-corrupt-strikes-tips":
		"Quantas peças com falha um endereço IP pode causar antes de ser banido. Valores maiores toleram corrupção ocasional (requer reinicialização do motor)",
	"bt-enable-upnp": "Redirecionamento de portas UPnP / NAT-PMP",
	"bt-enable-upnp-tips":
		"Abre automaticamente a porta de escuta do BitTorrent no roteador via UPnP IGD, NAT-PMP ou PCP (requer reinicialização do motor)",
	"bt-upnp-lease": "Duração do mapeamento de porta (segundos)",
	"bt-upnp-lease-tips":
		"Duração da concessão dos mapeamentos de porta UPnP e NAT-PMP. Concessões mais curtas se recuperam mais rápido de mapeamentos obsoletos (requer reinicialização do motor)",
	"bt-enable-lsd": "Descoberta de serviço local (LSD)",
	"bt-enable-lsd-tips":
		"Descobre pares na rede local via multicast BEP-14 (requer reinicialização do motor)",
	"bt-encryption-policy": "Criptografia de protocolo",
	"bt-encryption-policy-tips":
		"Política MSE/PE para conexões com pares. 'Preferir criptografia' tenta a criptografia primeiro e recorre ao texto simples (requer reinicialização do motor)",
	"bt-encryption-plaintext": "Somente texto simples",
	"bt-encryption-prefer": "Preferir criptografia",
	"bt-encryption-require": "Exigir criptografia",
	"bt-listen-v6": "Escutar em IPv6",
	"bt-listen-v6-tips":
		"Vincula um listener TCP IPv6 adicional na mesma porta (requer reinicialização do motor)",
	"sync-tracker-tips": "Sincronizar",
	"auto-sync-tracker":
		"Atualize a lista de rastreadores todos os dias automaticamente",
	"ed2k-server": "Servidores eDonkey",
	"ed2k-server-input-tips": "servidores eDonkey, um por linha (ip:porta)",
	"ed2k-server-tips": "Os servidores padrão serão usados se estiver vazio",
	"ed2k-kad": "eMule Kad",
	"ed2k-enable-kad": "Ativar descoberta de fontes via Kad",
	"ed2k-enable-kad-tips":
		"Descubra fontes ED2K pela rede eMule Kad (requer reinicialização do motor)",
	"ed2k-kad-port": "Porta UDP do Kad",
	"ed2k-kad-port-tips":
		"Porta UDP usada para descoberta via Kad. Os valores válidos são 1-65535 (requer reinicialização do motor)",
	"ed2k-kad-port-invalid":
		"A porta UDP do Kad deve ser um número inteiro de 1 a 65535",
	"listen-port-invalid":
		"A porta BT deve ser uma porta de 1 a 65535, um intervalo como 6881-6889 ou uma lista dessas separada por vírgulas",
	"dht-listen-port-invalid":
		"A porta DHT deve ser uma porta de 1 a 65535, um intervalo como 6881-6889 ou uma lista dessas separada por vírgulas",
	"ed2k-port-invalid": "A porta ED2K deve ser um número inteiro de 1 a 65535",
	"ed2k-enable-upload": "Compartilhar arquivos via ED2K",
	"ed2k-enable-upload-tips":
		"Envia a outros pares os arquivos concluídos e as partes verificadas dos downloads ED2K em andamento. O Kad pode continuar listando um arquivo que você parou de compartilhar por até 5 horas (requer reinicialização do motor)",
	"ftp-sftp-settings": "FTP / SFTP",
	"ftp-sftp-settings-tips":
		"Credenciais padrão para downloads FTP, FTPS e SFTP",
	"ftp-username": "Usuário FTP",
	"ftp-password": "Senha FTP",
	"sftp-private-key": "Caminho da chave privada SSH",
	"sftp-private-key-content": "Conteúdo da chave privada SSH",
	"sftp-private-key-tips":
		"Caminho do arquivo de chave privada PEM, ex.: ~/.ssh/id_rsa",
	"sftp-private-key-paste-tips": "Cole aqui o conteúdo da chave privada PEM",
	"sftp-key-passphrase": "Senha da chave",
	"sftp-key-passphrase-tips": "Deixe vazio se a chave não tiver senha",
	"m3u8-output-format": "M3U8 / HLS",
	"m3u8-output-format-label": "Formato de saída",
	"media-settings": "Sites de mídia (yt-dlp)",
	"media-settings-tips":
		"Configurações padrão para downloads de sites de mídia (YouTube, Vimeo, Bilibili e outros). Requer o yt-dlp no PATH; o ffmpeg é necessário para mesclar fluxos de vídeo e áudio separados.",
	"media-format": "Formato preferido",
	"media-format-placeholder":
		"ex.: bestvideo+bestaudio/best (string de formato do yt-dlp)",
	"media-format-tips":
		"Seletor de formato do yt-dlp. Deixe vazio para a melhor qualidade. Exemplos: bestvideo[height<=1080]+bestaudio/best, 137+140",
	port: "Portas de escuta",
	"bt-port": "Porta de escuta BT",
	"dht-port": "Porta de escuta DHT",
	"ed2k-port": "Porta de escuta ed2k",
	security: "Segurança",
	rpc: "RPC",
	"rpc-host": "Endereço do servidor",
	"rpc-host-local": "Somente este dispositivo",
	"rpc-host-all": "Todas as interfaces de rede",
	"rpc-host-custom": "Personalizado ({{host}})",
	"rpc-host-exposed-tips":
		"Outros dispositivos da sua rede podem acessar a porta RPC e, quando ativada, a porta do PeerBanHelper. Qualquer pessoa que conheça o segredo pode adicionar e controlar downloads.",
	"rpc-listen-port": "Porta de Escuta RPC",
	"rpc-secret": "Segredo de RPC",
	"rpc-secret-tips": "Veja o manual secreto de RPC",
	"rpc-allow-any-dir": "Permitir que clientes RPC usem qualquer caminho local",
	"rpc-allow-any-dir-tips":
		"Quando desativado, os downloads adicionados via RPC só podem ser salvos no diretório de download, em seus subdiretórios e nos diretórios de categoria e de roteamento, e os clientes RPC não podem fazer o aplicativo ler um arquivo de cookie, netrc ou chave SSH.",
	pbh: "PeerBanHelper",
	"pbh-enable": "Ativar API do PeerBanHelper",
	"pbh-enable-tips":
		"Expõe um listener JSON-RPC compatível com Aria2Next para o PeerBanHelper monitorar e banir pares BitTorrent",
	"pbh-listen-port": "Porta de escuta do PeerBanHelper",
	"pbh-rpc-secret": "Token RPC do PeerBanHelper",
	"generate-pbh-rpc-secret": "Gerar um novo token",
	"pbh-endpoint-hint":
		"No PeerBanHelper, adicione um downloader Aria2Next apontando para http://127.0.0.1:{{port}}/jsonrpc",
	"randomize-port": "Escolher uma porta aleatória",
	"generate-rpc-secret": "Gerar um novo segredo RPC",
	"copy-rpc-url": "Copiar URL do RPC",
	"engine-overrides": "Substituições do motor (JSON)",
	"engine-overrides-placeholder":
		'{\n  "piece-length": "1M",\n  "timeout": 60\n}',
	"engine-overrides-tips":
		"Avançado: defina chaves arbitrárias de opções do motor como um objeto JSON. As alterações entram em vigor após reiniciar.",
	"engine-overrides-invalid":
		"As substituições do motor devem ser um objeto JSON válido.",
	"engine-overrides-reserved-keys":
		"Chaves reservadas ignoradas nas substituições do motor: {{keys}}",
	"engine-overrides-too-large":
		"As substituições do motor são muito grandes: no máximo 64 chaves, 64 caracteres por chave e 2048 caracteres por valor.",
	developer: "Desenvolvedor",
	"user-agent": "User-Agent",
	"mock-user-agent": "Mock User-Agent",
	cookies: "Cookies",
	"cookies-tips":
		"Carrega cookies de um arquivo cookies.txt no formato Netscape/Mozilla (também aceita a saída do curl/wget).",
	"load-cookies-placeholder": "/path/to/cookies.txt",
	"saved-cookies": "Credenciais de domínio salvas",
	"saved-cookies-tips":
		"Cookies e User-Agent importados de um navegador ao contornar o Cloudflare ou outros downloads protegidos por autenticação. O Risuko os reutiliza em novos downloads do mesmo domínio até expirarem ou deixarem de funcionar.",
	"saved-cookies-empty": "Nenhuma credencial salva ainda.",
	"saved-cookies-host": "Domínio",
	"saved-cookies-browser": "Navegador",
	"saved-cookies-count": "Cookies",
	"saved-cookies-imported": "Importado em",
	"saved-cookies-actions": "Ações",
	"saved-cookies-clear": "Limpar tudo",
	"saved-cookies-confirm-clear":
		"Excluir todas as credenciais de domínio salvas? Downloads futuros nesses sites podem falhar até você importá-las novamente.",
	"saved-cookies-delete": "Excluir",
	netrc: "Credenciais .netrc",
	"netrc-tips":
		"Injeta a autenticação HTTP Basic de um arquivo .netrc quando a URL não tem credenciais incorporadas. Padrão: ~/.netrc (ou %USERPROFILE%/_netrc no Windows).",
	"netrc-path-placeholder": "Substituir o caminho do .netrc (opcional)",
	"no-netrc": "Desativar a consulta ao .netrc",
	"no-netrc-tips":
		"Ignora o .netrc por completo. Útil quando o arquivo contém credenciais que não são de download.",
	"app-log-path": "Diretório de logs",
	"log-dir-override-placeholder":
		"Diretório de logs personalizado (deixe em branco para o padrão)",
	"log-dir-override-tips":
		"Os logs estão sendo gravados em {{path}}. Definir um diretório personalizado (por exemplo, dentro de Downloads) os torna visíveis em qualquer gerenciador de arquivos. As alterações entram em vigor após a próxima reinicialização do app.",
	"download-session-path": "Diretório da sessão de Downloads",
	"session-reset": "Redefinir a sessão de downloads",
	"session-reset-confirm":
		"Tem certeza de que deseja redefinir a sessão de downloads?",
	"factory-reset": "Configurações de Fábrica",
	"factory-reset-confirm":
		"Você tem certeza de que deseja resetar às configurações de fábrica?",
	"download-protocol": "Protocolo",
	"protocols-default-client":
		"Definir como cliente padrão para os seguintes protocolos",
	"protocols-magnet": "Magnético [ magnet:// ]",
	"protocols-thunder": "Thunder [ thunder:// ]",
	"protocols-ed2k": "eDonkey [ ed2k:// ]",
	"protocols-adc": "Direct Connect [ adc:// adcs:// dchub:// nmdc:// ]",
	"protocols-gnutella": "Gnutella [ gnutella:// ]",
	"protocols-g2": "Gnutella2 [ g2:// ]",
	"gift-integration": "Integração com o daemon giFT",
	"gift-enabled": "Ativar delegação ao giFT",
	"gift-host": "Host do giFT",
	"gift-port": "Porta do giFT",
	"gift-not-detected": "Daemon giFT não detectado no host:porta configurado.",
	"browser-extensions": "Extensões",
	"baidu-exporter": "BaiduExporter",
	"browser-extensions-tips": "Fornecido pela comunidade, ",
	"baidu-exporter-help": "Clique aqui para ver as instruções de uso",
	"auto-update": "Atualização automática",
	"auto-check-update": "A verificação automática de atualizações",
	"last-check-update-time": "última verificação do tempo de atualização",
	"completion-script": "Script ao concluir",
	"completion-script-enabled": "Executar um script ao concluir o download",
	"completion-script-tips":
		"Executado em sandbox via tauri-plugin-shell. Executa uma vez ao fim de cada tarefa (sucesso ou erro).",
	"completion-script-command": "Comando",
	"completion-script-command-placeholder":
		"ex.: /usr/local/bin/notify.sh ou C:\\Tools\\notify.exe",
	"completion-script-args": "Argumentos",
	"completion-script-args-placeholder":
		"--path {path} --hash {hash} --status {status}",
	"completion-script-args-tips":
		"Marcadores: {path}, {hash}, {status}. Também expostos como variáveis de ambiente RISUKO_PATH, RISUKO_HASH, RISUKO_STATUS.",
	"completion-script-timeout": "Tempo limite (ms)",
	"completion-script-test": "Testar script",
	"completion-script-test-ok": "Teste do script concluído com sucesso",
	"completion-script-test-fail": "Falha no teste do script",
	"completion-script-empty": "Informe um comando primeiro",
	"completion-script-not-approved":
		"O script de conclusão não foi permitido, então nada foi salvo.",
	"not-saved": "Preferências não salvas",
	"not-saved-confirm":
		"As preferências modificadas serão perdidas. Tem certeza de que deseja sair?",
	"saved-credentials": "Credenciais salvas",
	"saved-credentials-description":
		"Gerencie as credenciais de autenticação salvas para downloads",
	"no-saved-credentials": "Nenhuma credencial salva",
	"credential-host": "Host",
	"credential-label": "Rótulo",
	"credential-protocol": "Protocolo",
	"credential-last-used": "Último uso",
	"credential-delete": "Excluir credencial",
	"credential-delete-confirm":
		"Tem certeza de que deseja excluir esta credencial salva?",
	"vault-enabled": "Segredos armazenados no chaveiro do sistema",
	"vault-disabled":
		"Chaveiro do sistema indisponível — segredos armazenados em texto simples",
	"credential-encrypted-badge": "Armazenado no chaveiro do sistema",
	"credential-vault-unavailable":
		"Chaveiro do sistema inacessível; as novas credenciais serão salvas em texto simples",
	"speed-limit-enabled": "Ativar limites de velocidade",
	"speed-limit-enabled-description":
		"Aplica os limites abaixo. Quando desativado, os downloads rodam na velocidade máxima; os valores são mantidos.",
	"appearance-theme": "Tema",
	"appearance-typography": "Tipografia",
	"appearance-window": "Janela",
	"task-list-style": "Lista de tarefas",
	"task-list-style-description":
		"Como os downloads são exibidos na lista de tarefas.",
	"task-list-style-compact": "Compacta",
	"task-list-style-card": "Cartões",
	"sidebar-collapsed": "Recolher barra lateral",
	"sidebar-collapsed-description": "Mostra apenas os ícones na barra lateral.",
};
