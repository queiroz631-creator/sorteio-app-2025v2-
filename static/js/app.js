// Clube QZ - JavaScript Principal
// Sistema de controle de tema, PWA e funcionalidades gerais

// Evitar redeclaração se já existe
if (typeof window.ClubeQZ !== 'undefined') {
    console.log('ClubeQZ já carregado, evitando redeclaração');
} else {

class ClubeQZ {
    constructor() {
        this.theme = localStorage.getItem('theme') || 'light';
        this.init();
    }

    init() {
        this.setupTheme();
        this.setupPWA();
        this.setupFormValidation();
        this.setupNotifications();
        this.setupOfflineDetection();
        console.log('Clube QZ inicializado com sucesso!');
    }

    // Controle de Tema (Dark/Light Mode)
    setupTheme() {
        // Aplicar tema salvo
        document.documentElement.setAttribute('data-theme', this.theme);
        this.updateThemeIcon();

        // Event listener para toggle de tema
        window.toggleTheme = () => {
            this.theme = this.theme === 'light' ? 'dark' : 'light';
            document.documentElement.setAttribute('data-theme', this.theme);
            localStorage.setItem('theme', this.theme);
            this.updateThemeIcon();
            this.showNotification('Tema alterado com sucesso!', 'info');
        };
    }

    updateThemeIcon() {
        const icon = document.getElementById('theme-icon');
        if (icon) {
            icon.className = this.theme === 'light' ? 'fas fa-moon' : 'fas fa-sun';
        }
    }

    // PWA Setup
    setupPWA() {
        // Install prompt
        let deferredPrompt;
        
        window.addEventListener('beforeinstallprompt', (e) => {
            e.preventDefault();
            deferredPrompt = e;
            this.showInstallButton();
        });

        // Install button handler
        window.installPWA = async () => {
            if (deferredPrompt) {
                deferredPrompt.prompt();
                const { outcome } = await deferredPrompt.userChoice;
                
                if (outcome === 'accepted') {
                    this.showNotification('App instalado com sucesso!', 'success');
                }
                
                deferredPrompt = null;
                this.hideInstallButton();
            }
        };

        // Update available handler
        if ('serviceWorker' in navigator) {
            navigator.serviceWorker.addEventListener('controllerchange', () => {
                this.showUpdateNotification();
            });
        }
    }

    showInstallButton() {
        if (!document.getElementById('install-button')) {
            const installButton = document.createElement('button');
            installButton.id = 'install-button';
            installButton.className = 'btn btn-success pwa-install';
            installButton.innerHTML = '<i class="fas fa-download me-2"></i>Instalar App';
            installButton.onclick = window.installPWA;
            document.body.appendChild(installButton);
        }
    }

    hideInstallButton() {
        const button = document.getElementById('install-button');
        if (button) {
            button.remove();
        }
    }

    showUpdateNotification() {
        const notification = document.createElement('div');
        notification.className = 'alert alert-info offline-indicator';
        notification.innerHTML = `
            <i class="fas fa-sync-alt me-2"></i>
            Nova versão disponível! 
            <button class="btn btn-sm btn-info ms-2" onclick="window.location.reload()">
                Atualizar
            </button>
        `;
        document.body.appendChild(notification);
        
        setTimeout(() => notification.remove(), 10000);
    }

    // Validação de Formulários
    setupFormValidation() {
        // Máscara para telefone
        window.formatarTelefone = (telefone) => {
            telefone = telefone.replace(/\D/g, '');
            
            if (telefone.length === 11) {
                return telefone.replace(/(\d{2})(\d{5})(\d{4})/, '($1) $2-$3');
            } else if (telefone.length === 10) {
                return telefone.replace(/(\d{2})(\d{4})(\d{4})/, '($1) $2-$3');
            }
            
            return telefone;
        };

        // Validação em tempo real
        document.addEventListener('DOMContentLoaded', () => {
            const telefoneInputs = document.querySelectorAll('input[name="telefone"]');
            telefoneInputs.forEach(input => {
                input.addEventListener('input', (e) => {
                    e.target.value = window.formatarTelefone(e.target.value);
                });
            });

        });
    }

    // Sistema de Notificações
    setupNotifications() {
        // Container para notificações
        if (!document.getElementById('notification-container')) {
            const container = document.createElement('div');
            container.id = 'notification-container';
            container.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                z-index: 9999;
                max-width: 350px;
            `;
            document.body.appendChild(container);
        }
    }

    showNotification(message, type = 'info', duration = 5000) {
        const container = document.getElementById('notification-container');
        const notification = document.createElement('div');
        
        const typeClasses = {
            success: 'alert-success',
            error: 'alert-danger',
            warning: 'alert-warning',
            info: 'alert-info'
        };

        const icons = {
            success: 'fas fa-check-circle',
            error: 'fas fa-exclamation-triangle',
            warning: 'fas fa-exclamation-circle',
            info: 'fas fa-info-circle'
        };

        notification.className = `alert ${typeClasses[type]} alert-dismissible fade show`;
        notification.innerHTML = `
            <i class="${icons[type]} me-2"></i>
            ${message}
            <button type="button" class="btn-close" onclick="this.parentElement.remove()"></button>
        `;

        container.appendChild(notification);

        // Auto remove
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, duration);
    }

    // Detecção de Status Offline/Online
    setupOfflineDetection() {
        const showOfflineStatus = () => {
            if (!document.getElementById('offline-indicator')) {
                const indicator = document.createElement('div');
                indicator.id = 'offline-indicator';
                indicator.className = 'alert alert-warning offline-indicator';
                indicator.innerHTML = `
                    <i class="fas fa-wifi me-2"></i>
                    Você está offline. Algumas funcionalidades podem não estar disponíveis.
                `;
                document.body.appendChild(indicator);
            }
        };

        const hideOfflineStatus = () => {
            const indicator = document.getElementById('offline-indicator');
            if (indicator) {
                indicator.remove();
            }
        };

        window.addEventListener('online', () => {
            hideOfflineStatus();
            this.showNotification('Conexão restaurada!', 'success');
        });

        window.addEventListener('offline', () => {
            showOfflineStatus();
            this.showNotification('Você está offline', 'warning');
        });

        // Verificar status inicial
        if (!navigator.onLine) {
            showOfflineStatus();
        }
    }

    // Utilitários Gerais
    static formatCurrency(value) {
        return new Intl.NumberFormat('pt-BR', {
            style: 'currency',
            currency: 'BRL'
        }).format(value);
    }

    static formatDate(date) {
        return new Intl.DateTimeFormat('pt-BR', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        }).format(new Date(date));
    }

    static debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
}

// Inicializar aplicação
window.addEventListener('DOMContentLoaded', () => {
    if (!window.clubeQZ) {
        window.clubeQZ = new ClubeQZ();
    }
});

// Função global para calcular números da sorte
window.calcularNumerosRapido = (valor) => {
    return Math.floor(valor / 20);
};

// HTMX Configuration
document.addEventListener('DOMContentLoaded', function() {
    // Configurar HTMX para mostrar indicadores de loading
    document.body.addEventListener('htmx:beforeRequest', function(evt) {
        const element = evt.detail.elt;
        if (element.classList.contains('btn')) {
            element.classList.add('loading');
        }
    });

    document.body.addEventListener('htmx:afterRequest', function(evt) {
        const element = evt.detail.elt;
        if (element.classList.contains('btn')) {
            element.classList.remove('loading');
        }
    });

    // Configurar HTMX para mostrar notificações em caso de erro
    document.body.addEventListener('htmx:responseError', function(evt) {
        if (window.clubeQZ) {
            window.clubeQZ.showNotification(
                'Erro na comunicação com o servidor. Tente novamente.',
                'error'
            );
        }
    });

    document.body.addEventListener('htmx:sendError', function(evt) {
        if (window.clubeQZ) {
            window.clubeQZ.showNotification(
                'Erro de conexão. Verifique sua internet.',
                'error'
            );
        }
    });
});

// Exportar para uso global
window.ClubeQZ = ClubeQZ;

// Inicializar aplicação
window.clubeQZ = new ClubeQZ();

} // Fim da verificação de redeclaração
