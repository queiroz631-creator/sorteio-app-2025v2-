// Clube QZ - Service Worker
// Gerenciamento de cache e funcionalidades offline para PWA

const CACHE_NAME = 'clube-qz-v1.0.0';
const STATIC_CACHE_NAME = 'clube-qz-static-v1.0.0';
const DYNAMIC_CACHE_NAME = 'clube-qz-dynamic-v1.0.0';

// Arquivos para cache estático (sempre em cache)
const STATIC_FILES = [
    '/',
    '/static/css/style.css',
    '/static/js/app.js',
    '/static/manifest.json',
    '/static/icons/icon-192.svg',
    '/static/icons/icon-512.svg',
    // CDN files que são críticos
    'https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css',
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css',
    'https://unpkg.com/htmx.org@1.9.6',
    'https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js'
];

// Arquivos para cache dinâmico (páginas visitadas)
const DYNAMIC_FILES = [
    '/cadastro',
    '/dashboard',
    '/cadastrar_nota',
    '/mostrar_notas',
    '/mostrar_numeros',
    '/informacoes'
];

// Instalação do Service Worker
self.addEventListener('install', event => {
    console.log('Service Worker: Installing...');
    
    event.waitUntil(
        Promise.all([
            // Cache estático
            caches.open(STATIC_CACHE_NAME).then(cache => {
                console.log('Service Worker: Caching static files');
                return cache.addAll(STATIC_FILES);
            }),
            // Cache dinâmico inicial
            caches.open(DYNAMIC_CACHE_NAME).then(cache => {
                console.log('Service Worker: Creating dynamic cache');
                return cache.addAll(['/']); // Apenas a página inicial
            })
        ]).then(() => {
            console.log('Service Worker: Installation complete');
            // Força a ativação imediata
            return self.skipWaiting();
        }).catch(error => {
            console.error('Service Worker: Installation failed', error);
        })
    );
});

// Ativação do Service Worker
self.addEventListener('activate', event => {
    console.log('Service Worker: Activating...');
    
    event.waitUntil(
        Promise.all([
            // Limpar caches antigos
            caches.keys().then(cacheNames => {
                return Promise.all(
                    cacheNames.map(cacheName => {
                        if (cacheName !== STATIC_CACHE_NAME && 
                            cacheName !== DYNAMIC_CACHE_NAME && 
                            cacheName !== CACHE_NAME) {
                            console.log('Service Worker: Deleting old cache:', cacheName);
                            return caches.delete(cacheName);
                        }
                    })
                );
            }),
            // Assumir controle de todas as páginas
            self.clients.claim()
        ]).then(() => {
            console.log('Service Worker: Activation complete');
        })
    );
});

// Interceptação de requisições (estratégia de cache)
self.addEventListener('fetch', event => {
    const { request } = event;
    const url = new URL(request.url);
    
    // Ignorar requisições não HTTP/HTTPS
    if (!request.url.startsWith('http')) {
        return;
    }
    
    // Estratégias diferentes baseadas no tipo de recurso
    if (isStaticAsset(request.url)) {
        // Cache First para assets estáticos
        event.respondWith(cacheFirst(request));
    } else if (isAPIRequest(request.url)) {
        // Network First para APIs
        event.respondWith(networkFirst(request));
    } else if (isPageRequest(request)) {
        // Stale While Revalidate para páginas
        event.respondWith(staleWhileRevalidate(request));
    } else {
        // Network First como padrão
        event.respondWith(networkFirst(request));
    }
});

// Estratégia Cache First
async function cacheFirst(request) {
    try {
        const cachedResponse = await caches.match(request);
        if (cachedResponse) {
            return cachedResponse;
        }
        
        const networkResponse = await fetch(request);
        if (networkResponse.ok) {
            const cache = await caches.open(STATIC_CACHE_NAME);
            cache.put(request, networkResponse.clone());
        }
        return networkResponse;
    } catch (error) {
        console.error('Cache First failed:', error);
        return await getOfflineFallback(request);
    }
}

// Estratégia Network First
async function networkFirst(request) {
    try {
        const networkResponse = await fetch(request);
        if (networkResponse.ok) {
            const cache = await caches.open(DYNAMIC_CACHE_NAME);
            cache.put(request, networkResponse.clone());
        }
        return networkResponse;
    } catch (error) {
        console.log('Network failed, trying cache:', request.url);
        const cachedResponse = await caches.match(request);
        if (cachedResponse) {
            return cachedResponse;
        }
        return await getOfflineFallback(request);
    }
}

// Estratégia Stale While Revalidate
async function staleWhileRevalidate(request) {
    const cache = await caches.open(DYNAMIC_CACHE_NAME);
    const cachedResponse = await cache.match(request);
    
    const networkResponsePromise = fetch(request).then(response => {
        if (response.ok) {
            cache.put(request, response.clone());
        }
        return response;
    }).catch(error => {
        console.log('Network update failed:', error);
    });
    
    // Retorna cache imediatamente se disponível, senão espera pela rede
    return cachedResponse || await networkResponsePromise || await getOfflineFallback(request);
}

// Fallback offline
async function getOfflineFallback(request) {
    if (isPageRequest(request)) {
        // Retorna página offline personalizada ou página inicial cacheada
        const cachedPage = await caches.match('/');
        if (cachedPage) {
            return cachedPage;
        }
        
        // Página offline mínima
        return new Response(`
            <!DOCTYPE html>
            <html lang="pt-BR">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Offline - Clube QZ</title>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f8f9fa; }
                    .offline-container { max-width: 500px; margin: 0 auto; }
                    .icon { font-size: 4rem; color: #dc2626; margin-bottom: 1rem; }
                    h1 { color: #dc2626; }
                    .btn { background: #dc2626; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 20px; }
                </style>
            </head>
            <body>
                <div class="offline-container">
                    <div class="icon">📱</div>
                    <h1>Clube QZ</h1>
                    <h2>Você está offline</h2>
                    <p>Não foi possível carregar esta página. Verifique sua conexão com a internet e tente novamente.</p>
                    <a href="/" class="btn" onclick="window.location.reload()">Tentar Novamente</a>
                </div>
            </body>
            </html>
        `, {
            headers: { 'Content-Type': 'text/html' }
        });
    }
    
    // Fallback genérico para outros tipos de requisição
    return new Response('Offline', { status: 503, statusText: 'Service Unavailable' });
}

// Utilitários para identificar tipos de requisição
function isStaticAsset(url) {
    const staticExtensions = ['.css', '.js', '.svg', '.png', '.jpg', '.jpeg', '.gif', '.ico', '.woff', '.woff2', '.ttf'];
    return staticExtensions.some(ext => url.includes(ext)) || url.includes('cdn.') || url.includes('unpkg.com');
}

function isAPIRequest(url) {
    return url.includes('/api/') || url.includes('/processar_') || url.includes('/atualizar_');
}

function isPageRequest(request) {
    return request.method === 'GET' && request.headers.get('accept')?.includes('text/html');
}

// Mensagens do Service Worker para o cliente
self.addEventListener('message', event => {
    if (event.data && event.data.type === 'SKIP_WAITING') {
        self.skipWaiting();
    }
    
    if (event.data && event.data.type === 'GET_VERSION') {
        event.ports[0].postMessage({ version: CACHE_NAME });
    }
    
    if (event.data && event.data.type === 'CLEAR_CACHE') {
        clearAllCaches().then(() => {
            event.ports[0].postMessage({ success: true });
        });
    }
});

// Limpar todos os caches
async function clearAllCaches() {
    const cacheNames = await caches.keys();
    await Promise.all(
        cacheNames.map(cacheName => caches.delete(cacheName))
    );
}

// Sincronização em background (quando voltar online)
self.addEventListener('sync', event => {
    if (event.tag === 'background-sync') {
        event.waitUntil(doBackgroundSync());
    }
});

async function doBackgroundSync() {
    try {
        // Aqui você pode implementar sincronização de dados pendentes
        console.log('Background sync executada');
        
        // Enviar notificação para o cliente sobre sincronização
        const clients = await self.clients.matchAll();
        clients.forEach(client => {
            client.postMessage({
                type: 'BACKGROUND_SYNC_COMPLETE',
                data: { timestamp: Date.now() }
            });
        });
    } catch (error) {
        console.error('Background sync failed:', error);
    }
}

// Push notifications (para futuras implementações)
self.addEventListener('push', event => {
    const options = {
        body: event.data?.text() || 'Nova atualização do Clube QZ!',
        icon: '/static/icons/icon-192.svg',
        badge: '/static/icons/icon-192.svg',
        vibrate: [100, 50, 100],
        data: {
            dateOfArrival: Date.now(),
            primaryKey: '1'
        },
        actions: [
            {
                action: 'explore',
                title: 'Ver Detalhes',
                icon: '/static/icons/icon-192.svg'
            },
            {
                action: 'close',
                title: 'Fechar'
            }
        ]
    };
    
    event.waitUntil(
        self.registration.showNotification('Clube QZ', options)
    );
});

// Clique em notificações
self.addEventListener('notificationclick', event => {
    event.notification.close();
    
    if (event.action === 'explore') {
        event.waitUntil(
            clients.openWindow('/')
        );
    }
});

console.log('Service Worker: Script loaded successfully');
