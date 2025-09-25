import os
import logging
from flask import Flask

# Configurar logging para debug
logging.basicConfig(level=logging.DEBUG)

# Criar a aplicação Flask
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "sorteio_qz_secret_key_2024")

# Importar e registrar as rotas
from routes.main_routes import main_bp
from routes.cliente_routes import cliente_bp
from routes.nota_routes import nota_bp
from routes.dashboard_routes import dashboard_bp
from routes.numero_routes import numero_bp
from routes.historico_routes import historico_bp
from routes.informacoes_routes import informacoes_bp

app.register_blueprint(main_bp)
app.register_blueprint(cliente_bp)
app.register_blueprint(nota_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(numero_bp)
app.register_blueprint(historico_bp)
app.register_blueprint(informacoes_bp)

# Inicializar thread de verificação automatizada
from business.thread_verificacao import thread_verificacao
thread_verificacao.iniciar_thread()

# Configuração PWA
@app.after_request
def after_request(response):
    """Adicionar headers para PWA"""
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response
