from flask import Blueprint, render_template

# Criar blueprint para as rotas principais
main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    """Tela inicial do sorteio"""
    return render_template('index.html')
