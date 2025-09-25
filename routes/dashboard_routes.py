from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
import logging
from business.dashboard import DashboardBusiness

# Criar blueprint para rotas de dashboard
dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/dashboard')
def dashboard():
    """Dashboard do cliente"""
    cpf = session.get('cpf')
    if not cpf:
        flash('Você precisa se cadastrar primeiro', 'error')
        return redirect(url_for('cliente.cadastro'))
    
    # Obter resumo do cliente
    resumo = DashboardBusiness.obter_resumo_cliente(cpf)
    
    return render_template('dashboard.html', resumo=resumo)

@dashboard_bp.route('/atualizar_dashboard')
def atualizar_dashboard():
    """Atualizar dados do dashboard via HTMX"""
    cpf = session.get('cpf')
    if not cpf:
        return jsonify({'erro': 'Sessão expirada'}), 401
    
    resumo = DashboardBusiness.obter_resumo_cliente(cpf)
    return render_template('dashboard.html', resumo=resumo)

@dashboard_bp.route('/estatisticas_gerais')
def estatisticas_gerais():
    """Obter estatísticas gerais do sistema"""
    try:
        estatisticas = DashboardBusiness.obter_estatisticas_gerais()
        return jsonify(estatisticas)
        
    except Exception as e:
        logging.error(f"Erro ao obter estatísticas gerais: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@dashboard_bp.route('/atividade_recente')
def atividade_recente():
    """Obter atividade recente do cliente"""
    try:
        cpf = session.get('cpf')
        if not cpf:
            return jsonify({'erro': 'Sessão expirada'}), 401
        
        limite = request.args.get('limite', 5, type=int)
        atividades = DashboardBusiness.obter_atividade_recente(cpf, limite)
        
        return jsonify({
            'atividades': [
                {
                    'tipo': atividade[0],
                    'referencia': atividade[1],
                    'valor': float(atividade[2]),
                    'data': atividade[3]
                } for atividade in atividades
            ]
        })
        
    except Exception as e:
        logging.error(f"Erro ao obter atividade recente: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@dashboard_bp.route('/ranking_participantes')
def ranking_participantes():
    """Obter ranking dos participantes"""
    try:
        limite = request.args.get('limite', 10, type=int)
        ranking = DashboardBusiness.obter_ranking_participantes(limite)
        
        return jsonify({
            'ranking': [
                {
                    'nome': participante[0],
                    'cpf': participante[1][:3] + '.***.***-**',  # Mascarar CPF
                    'total_numeros': participante[2]
                } for participante in ranking
            ]
        })
        
    except Exception as e:
        logging.error(f"Erro ao obter ranking: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500