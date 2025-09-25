from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
import logging
from business.historico_sorteio import HistoricoSorteioBusiness

# Criar blueprint para rotas de histórico de sorteios
historico_bp = Blueprint('historico', __name__)

@historico_bp.route('/meu_historico')
def meu_historico():
    """Mostrar histórico de participações do cliente"""
    cpf = session.get('cpf')
    if not cpf:
        flash('Você precisa se cadastrar primeiro', 'error')
        return redirect(url_for('cliente.cadastro'))
    
    # Obter histórico do cliente
    historico = HistoricoSorteioBusiness.obter_historico_cliente(cpf)
    
    return render_template('historico_cliente.html', historico=historico)

@historico_bp.route('/todos_sorteios')
def todos_sorteios():
    """Listar todos os sorteios realizados"""
    try:
        sorteios = HistoricoSorteioBusiness.obter_todos_sorteios()
        return render_template('todos_sorteios.html', sorteios=sorteios)
        
    except Exception as e:
        logging.error(f"Erro ao obter sorteios: {e}")
        return render_template('todos_sorteios.html', sorteios=[])

@historico_bp.route('/participantes_sorteio/<versao_sorteio>')
def participantes_sorteio(versao_sorteio):
    """Mostrar participantes de um sorteio específico"""
    try:
        participantes = HistoricoSorteioBusiness.obter_participantes_sorteio(versao_sorteio)
        return render_template('participantes_sorteio.html', 
                             participantes=participantes, 
                             versao=versao_sorteio)
        
    except Exception as e:
        logging.error(f"Erro ao obter participantes: {e}")
        return render_template('participantes_sorteio.html', 
                             participantes=[], 
                             versao=versao_sorteio)

@historico_bp.route('/registrar_participacao', methods=['POST'])
def registrar_participacao():
    """Registrar participação de um cliente no sorteio"""
    try:
        id_cliente = int(request.form.get('id_cliente', 0))
        versao_sorteio = request.form.get('versao_sorteio', '').strip()
        saldo_anterior = float(request.form.get('saldo_anterior', 0))
        
        if not id_cliente or not versao_sorteio:
            return jsonify({'sucesso': False, 'mensagem': 'Dados obrigatórios'})
        
        # Registrar participação
        sucesso, mensagem = HistoricoSorteioBusiness.registrar_participacao(
            id_cliente, versao_sorteio, saldo_anterior
        )
        
        return jsonify({'sucesso': sucesso, 'mensagem': mensagem})
        
    except ValueError:
        return jsonify({'sucesso': False, 'mensagem': 'Dados inválidos'})
    except Exception as e:
        logging.error(f"Erro ao registrar participação: {e}")
        return jsonify({'sucesso': False, 'mensagem': 'Erro interno do servidor'})

@historico_bp.route('/registrar_resultado', methods=['POST'])
def registrar_resultado():
    """Registrar resultado de um sorteio"""
    try:
        # Verificar se é administrador (implementar verificação adequada)
        if not session.get('is_admin'):
            return jsonify({'sucesso': False, 'mensagem': 'Acesso negado'})
        
        versao_sorteio = request.form.get('versao_sorteio', '').strip()
        num_sorte_vencedor = request.form.get('num_sorte_vencedor', '').strip()
        premio_descricao = request.form.get('premio_descricao', '').strip()
        
        if not all([versao_sorteio, num_sorte_vencedor, premio_descricao]):
            return jsonify({'sucesso': False, 'mensagem': 'Todos os campos são obrigatórios'})
        
        # Registrar resultado
        sucesso, mensagem = HistoricoSorteioBusiness.registrar_resultado_sorteio(
            versao_sorteio, num_sorte_vencedor, premio_descricao
        )
        
        return jsonify({'sucesso': sucesso, 'mensagem': mensagem})
        
    except Exception as e:
        logging.error(f"Erro ao registrar resultado: {e}")
        return jsonify({'sucesso': False, 'mensagem': 'Erro interno do servidor'})

@historico_bp.route('/vencedores')
def vencedores():
    """Listar todos os vencedores"""
    try:
        vencedores = HistoricoSorteioBusiness.obter_vencedores()
        return render_template('vencedores.html', vencedores=vencedores)
        
    except Exception as e:
        logging.error(f"Erro ao obter vencedores: {e}")
        return render_template('vencedores.html', vencedores=[])

@historico_bp.route('/estatisticas_sorteio/<versao_sorteio>')
def estatisticas_sorteio(versao_sorteio):
    """Obter estatísticas de um sorteio específico"""
    try:
        estatisticas = HistoricoSorteioBusiness.obter_estatisticas_sorteio(versao_sorteio)
        return jsonify(estatisticas)
        
    except Exception as e:
        logging.error(f"Erro ao obter estatísticas: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@historico_bp.route('/verificar_participacao', methods=['POST'])
def verificar_participacao():
    """Verificar se um cliente participou de um sorteio"""
    try:
        cpf = request.form.get('cpf', '').strip()
        versao_sorteio = request.form.get('versao_sorteio', '').strip()
        
        if not cpf or not versao_sorteio:
            return jsonify({'sucesso': False, 'mensagem': 'CPF e versão do sorteio são obrigatórios'})
        
        participou = HistoricoSorteioBusiness.verificar_cliente_participou(cpf, versao_sorteio)
        
        return jsonify({
            'sucesso': True,
            'participou': participou,
            'mensagem': 'Verificação realizada com sucesso'
        })
        
    except Exception as e:
        logging.error(f"Erro ao verificar participação: {e}")
        return jsonify({'sucesso': False, 'mensagem': 'Erro interno do servidor'})