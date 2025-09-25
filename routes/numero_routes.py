from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
import logging
from business.numero_sorte import NumeroSorteBusiness

# Criar blueprint para rotas de números da sorte
numero_bp = Blueprint('numero', __name__)

@numero_bp.route('/mostrar_numeros')
def mostrar_numeros():
    """Mostrar todos os números da sorte do cliente"""
    cpf = session.get('cpf')
    if not cpf:
        flash('Você precisa se cadastrar primeiro', 'error')
        return redirect(url_for('cliente.cadastro'))
    
    # Obter números da sorte do cliente
    numeros = NumeroSorteBusiness.obter_numeros_cliente(cpf)
    
    return render_template('mostrar_numeros.html', numeros=numeros)

@numero_bp.route('/gerar_numeros', methods=['POST'])
def gerar_numeros():
    """Gerar números da sorte manualmente (uso administrativo)"""
    try:
        cpf = request.form.get('cpf', '').strip()
        quantidade = int(request.form.get('quantidade', 0))
        
        if not cpf or quantidade <= 0:
            return jsonify({'sucesso': False, 'mensagem': 'Dados inválidos'})
        
        # Gerar números da sorte
        sucesso, mensagem = NumeroSorteBusiness.gerar_numeros_sorte(cpf, quantidade)
        
        return jsonify({'sucesso': sucesso, 'mensagem': mensagem})
        
    except ValueError:
        return jsonify({'sucesso': False, 'mensagem': 'Quantidade inválida'})
    except Exception as e:
        logging.error(f"Erro ao gerar números: {e}")
        return jsonify({'sucesso': False, 'mensagem': 'Erro interno do servidor'})

@numero_bp.route('/verificar_numero_sorteado', methods=['POST'])
def verificar_numero_sorteado():
    """Verificar se um número foi sorteado"""
    try:
        num_sorte = request.form.get('num_sorte', '').strip()
        
        if not num_sorte:
            return jsonify({'sucesso': False, 'mensagem': 'Número inválido'})
        
        # Verificar se o número foi sorteado
        foi_sorteado = NumeroSorteBusiness.verificar_numero_sorteado(num_sorte)
        
        return jsonify({
            'sucesso': True, 
            'foi_sorteado': foi_sorteado,
            'mensagem': 'Número verificado com sucesso'
        })
        
    except Exception as e:
        logging.error(f"Erro ao verificar número sorteado: {e}")
        return jsonify({'sucesso': False, 'mensagem': 'Erro interno do servidor'})

@numero_bp.route('/estatisticas_numeros')
def estatisticas_numeros():
    """Obter estatísticas dos números da sorte"""
    try:
        estatisticas = NumeroSorteBusiness.obter_estatisticas_numeros()
        return jsonify(estatisticas)
        
    except Exception as e:
        logging.error(f"Erro ao obter estatísticas: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@numero_bp.route('/listar_numeros_periodo')
def listar_numeros_periodo():
    """Listar números gerados em um período"""
    try:
        data_inicio = request.args.get('data_inicio')
        data_fim = request.args.get('data_fim')
        
        if not data_inicio or not data_fim:
            return jsonify({'erro': 'Datas obrigatórias'}), 400
        
        numeros = NumeroSorteBusiness.listar_numeros_por_data(data_inicio, data_fim)
        
        return jsonify({
            'numeros': [
                {
                    'num_sorte': numero[0],
                    'cpf': numero[1][:3] + '.***.***-**',  # Mascarar CPF
                    'data_cadastro': numero[2]
                } for numero in numeros
            ]
        })
        
    except Exception as e:
        logging.error(f"Erro ao listar números por período: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@numero_bp.route('/validar_formato_numero', methods=['POST'])
def validar_formato_numero():
    """Validar formato de um número da sorte"""
    try:
        num_sorte = request.form.get('num_sorte', '').strip()
        
        is_valid = NumeroSorteBusiness.validar_numero_formato(num_sorte)
        
        return jsonify({
            'sucesso': True,
            'valido': is_valid,
            'mensagem': 'Formato válido' if is_valid else 'Formato inválido'
        })
        
    except Exception as e:
        logging.error(f"Erro ao validar formato: {e}")
        return jsonify({'sucesso': False, 'mensagem': 'Erro interno do servidor'})