from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
import logging
from business.cadastrar_nota import CadastrarNotaBusiness
from business.numero_sorte import NumeroSorteBusiness

# Criar blueprint para rotas de notas
nota_bp = Blueprint('nota', __name__)


@nota_bp.route('/cadastrar_nota')
def cadastrar_nota():
    """Tela para cadastrar nota fiscal"""
    cpf = session.get('cpf')
    if not cpf:
        flash('Você precisa se cadastrar primeiro', 'error')
        return redirect(url_for('cliente.cadastro'))

    # Obter dados salvos em caso de erro anterior
    dados = {
        'valor': session.pop('nota_valor', ''),
        'num_nota': session.pop('nota_num_nota', '')
    }
    
    return render_template('cadastrar_nota.html', dados=dados)


@nota_bp.route('/processar_nota', methods=['POST'])
def processar_nota():
    """Processar cadastro de nota fiscal"""
    try:
        cpf = session.get('cpf')
        if not cpf:
            flash('Sessão expirada. Faça o cadastro novamente', 'error')
            return redirect(url_for('cliente.cadastro'))

        # Obter valor numérico formatado pelo JavaScript ou converter valor manual
        valor_str = request.form.get('valor_numerico') or request.form.get('valor', '0')
        
        # Se vier formatado em português (com vírgula), converter
        if ',' in valor_str:
            valor_str = valor_str.replace('.', '').replace(',', '.')
        
        valor = float(valor_str)
        num_nota = request.form.get('num_nota', '').strip()

        # Cadastrar nota
        sucesso, mensagem = CadastrarNotaBusiness.cadastrar_nota(
            valor, num_nota, cpf)

        if sucesso:
            flash(mensagem, 'success')
            return redirect(url_for('dashboard.dashboard'))
        else:
            # Salvar dados na sessão para manter no formulário
            session['nota_valor'] = valor
            session['nota_num_nota'] = num_nota
            flash(mensagem, 'error')
            return redirect(url_for('nota.cadastrar_nota'))

    except ValueError:
        flash('Valor inválido', 'error')
        return redirect(url_for('nota.cadastrar_nota'))
    except Exception as e:
        logging.error(f"Erro ao processar nota: {e}")
        flash('Erro interno do servidor', 'error')
        return redirect(url_for('nota.cadastrar_nota'))


@nota_bp.route('/mostrar_notas')
def mostrar_notas():
    """Mostrar todas as notas do cliente"""
    cpf = session.get('cpf')
    if not cpf:
        flash('Você precisa se cadastrar primeiro', 'error')
        return redirect(url_for('cliente.cadastro'))

    # Obter notas do cliente usando consulta SQL direta
    try:
        # Limpar CPF - apenas números
        cpf = ''.join(filter(str.isdigit, cpf))

        from database import db
        db.connect()

        # Primeiro, buscar o num_sorteio do cliente
        sql_cliente = "SELECT num_sorteio FROM tab_cliente WHERE cpf = ?"
        resultado_cliente = db.execute_select(sql_cliente, (cpf,))
        
        if not resultado_cliente:
            db.close()
            return render_template('mostrar_notas.html', notas=[])
            
        num_sorteio_cliente = resultado_cliente[0][0]

        # Buscar notas do cliente filtrando pelo seu num_sorteio
        sql = "SELECT valor, num_nota, status, dt_registro FROM tab_nota WHERE cpf = ? AND num_sorteio = ? ORDER BY dt_registro DESC"
        notas = db.execute_select(sql, (cpf, num_sorteio_cliente))

        db.close()

        return render_template('mostrar_notas.html', notas=notas)

    except Exception as e:
        logging.error(f"Erro ao obter notas: {e}")
        return render_template('mostrar_notas.html', notas=[])


@nota_bp.route('/editar_nota', methods=['POST'])
def editar_nota():
    """Editar uma nota fiscal"""
    try:
        cpf = session.get('cpf')
        if not cpf:
            return jsonify({'sucesso': False, 'mensagem': 'Sessão expirada'})

        num_nota_original = request.form.get('num_nota_original', '').strip()
        novo_valor = float(request.form.get('novo_valor', 0))
        novo_num_nota = request.form.get('novo_num_nota', '').strip()

        # Editar nota
        sucesso, mensagem = CadastrarNotaBusiness.editar_nota(
            num_nota_original, novo_valor, novo_num_nota)

        return jsonify({'sucesso': sucesso, 'mensagem': mensagem})

    except ValueError:
        return jsonify({'sucesso': False, 'mensagem': 'Valor inválido'})
    except Exception as e:
        logging.error(f"Erro ao editar nota: {e}")
        return jsonify({
            'sucesso': False,
            'mensagem': 'Erro interno do servidor'
        })


@nota_bp.route('/excluir_nota', methods=['POST'])
def excluir_nota():
    """Excluir uma nota fiscal"""
    try:
        cpf = session.get('cpf')
        if not cpf:
            return jsonify({'sucesso': False, 'mensagem': 'Sessão expirada'})

        num_nota = request.form.get('num_nota', '').strip()

        # Excluir nota
        sucesso, mensagem = CadastrarNotaBusiness.excluir_nota(num_nota)

        return jsonify({'sucesso': sucesso, 'mensagem': mensagem})

    except Exception as e:
        logging.error(f"Erro ao excluir nota: {e}")
        return jsonify({
            'sucesso': False,
            'mensagem': 'Erro interno do servidor'
        })


@nota_bp.route('/validar_nota', methods=['POST'])
def validar_nota():
    """Validar uma nota fiscal (rota para administradores)"""
    try:
        # Verificar se é administrador (implementar verificação adequada)
        if not session.get('is_admin'):
            return jsonify({'sucesso': False, 'mensagem': 'Acesso negado'})

        num_nota = request.form.get('num_nota', '').strip()

        # Validar nota
        sucesso, mensagem = CadastrarNotaBusiness.validar_nota(num_nota)

        return jsonify({'sucesso': sucesso, 'mensagem': mensagem})

    except Exception as e:
        logging.error(f"Erro ao validar nota: {e}")
        return jsonify({
            'sucesso': False,
            'mensagem': 'Erro interno do servidor'
        })
