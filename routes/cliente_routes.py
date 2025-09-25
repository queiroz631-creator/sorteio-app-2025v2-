from flask import Blueprint, render_template, request, redirect, url_for, session, flash
import logging
from business.cadastrar_cliente import ClienteBusiness

# Criar blueprint para rotas de cliente
cliente_bp = Blueprint('cliente', __name__)

@cliente_bp.route('/verificar_cpf')
def verificar_cpf():
    """Tela para verificar CPF antes do cadastro/login"""
    return render_template('verificar_cpf.html')

@cliente_bp.route('/processar_verificacao_cpf', methods=['POST'])
def processar_verificacao_cpf():
    """Processar verificação de CPF"""
    cpf = request.form.get('cpf', '').strip()
    
    if not cpf:
        flash('CPF é obrigatório', 'error')
        return redirect(url_for('cliente.verificar_cpf'))
    
    # Buscar cliente por CPF
    cliente = ClienteBusiness.buscar_cliente_por_cpf(cpf)
    
    if cliente:
        # Cliente existe - pedir confirmação de telefone
        session['login_cpf'] = cpf
        session['cliente_telefone'] = cliente['telefone']  # telefone do banco
        return redirect(url_for('cliente.confirmar_telefone'))
    else:
        # Cliente não existe - ir para cadastro com CPF preenchido
        session['cadastro_cpf'] = cpf
        return redirect(url_for('cliente.cadastro'))

@cliente_bp.route('/confirmar_telefone')
def confirmar_telefone():
    """Tela para confirmar telefone do cliente existente"""
    if 'login_cpf' not in session:
        flash('Sessão inválida', 'error')
        return redirect(url_for('cliente.verificar_cpf'))
    
    # Mascarar telefone para exibição
    telefone_completo = session.get('cliente_telefone', '')
    if len(telefone_completo) >= 8:
        telefone_mascarado = telefone_completo[:2] + '****' + telefone_completo[-4:]
    else:
        telefone_mascarado = '****' + telefone_completo[-4:] if len(telefone_completo) >= 4 else '****'
    
    return render_template('confirmar_telefone.html', telefone_mascarado=telefone_mascarado)

@cliente_bp.route('/processar_confirmacao_telefone', methods=['POST'])
def processar_confirmacao_telefone():
    """Processar confirmação de telefone"""
    if 'login_cpf' not in session:
        flash('Sessão inválida', 'error')
        return redirect(url_for('cliente.verificar_cpf'))
    
    telefone_informado = request.form.get('telefone', '').strip()
    cpf = session.get('login_cpf')
    telefone_correto = session.get('cliente_telefone', '')
    
    # Limpar ambos os telefones para comparação
    telefone_informado_limpo = ''.join(filter(str.isdigit, telefone_informado))
    telefone_correto_limpo = ''.join(filter(str.isdigit, telefone_correto))
    
    if telefone_informado_limpo == telefone_correto_limpo:
        # Telefone confirmado - verificar versão do sorteio
        from business.cadastrar_cliente import ClienteBusiness
        
        # Buscar dados do cliente para obter num_sorteio
        cliente = ClienteBusiness.buscar_cliente_por_cpf(cpf)
        if cliente:
            num_sorteio_cliente = cliente.get('num_sorteio')
            
            # Buscar o número do sorteio atual (máximo)
            from database import db
            conectado = db.connect()
            
            if conectado:
                sql_sorteio_atual = "SELECT MAX(num_sorteio) FROM Sorteio_qz.dbo.tab_sorteio"
                resultado_sorteio = db.execute_select(sql_sorteio_atual)
                num_sorteio_atual = resultado_sorteio[0][0] if resultado_sorteio and resultado_sorteio[0][0] else 1
                db.close()
            else:
                num_sorteio_atual = 1  # Fallback
            
            if num_sorteio_cliente == num_sorteio_atual:
                # Mesmo sorteio - login direto
                session['cpf'] = cpf
                session.pop('login_cpf', None)
                session.pop('cliente_telefone', None)
                flash('Login realizado com sucesso!', 'success')
                
                # Redirecionar para página que salva dados no localStorage
                return redirect(url_for('cliente.salvar_login_cache', cpf=cpf, telefone=telefone_correto))
            else:
                # Sorteio diferente - mostrar confirmação
                session['cpf_pendente'] = cpf
                session.pop('login_cpf', None)
                session.pop('cliente_telefone', None)
                return redirect(url_for('cliente.confirmar_sorteio'))
        else:
            # Erro ao buscar cliente
            flash('Erro interno. Tente novamente.', 'error')
            return redirect(url_for('cliente.confirmar_telefone'))
    else:
        flash('Telefone incorreto. Tente novamente.', 'error')
        return redirect(url_for('cliente.confirmar_telefone'))

@cliente_bp.route('/cadastro')
def cadastro():
    """Tela de cadastro do cliente"""
    # Obter dados da sessão em caso de erro anterior
    dados = {
        'nome': session.pop('cadastro_nome', ''),
        'cpf': session.pop('cadastro_cpf', ''),
        'telefone': session.pop('cadastro_telefone', ''),
        'data_nascimento': session.pop('cadastro_data_nascimento', '')
    }
    return render_template('cadastro.html', dados=dados)

@cliente_bp.route('/processar_cadastro', methods=['POST'])
def processar_cadastro():
    """Processar cadastro do cliente"""
    nome = request.form.get('nome', '').strip()
    cpf = request.form.get('cpf', '').strip()
    telefone = request.form.get('telefone', '').strip()
    data_nascimento = request.form.get('data_nascimento', '').strip()
    
    try:
        # Validar e cadastrar cliente
        sucesso, mensagem = ClienteBusiness.cadastrar_cliente(nome, cpf, telefone, data_nascimento)
        
        if sucesso:
            # Salvar CPF na sessão
            session['cpf'] = cpf
            # Limpar dados temporários
            session.pop('cadastro_nome', None)
            session.pop('cadastro_cpf', None)
            session.pop('cadastro_telefone', None)
            session.pop('cadastro_data_nascimento', None)
            flash(mensagem, 'success')
            
            # Redirecionar para salvar no cache após cadastro
            return redirect(url_for('cliente.salvar_login_cache', cpf=cpf, telefone=telefone))
        else:
            # Manter dados preenchidos em caso de erro
            session['cadastro_nome'] = nome
            session['cadastro_cpf'] = cpf
            session['cadastro_telefone'] = telefone
            session['cadastro_data_nascimento'] = data_nascimento
            flash(mensagem, 'error')
            return redirect(url_for('cliente.cadastro'))
            
    except Exception as e:
        logging.error(f"Erro ao processar cadastro: {e}")
        # Manter dados preenchidos em caso de erro
        session['cadastro_nome'] = nome
        session['cadastro_cpf'] = cpf
        session['cadastro_telefone'] = telefone
        session['cadastro_data_nascimento'] = data_nascimento
        flash('Erro interno do servidor', 'error')
        return redirect(url_for('cliente.cadastro'))

@cliente_bp.route('/perfil')
def perfil():
    """Tela de perfil do cliente"""
    cpf = session.get('cpf')
    if not cpf:
        flash('Você precisa se cadastrar primeiro', 'error')
        return redirect(url_for('cliente.cadastro'))
    
    # Obter dados do cliente
    cliente = ClienteBusiness.buscar_cliente_por_cpf(cpf)
    
    if not cliente:
        flash('Cliente não encontrado', 'error')
        return redirect(url_for('cliente.cadastro'))
    
    return render_template('perfil.html', cliente=cliente)

@cliente_bp.route('/atualizar_perfil', methods=['POST'])
def atualizar_perfil():
    """Atualizar dados do perfil"""
    try:
        cpf = session.get('cpf')
        if not cpf:
            flash('Sessão expirada. Faça o cadastro novamente', 'error')
            return redirect(url_for('cliente.cadastro'))
        
        nome = request.form.get('nome', '').strip()
        telefone = request.form.get('telefone', '').strip()
        
        # Atualizar dados do cliente
        sucesso, mensagem = ClienteBusiness.atualizar_cliente(nome, cpf, telefone)
        
        if sucesso:
            flash(mensagem, 'success')
        else:
            flash(mensagem, 'error')
            
        return redirect(url_for('cliente.perfil'))
        
    except Exception as e:
        logging.error(f"Erro ao atualizar perfil: {e}")
        flash('Erro interno do servidor', 'error')
        return redirect(url_for('cliente.perfil'))

@cliente_bp.route('/confirmar_sorteio')
def confirmar_sorteio():
    """Tela para confirmar participação em novo sorteio"""
    if 'cpf_pendente' not in session:
        flash('Sessão inválida', 'error')
        return redirect(url_for('cliente.verificar_cpf'))
    
    # Buscar descrição do sorteio atual
    from database import db
    conectado = db.connect()
    
    descricao_sorteio = "Novo sorteio disponível"  # Fallback
    
    if conectado:
        sql_descricao = "SELECT descricao FROM Sorteio_qz.dbo.tab_sorteio WHERE num_sorteio = (SELECT MAX(num_sorteio) FROM Sorteio_qz.dbo.tab_sorteio)"
        resultado_descricao = db.execute_select(sql_descricao)
        if resultado_descricao and resultado_descricao[0][0]:
            descricao_sorteio = resultado_descricao[0][0]
        db.close()
    
    return render_template('confirmar_sorteio.html', descricao_sorteio=descricao_sorteio)

@cliente_bp.route('/processar_confirmacao_sorteio', methods=['POST'])
def processar_confirmacao_sorteio():
    """Processar confirmação de participação no sorteio"""
    if 'cpf_pendente' not in session:
        flash('Sessão inválida', 'error')
        return redirect(url_for('cliente.verificar_cpf'))
    
    confirmacao = request.form.get('confirmacao')
    cpf = session.get('cpf_pendente')
    
    if confirmacao == 'sim':
        # Usuário quer continuar - registrar no histórico e atualizar saldo
        try:
            # Buscar dados completos do cliente
            cliente_completo = ClienteBusiness.buscar_cliente_completo_por_cpf(cpf)
            if not cliente_completo:
                flash('Erro interno. Tente novamente.', 'error')
                return redirect(url_for('cliente.confirmar_sorteio'))
            
            id_cliente = cliente_completo.get('id')
            saldo_atual = cliente_completo.get('saldo', 0)
            
            # Registrar no histórico e zerar saldo
            sucesso = ClienteBusiness.registrar_mudanca_sorteio(id_cliente, saldo_atual)
            
            if sucesso:
                session['cpf'] = cpf
                session.pop('cpf_pendente', None)
                flash('Login realizado com sucesso! Seu saldo anterior foi preservado no histórico.', 'success')
                
                # Buscar telefone do cliente para salvar no cache
                cliente_dados = ClienteBusiness.buscar_cliente_por_cpf(cpf)
                telefone_cliente = cliente_dados.get('telefone', '') if cliente_dados else ''
                return redirect(url_for('cliente.salvar_login_cache', cpf=cpf, telefone=telefone_cliente))
            else:
                flash('Erro ao processar mudança de sorteio. Tente novamente.', 'error')
                return redirect(url_for('cliente.confirmar_sorteio'))
                
        except Exception as e:
            logging.error(f"Erro ao processar confirmação de sorteio: {e}")
            flash('Erro interno. Tente novamente.', 'error')
            return redirect(url_for('cliente.confirmar_sorteio'))
    else:
        # Usuário não quer continuar - voltar ao início
        session.pop('cpf_pendente', None)
        flash('Login cancelado', 'info')
        return redirect(url_for('main.index'))

@cliente_bp.route('/sair')
def sair():
    """Limpar sessão e voltar para início"""
    session.clear()
    flash('Sessão encerrada com sucesso', 'info')
    return redirect(url_for('main.index'))

@cliente_bp.route('/salvar_login_cache')
def salvar_login_cache():
    """Página intermediária para salvar dados no localStorage"""
    cpf = request.args.get('cpf', '')
    telefone = request.args.get('telefone', '')
    
    # Buscar dados completos do cliente
    cliente = ClienteBusiness.buscar_cliente_por_cpf(cpf)
    if not cliente:
        flash('Erro ao recuperar dados do cliente', 'error')
        return redirect(url_for('dashboard.dashboard'))
    
    return render_template('salvar_login_cache.html', 
                         cpf=cpf, 
                         telefone=telefone, 
                         nome=cliente.get('nome', ''),
                         data_nascimento=cliente.get('data_nascimento', ''))

@cliente_bp.route('/verificar_login_automatico', methods=['POST'])
def verificar_login_automatico():
    """Verificar se é possível fazer login automático com dados salvos"""
    try:
        cpf = request.form.get('cpf', '').strip()
        telefone = request.form.get('telefone', '').strip()
        
        if not cpf or not telefone:
            flash('Dados insuficientes para login automático', 'error')
            return redirect(url_for('cliente.verificar_cpf'))
        
        # Buscar cliente por CPF
        cliente = ClienteBusiness.buscar_cliente_por_cpf(cpf)
        
        if not cliente:
            flash('CPF não encontrado. Cadastre-se novamente.', 'error')
            return redirect(url_for('cliente.verificar_cpf'))
        
        # Verificar se o telefone confere
        telefone_banco = cliente.get('telefone', '')
        telefone_limpo = ''.join(filter(str.isdigit, telefone))
        telefone_banco_limpo = ''.join(filter(str.isdigit, telefone_banco))
        
        if telefone_limpo != telefone_banco_limpo:
            flash('Dados salvos inválidos. Faça login novamente.', 'error')
            return redirect(url_for('cliente.verificar_cpf'))
        
        # Verificar versão do sorteio
        num_sorteio_cliente = cliente.get('num_sorteio')
        
        from database import db
        conectado = db.connect()
        
        if conectado:
            sql_sorteio_atual = "SELECT MAX(num_sorteio) FROM Sorteio_qz.dbo.tab_sorteio"
            resultado_sorteio = db.execute_select(sql_sorteio_atual)
            num_sorteio_atual = resultado_sorteio[0][0] if resultado_sorteio and resultado_sorteio[0][0] else 1
            db.close()
            
            if num_sorteio_cliente == num_sorteio_atual:
                # Login automático bem-sucedido
                session['cpf'] = cpf
                flash('Login automático realizado com sucesso!', 'success')
                return redirect(url_for('dashboard.dashboard'))
            else:
                # Sorteio diferente - redirecionar para confirmação
                session['cpf_pendente'] = cpf
                return redirect(url_for('cliente.confirmar_sorteio'))
        else:
            # Sem conexão com banco - aceitar login automático direto
            session['cpf'] = cpf
            flash('Login automático realizado com sucesso!', 'success')
            return redirect(url_for('dashboard.dashboard'))
            
    except Exception as e:
        logging.error(f"Erro no login automático: {e}")
        flash('Erro ao tentar login automático. Faça login manualmente.', 'error')
        return redirect(url_for('cliente.verificar_cpf'))