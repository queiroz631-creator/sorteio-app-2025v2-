import logging
from datetime import datetime
from database import db
from utils.timezone import format_brasilia_datetime

class ExampleDataClientes:
    """Dados de exemplo para clientes quando banco SQL Server não disponível"""
    clientes = []

class ClienteBusiness:
    """Classe de negócio para operações com clientes"""
    
    @staticmethod
    def validar_cpf(cpf):
        """Validar CPF usando algoritmo oficial brasileiro"""
        # Remove caracteres especiais
        cpf = ''.join(filter(str.isdigit, cpf))
        
        # Verifica se tem 11 dígitos
        if len(cpf) != 11:
            return False
        
        # Verifica se todos os dígitos são iguais (CPFs inválidos conhecidos)
        if cpf == cpf[0] * 11:
            return False
        
        # Calcula o primeiro dígito verificador
        soma = 0
        for i in range(9):
            soma += int(cpf[i]) * (10 - i)
        
        resto = soma % 11
        if resto < 2:
            digito1 = 0
        else:
            digito1 = 11 - resto
        
        # Verifica o primeiro dígito
        if int(cpf[9]) != digito1:
            return False
        
        # Calcula o segundo dígito verificador
        soma = 0
        for i in range(10):
            soma += int(cpf[i]) * (11 - i)
        
        resto = soma % 11
        if resto < 2:
            digito2 = 0
        else:
            digito2 = 11 - resto
        
        # Verifica o segundo dígito
        if int(cpf[10]) != digito2:
            return False
        
        return True
    
    @staticmethod
    def validar_telefone(telefone):
        """Validar formato básico do telefone"""
        # Remove caracteres especiais
        numeros = ''.join(filter(str.isdigit, telefone))
        
        # Verifica se tem 10 ou 11 dígitos
        return len(numeros) in [10, 11]
    
    @staticmethod
    def cadastrar_cliente(nome, cpf, telefone, data_nascimento):
        """
        Cadastrar novo cliente
        
        Args:
            nome: Nome completo do cliente
            cpf: CPF do cliente
            telefone: Telefone do cliente
            data_nascimento: Data de nascimento do cliente (formato DD/MM/AAAA ou YYYY-MM-DD)
        
        SQL: 
        1. SELECT COUNT(*) FROM tab_cliente WHERE cpf = 'CPF' (verificar duplicata)
        2. INSERT INTO tab_cliente... (cadastrar se não existe)
        """
        try:
            if not ClienteBusiness.validar_cpf(cpf):
                return False, "CPF inválido"
            
            if not ClienteBusiness.validar_telefone(telefone):
                return False, "Telefone inválido"
            
            # Converter nome para maiúsculo
            nome = nome.upper().strip()
            
            if not nome or len(nome) < 3:
                return False, "Nome deve ter pelo menos 3 caracteres"
            
            if not data_nascimento:
                return False, "Data de nascimento é obrigatória"
            
            # Validar idade mínima de 18 anos
            try:
                from datetime import date
                # Tentar primeiro formato DD/MM/AAAA, depois AAAA-MM-DD como fallback
                try:
                    data_nasc = datetime.strptime(data_nascimento, '%d/%m/%Y').date()
                except ValueError:
                    # Fallback para formato antigo AAAA-MM-DD
                    data_nasc = datetime.strptime(data_nascimento, '%Y-%m-%d').date()
                
                hoje = date.today()
                idade = hoje.year - data_nasc.year - ((hoje.month, hoje.day) < (data_nasc.month, data_nasc.day))
                
                if idade < 18:
                    return False, "Você deve ter pelo menos 18 anos para participar"
                
                # Converter para formato banco de dados (AAAA-MM-DD) se veio no formato DD/MM/AAAA
                data_nascimento = data_nasc.strftime('%Y-%m-%d')
                    
            except ValueError:
                return False, "Data de nascimento inválida. Use o formato DD/MM/AAAA"
            
            # Limpar CPF - apenas números
            cpf_limpo = ''.join(filter(str.isdigit, cpf))
            
            # Conectar ao banco
            conectado = db.connect()
            
            if conectado:
                # Verificar se CPF já existe
                sql_verificar = "SELECT COUNT(*) FROM tab_cliente WHERE cpf = ?"
                resultado_verificacao = db.execute_select(sql_verificar, (cpf_limpo,))
                
                if resultado_verificacao and resultado_verificacao[0][0] > 0:
                    db.close()
                    return False, "CPF já cadastrado no sistema"
                
                # Limpar telefone - apenas números
                telefone_limpo = ''.join(filter(str.isdigit, telefone))
                
                # Data cadastro - formato completo YYYY-MM-DD HH:MM:SS
                data_cadastro = format_brasilia_datetime()

                # Obter o número do sorteio atual
                sql_sorteio = "SELECT TOP 1 num_sorteio FROM Sorteio_qz.dbo.tab_sorteio ORDER BY num_sorteio DESC"
                resultado_sorteio = db.execute_select(sql_sorteio)
                num_sorteio = resultado_sorteio[0][0] if resultado_sorteio and resultado_sorteio[0][0] else 1
                
                # Executar INSERT direto
                sql = "INSERT INTO tab_cliente (nome, cpf, telefone, dt_nascimento, ativo, saldo, atualizado, dt_cadastro, novo_cadastro, num_sorteio) VALUES (?, ?, ?, ?, 1, 0, 0, ?, 1, ?)"
                resultado = db.execute_insert_update_delete(sql, (nome, cpf_limpo, telefone_limpo, data_nascimento, data_cadastro, num_sorteio))
                
                if resultado:
                    # Obter o ID do cliente recém-cadastrado
                    sql_id = "SELECT id FROM tab_cliente WHERE cpf = ?"
                    resultado_id = db.execute_select(sql_id, (cpf_limpo,))
                    
                    if resultado_id and resultado_id[0]:
                        id_cliente = resultado_id[0][0]
                        
                        # Inserir registro no histórico do sorteio
                        sql_historico = "INSERT INTO tab_historico_sorteio (id_cliente, num_sorteio, dt_registro, saldo_anterior) VALUES (?, ?, ?, 0)"
                        db.execute_insert_update_delete(sql_historico, (id_cliente, num_sorteio, data_cadastro))
                
                db.close()
                
                if resultado:
                    return True, "Cliente cadastrado com sucesso"
                else:
                    return False, "Erro ao cadastrar cliente"
            else:
                # Limpar CPF e telefone também no fallback
                cpf_limpo = ''.join(filter(str.isdigit, cpf))
                telefone_limpo = ''.join(filter(str.isdigit, telefone))
                
                # Verificar se CPF já existe no fallback
                for cliente_existente in ExampleDataClientes.clientes:
                    if cliente_existente.get('cpf') == cpf_limpo:
                        return False, "CPF já cadastrado no sistema"
                
                # Fallback: usar dados de exemplo
                cliente_id = len(ExampleDataClientes.clientes) + 1
                cliente = {
                    'id': cliente_id,
                    'nome': nome.strip(),
                    'cpf': cpf_limpo,
                    'telefone': telefone_limpo,
                    'data_nascimento': data_nascimento,
                    'data_cadastro': format_brasilia_datetime()
                }
                ExampleDataClientes.clientes.append(cliente)
                
                # Inserir também no histórico (modo exemplo)
                from business.historico_sorteio import ExampleDataHistorico
                historico = {
                    'id': len(ExampleDataHistorico.historico_sorteio) + 1,
                    'id_cliente': cliente_id,
                    'num_sorteio': 1,  # Padrão para modo exemplo
                    'dt_registro': format_brasilia_datetime(),
                    'saldo_anterior': 0
                }
                ExampleDataHistorico.historico_sorteio.append(historico)
                
                return True, "Cliente cadastrado com sucesso (modo exemplo)"
                
        except Exception as e:
            logging.error(f"Erro ao cadastrar cliente: {e}")
            return False, f"Erro interno: {str(e)}"
    
    @staticmethod
    def buscar_cliente_por_cpf(cpf):
        """
        Buscar cliente por CPF
        
        SQL: SELECT nome, cpf, telefone, data_cadastro FROM clientes WHERE cpf = 'CPF'
        """
        try:
            # Conectar ao banco
            conectado = db.connect()
            
            if conectado:
                # Limpar CPF para busca - apenas números
                cpf_limpo = ''.join(filter(str.isdigit, cpf))
                
                # Executar consulta incluindo num_sorteio
                sql = "SELECT nome, cpf, telefone, dt_cadastro, num_sorteio FROM tab_cliente WHERE cpf = ?"
                resultado = db.execute_select(sql, (cpf_limpo,))
                
                db.close()
                
                if resultado:
                    # Retornar como dicionário para facilitar acesso
                    cliente_data = resultado[0]
                    return {
                        'nome': cliente_data[0],
                        'cpf': cliente_data[1],
                        'telefone': cliente_data[2],
                        'data_cadastro': cliente_data[3],
                        'num_sorteio': cliente_data[4]
                    }
                return None
            else:
                # Limpar CPF para busca no fallback
                cpf_limpo = ''.join(filter(str.isdigit, cpf))
                
                # Fallback: buscar nos dados de exemplo
                for cliente in ExampleDataClientes.clientes:
                    if cliente.get('cpf') == cpf_limpo:
                        return {
                            'nome': cliente.get('nome', ''),
                            'cpf': cliente.get('cpf', ''),
                            'telefone': cliente.get('telefone', ''),
                            'data_cadastro': cliente.get('data_cadastro', ''),
                            'num_sorteio': cliente.get('num_sorteio', 1)  # Padrão para fallback
                        }
                return None
            
        except Exception as e:
            logging.error(f"Erro ao buscar cliente: {e}")
            return None
    
    @staticmethod
    def buscar_cliente_completo_por_cpf(cpf):
        """
        Buscar cliente completo por CPF incluindo ID e saldo
        
        SQL: SELECT id, nome, cpf, telefone, saldo, num_sorteio FROM tab_cliente WHERE cpf = 'CPF'
        """
        try:
            # Conectar ao banco
            conectado = db.connect()
            
            if conectado:
                # Limpar CPF para busca - apenas números
                cpf_limpo = ''.join(filter(str.isdigit, cpf))
                
                # Executar consulta incluindo id e saldo
                sql = "SELECT id, nome, cpf, telefone, saldo, num_sorteio FROM tab_cliente WHERE cpf = ?"
                resultado = db.execute_select(sql, (cpf_limpo,))
                
                db.close()
                
                if resultado:
                    # Retornar como dicionário para facilitar acesso
                    cliente_data = resultado[0]
                    return {
                        'id': cliente_data[0],
                        'nome': cliente_data[1],
                        'cpf': cliente_data[2],
                        'telefone': cliente_data[3],
                        'saldo': cliente_data[4],
                        'num_sorteio': cliente_data[5]
                    }
                return None
            else:
                # Limpar CPF para busca no fallback
                cpf_limpo = ''.join(filter(str.isdigit, cpf))
                
                # Fallback: buscar nos dados de exemplo
                for cliente in ExampleDataClientes.clientes:
                    if cliente.get('cpf') == cpf_limpo:
                        return {
                            'id': cliente.get('id'),
                            'nome': cliente.get('nome', ''),
                            'cpf': cliente.get('cpf', ''),
                            'telefone': cliente.get('telefone', ''),
                            'saldo': cliente.get('saldo', 0),
                            'num_sorteio': cliente.get('num_sorteio', 1)
                        }
                return None
            
        except Exception as e:
            logging.error(f"Erro ao buscar cliente completo: {e}")
            return None
    
    @staticmethod
    def registrar_mudanca_sorteio(id_cliente, saldo_anterior):
        """
        Registrar mudança de sorteio no histórico e zerar saldo do cliente
        
        SQL: INSERT INTO tab_historico_sorteio (id_cliente, num_sorteio, dt_registro, saldo_anterior) VALUES (?, ?, ?, ?)
        SQL: UPDATE tab_cliente SET saldo = 0 WHERE id = ?
        """
        try:
            # Conectar ao banco
            conectado = db.connect()
            
            if conectado:
                # Obter o número do sorteio atual
                sql_sorteio = "SELECT TOP 1 num_sorteio FROM Sorteio_qz.dbo.tab_sorteio ORDER BY num_sorteio DESC"
                resultado_sorteio = db.execute_select(sql_sorteio)
                num_sorteio_atual = resultado_sorteio[0][0] if resultado_sorteio and resultado_sorteio[0][0] else 1
                
                # Inserir registro no histórico
                dt_registro = format_brasilia_datetime()
                sql_historico = "INSERT INTO tab_historico_sorteio (id_cliente, num_sorteio, dt_registro, saldo_anterior) VALUES (?, ?, ?, ?)"
                resultado_historico = db.execute_insert_update_delete(sql_historico, (id_cliente, num_sorteio_atual, dt_registro, saldo_anterior))
                
                if resultado_historico:
                    # Zerar saldo do cliente e atualizar num_sorteio
                    sql_update = "UPDATE tab_cliente SET saldo = 0, num_sorteio = ? WHERE id = ?"
                    resultado_update = db.execute_insert_update_delete(sql_update, (num_sorteio_atual, id_cliente))
                    
                    db.close()
                    return resultado_update
                else:
                    db.close()
                    return False
            else:
                # Fallback: atualizar dados de exemplo
                for cliente in ExampleDataClientes.clientes:
                    if cliente.get('id') == id_cliente:
                        cliente['saldo'] = 0
                        cliente['num_sorteio'] = 1  # Atualizar para sorteio atual (modo exemplo)
                        break
                
                # Inserir também no histórico (modo exemplo)
                from business.historico_sorteio import ExampleDataHistorico
                historico = {
                    'id': len(ExampleDataHistorico.historico_sorteio) + 1,
                    'id_cliente': id_cliente,
                    'num_sorteio': 1,  # Padrão para modo exemplo
                    'dt_registro': format_brasilia_datetime(),
                    'saldo_anterior': saldo_anterior
                }
                ExampleDataHistorico.historico_sorteio.append(historico)
                return True
                
        except Exception as e:
            logging.error(f"Erro ao registrar mudança de sorteio: {e}")
            return False
    
    @staticmethod
    def atualizar_cliente(nome, cpf, telefone):
        """
        Atualizar dados do cliente
        
        SQL: exec proc_cliente_update @nome='Nome', @cpf='CPF', @telefone='Telefone'
        """
        try:
            if not ClienteBusiness.validar_cpf(cpf):
                return False, "CPF inválido"
            
            if not ClienteBusiness.validar_telefone(telefone):
                return False, "Telefone inválido"
            
            if not nome or len(nome.strip()) < 3:
                return False, "Nome deve ter pelo menos 3 caracteres"
            
            # Conectar ao banco
            db.connect()
            
            # Executar UPDATE direto
            sql = "UPDATE clientes SET nome=?, telefone=? WHERE cpf=?"
            resultado = db.execute_insert_update_delete(sql, (nome, telefone, cpf))
            
            db.close()
            
            if resultado:
                return True, "Cliente atualizado com sucesso"
            else:
                return False, "Erro ao atualizar cliente"
                
        except Exception as e:
            logging.error(f"Erro ao atualizar cliente: {e}")
            return False, f"Erro interno: {str(e)}"
    
    @staticmethod
    def listar_todos_clientes():
        """
        Listar todos os clientes
        
        SQL: SELECT nome, cpf, telefone, data_cadastro FROM clientes ORDER BY data_cadastro DESC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            # Executar consulta
            sql = "SELECT nome, cpf, telefone, data_cadastro FROM clientes ORDER BY data_cadastro DESC"
            resultado = db.execute_select(sql)
            
            db.close()
            
            return resultado if resultado else []
            
        except Exception as e:
            logging.error(f"Erro ao listar clientes: {e}")
            return []