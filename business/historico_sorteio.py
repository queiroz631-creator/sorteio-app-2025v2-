import logging
from datetime import datetime
from database import db
from utils.timezone import format_brasilia_datetime

class ExampleDataHistorico:
    """Dados de exemplo para histórico quando banco SQL Server não disponível"""
    historico_sorteio = []

class HistoricoSorteioBusiness:
    """Classe de negócio para operações com histórico de sorteios"""
    
    @staticmethod
    def registrar_participacao(id_cliente, versao_sorteio, saldo_anterior=0):
        """
        Registrar participação do cliente no sorteio
        
        SQL: exec proc_historico_sorteio_insert @opcao=1, @id_cliente=ID, @versao_sorteio='Versao', @data_cadastro='Data', @saldo_anterior=Saldo
        """
        try:
            # Conectar ao banco
            conectado = db.connect()
            
            if conectado:
                # Executar INSERT direto
                data_cadastro = format_brasilia_datetime()
                sql = "INSERT INTO historico_sorteio (id_cliente, versao_sorteio, data_cadastro, saldo_anterior) VALUES (?, ?, ?, ?)"
                resultado = db.execute_insert_update_delete(sql, (id_cliente, versao_sorteio, data_cadastro, saldo_anterior))
                
                db.close()
                
                if resultado:
                    return True, "Participação registrada com sucesso"
                else:
                    return False, "Erro ao registrar participação"
            else:
                # Fallback: usar dados de exemplo
                historico = {
                    'id': len(ExampleDataHistorico.historico_sorteio) + 1,
                    'id_cliente': id_cliente,
                    'versao_sorteio': versao_sorteio,
                    'data_cadastro': format_brasilia_datetime(),
                    'saldo_anterior': saldo_anterior
                }
                ExampleDataHistorico.historico_sorteio.append(historico)
                return True, "Participação registrada com sucesso (modo exemplo)"
                
        except Exception as e:
            logging.error(f"Erro ao registrar participação: {e}")
            return False, f"Erro interno: {str(e)}"
    
    @staticmethod
    def obter_historico_cliente(cpf):
        """
        Obter histórico de participações do cliente
        
        SQL: SELECT h.versao_sorteio, h.data_cadastro, h.saldo_anterior, c.nome 
             FROM historico_sorteio h 
             INNER JOIN clientes c ON h.id_cliente = c.id 
             WHERE c.cpf = 'CPF' 
             ORDER BY h.data_cadastro DESC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            # Limpar CPF - apenas números
            cpf_limpo = ''.join(filter(str.isdigit, cpf))
            
            sql = """
                SELECT h.versao_sorteio, h.data_cadastro, h.saldo_anterior, c.nome 
                FROM historico_sorteio h 
                INNER JOIN clientes c ON h.id_cliente = c.id 
                WHERE c.cpf = ? 
                ORDER BY h.data_cadastro DESC
            """
            resultado = db.execute_select(sql, (cpf_limpo,))
            
            db.close()
            
            return resultado if resultado else []
            
        except Exception as e:
            logging.error(f"Erro ao obter histórico do cliente: {e}")
            return []
    
    @staticmethod
    def obter_todos_sorteios():
        """
        Obter lista de todos os sorteios realizados
        
        SQL: SELECT DISTINCT versao_sorteio, MIN(data_cadastro) as data_sorteio, COUNT(*) as total_participantes
             FROM historico_sorteio 
             GROUP BY versao_sorteio 
             ORDER BY data_sorteio DESC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = """
                SELECT DISTINCT versao_sorteio, MIN(data_cadastro) as data_sorteio, COUNT(*) as total_participantes
                FROM historico_sorteio 
                GROUP BY versao_sorteio 
                ORDER BY data_sorteio DESC
            """
            resultado = db.execute_select(sql)
            
            db.close()
            
            return resultado if resultado else []
            
        except Exception as e:
            logging.error(f"Erro ao obter todos os sorteios: {e}")
            return []
    
    @staticmethod
    def obter_participantes_sorteio(versao_sorteio):
        """
        Obter participantes de um sorteio específico
        
        SQL: SELECT c.nome, c.cpf, h.data_cadastro, h.saldo_anterior 
             FROM historico_sorteio h 
             INNER JOIN clientes c ON h.id_cliente = c.id 
             WHERE h.versao_sorteio = 'Versao' 
             ORDER BY h.data_cadastro ASC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = """
                SELECT c.nome, c.cpf, h.data_cadastro, h.saldo_anterior 
                FROM historico_sorteio h 
                INNER JOIN clientes c ON h.id_cliente = c.id 
                WHERE h.versao_sorteio = ? 
                ORDER BY h.data_cadastro ASC
            """
            resultado = db.execute_select(sql, (versao_sorteio,))
            
            db.close()
            
            return resultado if resultado else []
            
        except Exception as e:
            logging.error(f"Erro ao obter participantes do sorteio: {e}")
            return []
    
    @staticmethod
    def registrar_resultado_sorteio(versao_sorteio, num_sorte_vencedor, premio_descricao):
        """
        Registrar resultado de um sorteio
        
        SQL: exec proc_resultado_sorteio_insert @versao_sorteio='Versao', @num_sorte_vencedor='Numero', @premio_descricao='Premio', @data_sorteio='Data'
        """
        try:
            # Conectar ao banco
            db.connect()
            
            # Executar INSERT direto
            data_sorteio = format_brasilia_datetime()
            sql = "INSERT INTO resultado_sorteio (versao_sorteio, num_sorte_vencedor, premio_descricao, data_sorteio) VALUES (?, ?, ?, ?)"
            resultado = db.execute_insert_update_delete(sql, (versao_sorteio, num_sorte_vencedor, premio_descricao, data_sorteio))
            
            db.close()
            
            if resultado:
                return True, "Resultado do sorteio registrado com sucesso"
            else:
                return False, "Erro ao registrar resultado do sorteio"
                
        except Exception as e:
            logging.error(f"Erro ao registrar resultado: {e}")
            return False, f"Erro interno: {str(e)}"
    
    @staticmethod
    def obter_vencedores():
        """
        Obter lista de vencedores de todos os sorteios
        
        SQL: SELECT r.versao_sorteio, r.num_sorte_vencedor, r.premio_descricao, r.data_sorteio, c.nome, c.cpf
             FROM resultado_sorteio r
             INNER JOIN numeros_sorte n ON r.num_sorte_vencedor = n.num_sorte
             INNER JOIN clientes c ON n.cpf = c.cpf
             ORDER BY r.data_sorteio DESC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = """
                SELECT r.versao_sorteio, r.num_sorte_vencedor, r.premio_descricao, r.data_sorteio, c.nome, c.cpf
                FROM resultado_sorteio r
                INNER JOIN numeros_sorte n ON r.num_sorte_vencedor = n.num_sorte
                INNER JOIN clientes c ON n.cpf = c.cpf
                ORDER BY r.data_sorteio DESC
            """
            resultado = db.execute_select(sql)
            
            db.close()
            
            return resultado if resultado else []
            
        except Exception as e:
            logging.error(f"Erro ao obter vencedores: {e}")
            return []
    
    @staticmethod
    def obter_estatisticas_sorteio(versao_sorteio):
        """
        Obter estatísticas de um sorteio específico
        
        SQLs utilizadas:
        - SELECT COUNT(*) FROM historico_sorteio WHERE versao_sorteio = 'Versao'
        - SELECT COUNT(DISTINCT id_cliente) FROM historico_sorteio WHERE versao_sorteio = 'Versao'
        - SELECT SUM(saldo_anterior) FROM historico_sorteio WHERE versao_sorteio = 'Versao'
        """
        try:
            # Conectar ao banco
            db.connect()
            
            # Total de participações
            participacoes_sql = f"SELECT COUNT(*) FROM historico_sorteio WHERE versao_sorteio = '{versao_sorteio}'"
            participacoes_result = db.execute_select(participacoes_sql)
            
            # Participantes únicos
            participantes_sql = f"SELECT COUNT(DISTINCT id_cliente) FROM historico_sorteio WHERE versao_sorteio = '{versao_sorteio}'"
            participantes_result = db.execute_select(participantes_sql)
            
            # Saldo total anterior
            saldo_sql = f"SELECT ISNULL(SUM(saldo_anterior), 0) FROM historico_sorteio WHERE versao_sorteio = '{versao_sorteio}'"
            saldo_result = db.execute_select(saldo_sql)
            
            db.close()
            
            estatisticas = {
                'total_participacoes': participacoes_result[0][0] if participacoes_result else 0,
                'participantes_unicos': participantes_result[0][0] if participantes_result else 0,
                'saldo_total_anterior': saldo_result[0][0] if saldo_result else 0
            }
            
            return estatisticas
            
        except Exception as e:
            logging.error(f"Erro ao obter estatísticas do sorteio: {e}")
            return {
                'total_participacoes': 0,
                'participantes_unicos': 0,
                'saldo_total_anterior': 0
            }
    
    @staticmethod
    def verificar_cliente_participou(cpf, versao_sorteio):
        """
        Verificar se um cliente participou de um sorteio específico
        
        SQL: SELECT COUNT(*) FROM historico_sorteio h 
             INNER JOIN clientes c ON h.id_cliente = c.id 
             WHERE c.cpf = 'CPF' AND h.versao_sorteio = 'Versao'
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = f"""
                SELECT COUNT(*) FROM historico_sorteio h 
                INNER JOIN clientes c ON h.id_cliente = c.id 
                WHERE c.cpf = '{cpf}' AND h.versao_sorteio = '{versao_sorteio}'
            """
            resultado = db.execute_select(sql)
            
            db.close()
            
            return resultado and resultado[0][0] > 0
            
        except Exception as e:
            logging.error(f"Erro ao verificar participação: {e}")
            return False