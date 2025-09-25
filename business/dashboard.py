import logging
from database import db
from business.cadastrar_cliente import ExampleDataClientes
from business.cadastrar_nota import ExampleDataNotas
from business.numero_sorte import ExampleDataNumeros


class DashboardBusiness:
    """Classe de negócio para operações do dashboard"""

    @staticmethod
    def obter_resumo_cliente(cpf):
        """
        Obter resumo completo do cliente para dashboard
        
        SQLs utilizadas:
        - SELECT nome, cpf, telefone FROM clientes WHERE cpf = 'CPF'
        - SELECT COUNT(*) FROM notas WHERE cpf = 'CPF'
        - SELECT COUNT(*) FROM numeros_sorte WHERE cpf = 'CPF'
        - SELECT SUM(valor) FROM notas WHERE cpf = 'CPF' AND validada = 1
        """
        try:
            # Conectar ao banco
            conectado = db.connect()

            if conectado:
                # Limpar CPF - apenas números
                cpf = ''.join(filter(str.isdigit, cpf))

                # Buscar informações do cliente incluindo num_sorteio e novo_cadastro
                cliente_sql = "SELECT nome, cpf, telefone, saldo, num_sorteio, novo_cadastro FROM tab_cliente WHERE cpf = ?"
                cliente_result = db.execute_select(cliente_sql, (cpf,))
                
                if not cliente_result:
                    db.close()
                    return None
                
                num_sorteio_cliente = cliente_result[0][4]  # num_sorteio é o 5º campo

                # Buscar informações do sorteio atual
                sorteio_sql = "SELECT num_sorteio, descricao FROM tab_sorteio WHERE num_sorteio = ?"
                sorteio_result = db.execute_select(sorteio_sql, (num_sorteio_cliente,))

                # Buscar total de notas cadastradas do sorteio atual do cliente
                notas_sql = "SELECT COUNT(*) FROM tab_nota WHERE cpf = ? AND num_sorteio = ?"
                notas_result = db.execute_select(notas_sql, (cpf, num_sorteio_cliente))

                # Buscar total de números da sorte do sorteio atual do cliente
                numeros_sql = "SELECT COUNT(*) FROM tab_numero_sorte WHERE cpf = ? AND num_sorteio = ?"
                numeros_result = db.execute_select(numeros_sql, (cpf, num_sorteio_cliente))

                # Buscar valor total em notas validadas do sorteio atual do cliente
                valor_validado_sql = "SELECT ISNULL(SUM(valor), 0) FROM tab_nota WHERE cpf = ? AND status = 1 AND num_sorteio = ?"
                valor_validado_result = db.execute_select(valor_validado_sql, (cpf, num_sorteio_cliente))

                # Buscar notas pendentes de validação do sorteio atual do cliente
                notas_pendentes_sql = "SELECT COUNT(*) FROM tab_nota WHERE cpf = ? AND status = 0 AND num_sorteio = ?"
                notas_pendentes_result = db.execute_select(notas_pendentes_sql, (cpf, num_sorteio_cliente))

                db.close()

                resumo = {
                    'cliente':
                    cliente_result[0] if cliente_result else
                    ('Cliente Exemplo', cpf, '(11) 99999-9999'),
                    'total_notas':
                    notas_result[0][0] if notas_result else 0,
                    'total_numeros':
                    numeros_result[0][0] if numeros_result else 0,
                    'valor_validado':
                    valor_validado_result[0][0]
                    if valor_validado_result else 0,
                    'notas_pendentes':
                    notas_pendentes_result[0][0]
                    if notas_pendentes_result else 0,
                    'sorteio':
                    sorteio_result[0] if sorteio_result else (num_sorteio_cliente, 'Sorteio Atual')
                }
            else:
                # Fallback: usar dados de exemplo
                cliente_info = None
                num_sorteio_cliente = 1  # Padrão para modo exemplo
                
                for cliente in ExampleDataClientes.clientes:
                    if cliente.get('cpf') == cpf:
                        cliente_info = (cliente.get('nome', ''),
                                        cliente.get('cpf', ''),
                                        cliente.get('telefone', ''),
                                        0,  # saldo exemplo
                                        1,  # num_sorteio exemplo
                                        0)  # novo_cadastro exemplo
                        num_sorteio_cliente = cliente.get('num_sorteio', 1)
                        break

                if not cliente_info:
                    return None

                # Dados de sorteio de exemplo
                sorteio_info = (num_sorteio_cliente, f'Sorteio {num_sorteio_cliente} - Exemplo')

                # Filtrar apenas por CPF e num_sorteio do cliente
                total_notas = len([
                    n for n in ExampleDataNotas.notas 
                    if n.get('cpf') == cpf and n.get('num_sorteio') == num_sorteio_cliente
                ])
                total_numeros = len([
                    n for n in ExampleDataNumeros.numeros_sorte
                    if n.get('cpf') == cpf and n.get('num_sorteio') == num_sorteio_cliente
                ])
                valor_validado = sum([
                    n.get('valor', 0) for n in ExampleDataNotas.notas
                    if n.get('cpf') == cpf and n.get('validada', False) and n.get('num_sorteio') == num_sorteio_cliente
                ])
                notas_pendentes = len([
                    n for n in ExampleDataNotas.notas
                    if n.get('cpf') == cpf and not n.get('validada', False) and n.get('num_sorteio') == num_sorteio_cliente
                ])

                resumo = {
                    'cliente':
                    cliente_info if cliente_info else
                    ('Cliente Exemplo', cpf, '(11) 99999-9999'),
                    'total_notas':
                    total_notas,
                    'total_numeros':
                    total_numeros,
                    'valor_validado':
                    valor_validado,
                    'notas_pendentes':
                    notas_pendentes,
                    'sorteio':
                    sorteio_info
                }

            return resumo

        except Exception as e:
            logging.error(f"Erro ao obter resumo: {e}")
            return {
                'cliente': ('Cliente Exemplo', cpf, '(11) 99999-9999'),
                'total_notas': 0,
                'total_numeros': 0,
                'valor_validado': 0,
                'notas_pendentes': 0,
                'sorteio': (1, 'Sorteio de Exemplo')
            }

    @staticmethod
    def obter_estatisticas_gerais():
        """
        Obter estatísticas gerais do sistema
        
        SQLs utilizadas:
        - SELECT COUNT(*) FROM clientes
        - SELECT COUNT(*) FROM notas
        - SELECT COUNT(*) FROM numeros_sorte
        - SELECT SUM(valor) FROM notas WHERE validada = 1
        """
        try:
            # Conectar ao banco
            db.connect()

            # Total de clientes
            clientes_sql = "SELECT COUNT(*) FROM tab_cliente"
            clientes_result = db.execute_select(clientes_sql)

            # Total de notas
            notas_sql = "SELECT COUNT(*) FROM tab_nota"
            notas_result = db.execute_select(notas_sql)

            # Total de números da sorte
            numeros_sql = "SELECT COUNT(*) FROM numeros_sorte"
            numeros_result = db.execute_select(numeros_sql)

            # Valor total arrecadado
            valor_total_sql = "SELECT ISNULL(SUM(valor), 0) FROM tab_nota WHERE status = 1"
            valor_total_result = db.execute_select(valor_total_sql)

            db.close()

            estatisticas = {
                'total_clientes':
                clientes_result[0][0] if clientes_result else 0,
                'total_notas':
                notas_result[0][0] if notas_result else 0,
                'total_numeros':
                numeros_result[0][0] if numeros_result else 0,
                'valor_total':
                valor_total_result[0][0] if valor_total_result else 0
            }

            return estatisticas

        except Exception as e:
            logging.error(f"Erro ao obter estatísticas gerais: {e}")
            return {
                'total_clientes': 0,
                'total_notas': 0,
                'total_numeros': 0,
                'valor_total': 0
            }

    @staticmethod
    def obter_atividade_recente(cpf, limite=5):
        """
        Obter atividades recentes do cliente
        
        SQLs utilizadas:
        - SELECT 'nota' as tipo, num_nota as referencia, valor, data_cadastro FROM notas WHERE cpf = 'CPF'
          UNION
          SELECT 'numero' as tipo, num_sorte as referencia, 0 as valor, data_cadastro FROM numeros_sorte WHERE cpf = 'CPF'
          ORDER BY data_cadastro DESC
        """
        try:
            # Conectar ao banco
            db.connect()

            # Buscar atividades recentes (notas e números)
            atividades_sql = """
                SELECT 'nota' as tipo, num_nota as referencia, valor, data_cadastro 
                FROM notas WHERE cpf = ?
                UNION
                SELECT 'numero' as tipo, num_sorte as referencia, 0 as valor, data_cadastro 
                FROM numeros_sorte WHERE cpf = ?
                ORDER BY data_cadastro DESC
                LIMIT ?
            """
            atividades_result = db.execute_select(atividades_sql, (cpf, cpf, limite))

            db.close()

            return atividades_result if atividades_result else []

        except Exception as e:
            logging.error(f"Erro ao obter atividade recente: {e}")
            return []

    @staticmethod
    def obter_ranking_participantes(limite=10):
        """
        Obter ranking dos participantes com mais números da sorte
        
        SQL utilizada:
        - SELECT c.nome, c.cpf, COUNT(n.id) as total_numeros 
          FROM clientes c 
          LEFT JOIN numeros_sorte n ON c.cpf = n.cpf 
          GROUP BY c.nome, c.cpf 
          ORDER BY total_numeros DESC
        """
        try:
            # Conectar ao banco
            db.connect()

            # Buscar ranking
            ranking_sql = """
                SELECT c.nome, c.cpf, COUNT(n.id) as total_numeros 
                FROM clientes c 
                LEFT JOIN numeros_sorte n ON c.cpf = n.cpf 
                GROUP BY c.nome, c.cpf 
                ORDER BY total_numeros DESC
                LIMIT ?
            """
            ranking_result = db.execute_select(ranking_sql, (limite,))

            db.close()

            return ranking_result if ranking_result else []

        except Exception as e:
            logging.error(f"Erro ao obter ranking: {e}")
            return []
