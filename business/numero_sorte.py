import logging
import random
from datetime import datetime
from database import db
from utils.timezone import format_brasilia_datetime


class ExampleDataNumeros:
    """Dados de exemplo para números da sorte quando banco SQL Server não disponível"""
    numeros_sorte = []


class NumeroSorteBusiness:
    """Classe de negócio para operações com números da sorte"""

    @staticmethod
    def gerar_numeros_sorte(cpf, quantidade, db_connection=None):
        """
        Gerar números da sorte para o cliente
        
        Args:
            cpf: CPF do cliente
            quantidade: Quantidade de números a gerar
            db_connection: Instância de conexão de banco (opcional, usa global se não fornecida)
        """
        # Inicializar variáveis para evitar erros LSP
        db_instance = None
        usar_conexao_global = False

        try:
            if quantidade <= 0:
                return True, "Nenhum número para gerar"

            # Usar conexão fornecida ou global
            db_instance = db_connection if db_connection else db

            # Se usando conexão global, precisa conectar/desconectar
            usar_conexao_global = db_connection is None
            if usar_conexao_global:
                db_instance.connect()

            # Buscar número do sorteio atual uma vez só
            sql_num_sorteio = "SELECT TOP 1 num_sorteio FROM Sorteio_qz.dbo.tab_sorteio ORDER BY num_sorteio DESC"
            resultado_num = db_instance.execute_select(sql_num_sorteio)

            if not resultado_num or not resultado_num[0][0]:
                return False, "Nenhum sorteio encontrado para gerar números"

            num_sorteio_atual = resultado_num[0][0]

            # Limpar CPF - apenas números
            cpf = ''.join(filter(str.isdigit, cpf))

            # Buscar números existentes para este sorteio para garantir unicidade
            # Agora os números já estão concatenados no banco
            sql_existentes = "SELECT num_sorte FROM tab_numero_sorte WHERE num_sorteio = ?"
            resultado_existentes = db_instance.execute_select(
                sql_existentes, (num_sorteio_atual, ))
            numeros_existentes = set([row[0] for row in resultado_existentes
                                      ]) if resultado_existentes else set()

            # Gerar cada número da sorte
            data_cadastro = format_brasilia_datetime()
            sql_insert = "INSERT INTO tab_numero_sorte (cpf, dt_cadastro, num_sorteio, num_sorte) VALUES (?, ?, ?, ?)"

            numeros_gerados = []
            tentativas_max = 1000  # Limite de tentativas para evitar loop infinito

            for i in range(quantidade):
                tentativas = 0
                numero_encontrado = False

                while not numero_encontrado and tentativas < tentativas_max:
                    # Gerar número aleatório de 5 dígitos
                    num_sorte_base = random.randint(10000, 99999)
                    
                    # Concatenar ANTES de enviar para o banco
                    num_sorte_concatenado = int(f"{num_sorteio_atual}{num_sorte_base:05d}")

                    # Verificar se não existe e não foi gerado nesta sessão
                    if num_sorte_concatenado not in numeros_existentes and num_sorte_concatenado not in numeros_gerados:
                        try:
                            # Verificação final no banco antes de inserir para garantir unicidade
                            sql_verificar = "SELECT COUNT(*) FROM tab_numero_sorte WHERE num_sorte = ? AND num_sorteio = ?"
                            resultado_verificar = db_instance.execute_select(sql_verificar, (num_sorte_concatenado, num_sorteio_atual))
                            
                            if resultado_verificar and resultado_verificar[0][0] == 0:
                                # Número concatenado é único, pode inserir
                                sucesso = db_instance.execute_insert_update_delete(
                                    sql_insert, (cpf, data_cadastro,
                                                 num_sorteio_atual, num_sorte_concatenado))

                                if sucesso:
                                    numeros_gerados.append(num_sorte_concatenado)
                                    numeros_existentes.add(num_sorte_concatenado)
                                    numero_encontrado = True
                                    logging.debug(f'Número concatenado {num_sorte_concatenado} inserido no banco (sorteio {num_sorteio_atual})')
                            else:
                                logging.debug(f'Número concatenado {num_sorte_concatenado} já existe, tentando outro')
                        except Exception as e:
                            logging.warning(
                                f"Erro ao verificar/inserir número concatenado {num_sorte_concatenado}: {e}")

                    tentativas += 1

                if not numero_encontrado:
                    # Se esgotou tentativas, retornar erro
                    if usar_conexao_global:
                        db_instance.close()
                    return False, f"Não foi possível gerar número único após {tentativas_max} tentativas"

            # Fechar conexão se for global
            if usar_conexao_global:
                db_instance.close()

            return True, f"{len(numeros_gerados)} número(s) da sorte gerado(s) com sucesso"
        except Exception as e:
            logging.error(f"Erro ao gerar números da sorte: {e}")
            # Garantir fechamento em caso de erro
            if usar_conexao_global and db_instance:
                try:
                    db_instance.close()
                except:
                    pass
            return False, f"Erro interno: {str(e)}"

    @staticmethod
    def obter_numeros_cliente(cpf):
        """
        Obter todos os números da sorte do cliente
        
        SQL: SELECT num_sorte, data_cadastro FROM numeros_sorte WHERE cpf = 'CPF' ORDER BY data_cadastro DESC
        """
        try:
            # Conectar ao banco
            conectado = db.connect()

            if conectado:
                # Limpar CPF - apenas números
                cpf = ''.join(filter(str.isdigit, cpf))

                # Buscar número do sorteio do cliente
                sql_cliente = "SELECT num_sorteio FROM tab_cliente WHERE cpf = ?"
                resultado_cliente = db.execute_select(sql_cliente, (cpf,))

                if not resultado_cliente:
                    db.close()
                    return []

                num_sorteio_cliente = resultado_cliente[0][0]

                # Buscar números da sorte do cliente filtrando pelo seu num_sorteio
                # Agora num_sorte já está concatenado no banco
                sql = "SELECT num_sorte, dt_cadastro FROM tab_numero_sorte WHERE cpf = ? AND num_sorteio = ? ORDER BY dt_cadastro DESC"
                resultado = db.execute_select(sql, (cpf, num_sorteio_cliente))

                db.close()

                return resultado if resultado else []
            else:
                # Fallback: buscar nos dados de exemplo
                # Primeiro buscar o num_sorteio do cliente nos dados de exemplo
                num_sorteio_cliente = 1  # Padrão
                from business.cadastrar_cliente import ExampleDataClientes
                for cliente in ExampleDataClientes.clientes:
                    if cliente.get('cpf') == cpf:
                        num_sorteio_cliente = cliente.get('num_sorteio', 1)
                        break
                
                numeros_cliente = []
                for numero in ExampleDataNumeros.numeros_sorte:
                    if numero.get('cpf') == cpf and numero.get('num_sorteio') == num_sorteio_cliente:
                        # No modo exemplo, num_sorte já deve estar concatenado
                        numeros_cliente.append(
                            (numero.get('num_sorte', ''), numero.get('data_cadastro', '')))

                # Ordenar por data (mais recente primeiro)
                numeros_cliente.sort(key=lambda x: x[1], reverse=True)
                return numeros_cliente

        except Exception as e:
            logging.error(f"Erro ao obter números da sorte: {e}")
            return []

    @staticmethod
    def verificar_numero_sorteado(num_sorte):
        """
        Verificar se um número foi sorteado
        
        SQL: SELECT COUNT(*) FROM historico_sorteio WHERE num_sorte_vencedor = 'Numero'
        """
        try:
            # Conectar ao banco
            db.connect()

            sql = "SELECT COUNT(*) FROM historico_sorteio WHERE num_sorte_vencedor = ?"
            resultado = db.execute_select(sql, (num_sorte, ))

            db.close()

            return resultado and resultado[0][0] > 0

        except Exception as e:
            logging.error(f"Erro ao verificar número sorteado: {e}")
            return False

    @staticmethod
    def obter_estatisticas_numeros():
        """
        Obter estatísticas dos números da sorte
        
        SQLs utilizadas:
        - SELECT COUNT(*) FROM numeros_sorte
        - SELECT COUNT(DISTINCT cpf) FROM numeros_sorte
        - SELECT cpf, COUNT(*) as total FROM numeros_sorte GROUP BY cpf ORDER BY total DESC LIMIT 1
        """
        try:
            # Conectar ao banco
            db.connect()

            # Total de números gerados
            total_sql = "SELECT COUNT(*) FROM tab_numero_sorte"
            total_result = db.execute_select(total_sql)

            # Total de participantes únicos
            participantes_sql = "SELECT COUNT(DISTINCT cpf) FROM tab_numero_sorte"
            participantes_result = db.execute_select(participantes_sql)

            # Cliente com mais números
            maior_participante_sql = """
                SELECT cpf, COUNT(*) as total 
                FROM tab_numero_sorte 
                GROUP BY cpf 
                ORDER BY total DESC 
                LIMIT 1
            """
            maior_participante_result = db.execute_select(
                maior_participante_sql)

            db.close()

            estatisticas = {
                'total_numeros':
                total_result[0][0] if total_result else 0,
                'total_participantes':
                participantes_result[0][0] if participantes_result else 0,
                'maior_participante':
                maior_participante_result[0]
                if maior_participante_result else None
            }

            return estatisticas

        except Exception as e:
            logging.error(f"Erro ao obter estatísticas: {e}")
            return {
                'total_numeros': 0,
                'total_participantes': 0,
                'maior_participante': None
            }

    @staticmethod
    def listar_numeros_por_data(data_inicio, data_fim):
        """
        Listar números gerados em um período
        
        SQL: SELECT num_sorte, cpf, data_cadastro FROM numeros_sorte 
             WHERE data_cadastro BETWEEN 'DataInicio' AND 'DataFim' 
             ORDER BY data_cadastro DESC
        """
        try:
            # Conectar ao banco
            db.connect()

            sql = """
                SELECT num_sorte, cpf, dt_cadastro 
                FROM tab_numero_sorte 
                WHERE dt_cadastro BETWEEN ? AND ? 
                ORDER BY dt_cadastro DESC
            """
            resultado = db.execute_select(sql, (data_inicio, data_fim))

            db.close()

            return resultado if resultado else []

        except Exception as e:
            logging.error(f"Erro ao listar números por data: {e}")
            return []

    @staticmethod
    def validar_numero_formato(num_sorte):
        """
        Validar formato do número da sorte
        Formato esperado: 6 dígitos (100000-999999)
        """
        if not num_sorte:
            return False

        # Converter para string se for número
        num_str = str(num_sorte)

        # Verificar se tem 6 dígitos
        if len(num_str) != 6:
            return False

        # Verificar se são todos dígitos
        if not num_str.isdigit():
            return False

        # Verificar se está no range válido
        numero = int(num_str)
        if numero < 100000 or numero > 999999:
            return False

        return True
