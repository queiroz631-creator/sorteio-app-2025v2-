import logging
from datetime import datetime
from database import db
from utils.timezone import format_brasilia_datetime


class ExampleDataNotas:
    """Dados de exemplo para notas quando banco SQL Server não disponível"""
    notas = []


class CadastrarNotaBusiness:
    """Classe de negócio para cadastro de tab_nota"""

    @staticmethod
    def calcular_numeros_sorte(valor_total):
        """
        Calcular quantos números da sorte devem ser gerados
        Regra: 1 número para cada R$ 20,00
        """
        if valor_total >= 20:
            return int(valor_total // 20)
        return 0

    @staticmethod
    def cadastrar_nota(valor, num_nota, cpf):
        """
        Cadastrar nova nota fiscal
        
        SQL: exec proc_nota_insert @opcao=1, @valor=Valor, @num_nota='Numero', @cpf='CPF', @data_cadastro='Data'
        """
        try:
            if valor < 5.00:
                return False, "Valor deve ser de pelo menos R$ 5,00"

            if not num_nota or len(num_nota.strip()) < 5:
                return False, "Número da nota deve ter pelo menos 5 caracteres"

            if not cpf or len(cpf.replace('-', '').replace('.', '')) != 11:
                return False, "CPF inválido"

            # Verificar se ainda está no período de cadastro
            if not CadastrarNotaBusiness.verificar_periodo_cadastro():
                return False, "Período de cadastro de notas encerrado"

            # Verificar se a nota já existe
            if CadastrarNotaBusiness.verificar_nota_existente(num_nota):
                return False, "Esta nota já foi cadastrada"

            # Conectar ao banco
            conectado = db.connect()

            if conectado:
                # Buscar número do sorteio atual
                sql_num_sorteio = "SELECT TOP 1 num_sorteio FROM Sorteio_qz.dbo.tab_sorteio ORDER BY num_sorteio DESC"
                resultado_num = db.execute_select(sql_num_sorteio)

                if not resultado_num or not resultado_num[0][0]:
                    db.close()
                    return True  # Se não tem sorteio, permite cadastro

                num_sorteio_atual = resultado_num[0][0]
                
                # Limpar CPF - apenas números
                cpf = ''.join(filter(str.isdigit, cpf))

                # Executar INSERT direto
                data_cadastro = format_brasilia_datetime()
                sql = "INSERT INTO tab_nota (num_nota, valor, cpf, status, somar, dt_registro, num_sorteio, sincronizada) VALUES (?, ?, ?, 0, 1, ?, ?, 0)"
                resultado = db.execute_insert_update_delete(
                    sql, (num_nota, valor, cpf, data_cadastro, num_sorteio_atual))

                db.close()

                if resultado:
                    return True, "Nota cadastrada com sucesso"
                else:
                    return False, "Erro ao cadastrar nota"
            else:
                # Fallback: usar dados de exemplo
                nota = {
                    'id': len(ExampleDataNotas.notas) + 1,
                    'valor': valor,
                    'num_nota': num_nota.strip(),
                    'cpf': cpf,
                    'validada': True,
                    'sincronizada': 0,
                    'data_cadastro':
                    format_brasilia_datetime()
                }
                ExampleDataNotas.notas.append(nota)
                return True, "Nota cadastrada com sucesso (modo exemplo)"

        except Exception as e:
            logging.error(f"Erro ao cadastrar nota: {e}")
            return False, f"Erro interno: {str(e)}"

    @staticmethod
    def verificar_nota_existente(num_nota):
        """
        Verificar se uma nota já foi cadastrada
        
        SQL: SELECT COUNT(*) FROM tab_nota WHERE num_nota = 'Numero'
        """
        try:
            # Conectar ao banco
            db.connect()

            # Executar consulta
            sql = "SELECT COUNT(*) FROM tab_nota WHERE num_nota = ?"
            resultado = db.execute_select(sql, (num_nota,))

            db.close()

            # Retorna True se a nota já existe (count > 0)
            return resultado and resultado[0][0] > 0

        except Exception as e:
            logging.error(f"Erro ao verificar nota existente: {e}")
            return False

    @staticmethod
    def verificar_periodo_cadastro():
        """
        Verificar se ainda está no período de cadastro de notas
        
        SQL: 
        1. "SELECT TOP 1 num_sorteio FROM Sorteio_qz.dbo.tab_sorteio ORDER BY num_sorteio DESC"
        2. SELECT dt_final FROM tab_sorteio WHERE num_sorteio = [num_sorteio_atual]
        """
        try:
            # Conectar ao banco
            conectado = db.connect()

            if conectado:
                # Buscar número do sorteio atual
                sql_num_sorteio = "SELECT TOP 1 num_sorteio FROM Sorteio_qz.dbo.tab_sorteio ORDER BY num_sorteio DESC"
                resultado_num = db.execute_select(sql_num_sorteio)

                if not resultado_num or not resultado_num[0][0]:
                    db.close()
                    return True  # Se não tem sorteio, permite cadastro

                num_sorteio_atual = resultado_num[0][0]

                # Buscar data final do sorteio
                sql_dt_final = "SELECT dt_final FROM tab_sorteio WHERE num_sorteio = ?"
                resultado_dt = db.execute_select(sql_dt_final,
                                                 (num_sorteio_atual, ))

                db.close()

                if not resultado_dt or not resultado_dt[0][0]:
                    return True  # Se não tem data final definida, permite cadastro

                dt_final = resultado_dt[0][0]
                from utils.timezone import get_brasilia_now
                data_atual = get_brasilia_now()

                # Verificar se data atual é menor que data final
                if isinstance(dt_final, str):
                    dt_final = datetime.strptime(dt_final, '%Y-%m-%d %H:%M:%S')

                return data_atual < dt_final
            else:
                # No modo fallback, sempre permite cadastro
                return True

        except Exception as e:
            logging.error(f"Erro ao verificar período de cadastro: {e}")
            # Em caso de erro, permite cadastro para não bloquear o sistema
            return True

    @staticmethod
    def validar_nota(num_nota):
        """
        Validar/aprovar uma nota fiscal
        
        SQL: exec proc_nota_update @num_nota='Numero', @validada=1
        """
        try:
            # Conectar ao banco
            db.connect()

            # Executar UPDATE direto
            sql = "UPDATE tab_nota SET status=1 WHERE num_nota=?"
            resultado = db.execute_insert_update_delete(sql, (num_nota,))

            db.close()

            if resultado:
                return True, "Nota validada com sucesso"
            else:
                return False, "Erro ao validar nota"

        except Exception as e:
            logging.error(f"Erro ao validar nota: {e}")
            return False, f"Erro interno: {str(e)}"

    @staticmethod
    def editar_nota(num_nota_original, novo_valor, novo_num_nota):
        """
        Editar uma nota fiscal (apenas se não validada)
        
        SQL: exec proc_nota_update @num_nota_original='Original', @valor=NovoValor, @num_nota='NovoNumero'
        """
        try:
            if novo_valor < 5.00:
                return False, "Valor deve ser de pelo menos R$ 5,00"

            if not novo_num_nota or len(novo_num_nota.strip()) < 5:
                return False, "Número da nota deve ter pelo menos 5 caracteres"

            # Verificar se a nota está validada
            if CadastrarNotaBusiness.verificar_nota_validada(
                    num_nota_original):
                return False, "Não é possível editar uma nota já validada"

            # Verificar se o novo número já existe (apenas se diferente do original)
            if novo_num_nota.strip() != num_nota_original.strip():
                if CadastrarNotaBusiness.verificar_nota_existente(
                        novo_num_nota):
                    return False, "Este número de nota já foi cadastrado"

            # Conectar ao banco
            db.connect()

            # Executar UPDATE direto
            status_nota = 0
            sql = "UPDATE tab_nota SET valor=?, num_nota=?, status=?, sincronizada=0 WHERE num_nota=?"
            resultado = db.execute_insert_update_delete(
                sql, (novo_valor, novo_num_nota, status_nota, num_nota_original))

            db.close()

            if resultado:
                return True, "Nota editada com sucesso"
            else:
                return False, "Erro ao editar nota"

        except Exception as e:
            logging.error(f"Erro ao editar nota: {e}")
            return False, f"Erro interno: {str(e)}"

    @staticmethod
    def verificar_nota_validada(num_nota):
        """
        Verificar se uma nota está validada
        
        SQL: SELECT status FROM tab_nota WHERE num_nota = 'Numero' AND status = 1
        """
        try:
            # Conectar ao banco
            db.connect()

            # Executar consulta
            sql = "SELECT status FROM tab_nota WHERE num_nota = ? AND status = 1"
            resultado = db.execute_select(sql, (num_nota,))

            db.close()

            # Retorna True se a nota está validada
            return resultado and resultado[0][0] == True

        except Exception as e:
            logging.error(f"Erro ao verificar validação da nota: {e}")
            return False

    @staticmethod
    def excluir_nota(num_nota):
        """
        Excluir uma nota fiscal (apenas se não validada)
        
        SQL: exec proc_nota_delete @num_nota='Numero'
        """
        try:
            # Verificar se a nota está validada
            if CadastrarNotaBusiness.verificar_nota_validada(num_nota):
                return False, "Não é possível excluir uma nota já validada"

            # Conectar ao banco
            db.connect()

            # Executar DELETE direto
            sql = "DELETE FROM tab_nota WHERE num_nota=?"
            resultado = db.execute_insert_update_delete(sql, (num_nota,))

            db.close()

            if resultado:
                return True, "Nota excluída com sucesso"
            else:
                return False, "Erro ao excluir nota"

        except Exception as e:
            logging.error(f"Erro ao excluir nota: {e}")
            return False, f"Erro interno: {str(e)}"
