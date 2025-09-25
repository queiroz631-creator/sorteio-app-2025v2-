import threading
import time
import logging
from database import DatabaseConnection
from business.numero_sorte import NumeroSorteBusiness
from business.cadastrar_nota import CadastrarNotaBusiness


class ThreadVerificacaoNotas:
    """
    Thread que verifica automaticamente notas validadas e processa saldo/números da sorte
    """

    def __init__(self):
        self.running = False
        self.thread = None
        self.db = DatabaseConnection()

    def iniciar_thread(self):
        """Iniciar thread de verificação"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._verificar_notas_loop,
                                           daemon=True)
            self.thread.start()
            logging.info("Thread de verificação de notas iniciada")

    def parar_thread(self):
        """Parar thread de verificação"""
        self.running = False
        if self.thread:
            self.thread.join()
            logging.info("Thread de verificação de notas parada")

    def _verificar_notas_loop(self):
        """Loop principal da thread - executa a cada 10 segundos"""
        while self.running:
            try:
                self._processar_notas_validadas()
                self._processar_notas_canceladas()
                # Aguardar 10 segundos para verificação rápida
                time.sleep(60)
            except Exception as e:
                logging.error(f"Erro na thread de verificação: {e}")
                time.sleep(60)  # Aguardar mesmo em caso de erro

    def _processar_notas_validadas(self):
        """
        Processar notas validadas que ainda não foram somadas
        
        SQLs utilizadas:
        1. SELECT cpf, valor FROM tab_nota WHERE status = 1 AND somar = 1
        2. SELECT saldo FROM tab_cliente WHERE cpf = 'CPF'
        3. UPDATE tab_cliente SET saldo = 'NOVO_SALDO' WHERE cpf = 'CPF'
        4. UPDATE tab_nota SET somar = 0 WHERE cpf = 'CPF' AND status = 1 AND somar = 1
        """
        try:
            # Conectar ao banco
            conectado = self.db.connect()

            if not conectado:
                logging.warning(
                    "Não foi possível conectar ao banco para verificação de notas"
                )
                return

            # 1. Buscar notas validadas que ainda não foram somadas
            sql_notas = "SELECT cpf, valor FROM tab_nota WHERE status = 1 AND somar = 1"
            notas_para_processar = self.db.execute_select(sql_notas)

            if not notas_para_processar:
                # Não há notas para processar
                self.db.close()
                return

            # Agrupar por CPF para processar clientes
            clientes_notas = {}
            for cpf, valor in notas_para_processar:
                if cpf not in clientes_notas:
                    clientes_notas[cpf] = []
                clientes_notas[cpf].append(float(valor))

            logging.info(
                f"Processando {len(clientes_notas)} clientes com notas validadas"
            )

            # 2. Processar cada cliente
            for cpf, valores_notas in clientes_notas.items():
                self._processar_cliente(cpf, valores_notas)

            self.db.close()

        except Exception as e:
            logging.error(f"Erro ao processar notas validadas: {e}")
            try:
                self.db.close()
            except:
                pass

    def _processar_cliente(self, cpf, valores_notas):
        """
        Processar um cliente específico - gerar números e atualizar saldo restante
        Implementa controle de transação para garantir atomicidade
        """
        try:
            # Calcular total das notas validadas
            total_notas = sum(valores_notas)

            # 1. Buscar saldo atual do cliente
            sql_saldo = "SELECT ISNULL(saldo, 0) FROM tab_cliente WHERE cpf = ?"
            resultado_saldo = self.db.execute_select(sql_saldo, (cpf,))

            saldo_atual = 0
            if resultado_saldo and len(resultado_saldo) > 0:
                saldo_atual = float(resultado_saldo[0][0])

            # 2. Calcular novo saldo total
            novo_saldo_total = saldo_atual + total_notas

            # 3. Calcular quantos números deveria ter com o novo saldo
            quantidade_num = CadastrarNotaBusiness.calcular_numeros_sorte(
                novo_saldo_total)

            # 4. Calcular saldo restante (sobra dos múltiplos de R$ 20)
            saldo_restante = novo_saldo_total % 20

            # === INÍCIO DA TRANSAÇÃO ===
            # 5. Iniciar transação para garantir atomicidade
            self.db.execute_insert_update_delete("BEGIN TRANSACTION")
            
            try:
                # 6. Gerar números da sorte se necessário
                if quantidade_num > 0:
                    sucesso, mensagem = NumeroSorteBusiness.gerar_numeros_sorte(
                        cpf, quantidade_num, self.db)
                    if not sucesso:
                        raise Exception(f"Erro ao gerar números: {mensagem}")

                # 7. Atualizar saldo do cliente apenas com o valor restante
                sql_update_saldo = "UPDATE tab_cliente SET saldo = ? WHERE cpf = ?"
                resultado_saldo = self.db.execute_insert_update_delete(sql_update_saldo, (saldo_restante, cpf))
                if not resultado_saldo:
                    raise Exception("Falha ao atualizar saldo do cliente")

                # 8. Marcar notas como processadas (somar = 0)
                sql_update_notas = "UPDATE tab_nota SET somar = 0 WHERE cpf = ? AND status = 1 AND somar = 1"
                resultado_notas = self.db.execute_insert_update_delete(sql_update_notas, (cpf,))
                if not resultado_notas:
                    raise Exception("Falha ao marcar notas como processadas")

                # 9. Confirmar transação
                self.db.execute_insert_update_delete("COMMIT")

                logging.info(
                    f"Cliente {cpf}: {quantidade_num} números gerados, saldo restante R$ {saldo_restante:.2f}, {len(valores_notas)} notas processadas"
                )

            except Exception as transacao_erro:
                # Desfazer transação em caso de erro
                try:
                    self.db.execute_insert_update_delete("ROLLBACK")
                    logging.warning(f"Transação desfeita para cliente {cpf}: {transacao_erro}")
                except:
                    logging.error(f"Falha ao executar ROLLBACK para cliente {cpf}")
                raise transacao_erro

        except Exception as e:
            logging.error(f"Erro ao processar cliente {cpf}: {e}")

    def _processar_notas_canceladas(self):
        """
        Processar notas canceladas (status=3) para excluir números da sorte e ajustar saldo
        
        SQLs utilizadas:
        1. SELECT cpf, SUM(valor) FROM tab_nota WHERE status = 3 AND somar = 1 GROUP BY cpf
        2. SELECT saldo FROM tab_cliente WHERE cpf = 'CPF'
        3. SELECT TOP X num_sorte FROM tab_numero_sorte WHERE cpf = 'CPF' ORDER BY dt_cadastro DESC
        4. DELETE FROM tab_numero_sorte WHERE num_sorte IN (...)
        5. UPDATE tab_cliente SET saldo = 'NOVO_SALDO' WHERE cpf = 'CPF'
        6. UPDATE tab_nota SET somar = 0 WHERE cpf = 'CPF' AND status = 3 AND somar = 1
        """
        try:
            # Conectar ao banco
            conectado = self.db.connect()

            if not conectado:
                logging.warning(
                    "Não foi possível conectar ao banco para processar notas canceladas"
                )
                return

            # 1. Buscar notas canceladas que ainda não foram processadas
            sql_notas_canceladas = "SELECT cpf, SUM(valor) as total_cancelado FROM tab_nota WHERE status = 3 AND somar = 1 GROUP BY cpf"
            notas_canceladas = self.db.execute_select(sql_notas_canceladas)

            if not notas_canceladas:
                # Não há notas canceladas para processar
                self.db.close()
                return

            logging.info(
                f"Processando {len(notas_canceladas)} clientes com notas canceladas"
            )

            # 2. Processar cada cliente com notas canceladas
            for cpf, total_cancelado in notas_canceladas:
                self._processar_cliente_cancelado(cpf, float(total_cancelado))

            self.db.close()

        except Exception as e:
            logging.error(f"Erro ao processar notas canceladas: {e}")
            try:
                self.db.close()
            except:
                pass

    def _processar_cliente_cancelado(self, cpf, total_cancelado):
        """
        Processar um cliente específico com notas canceladas
        """
        try:
            # 1. Calcular quantos números da sorte devem ser excluídos
            numeros_para_excluir = int(total_cancelado // 20)
            saldo_restante_cancelado = total_cancelado % 20

            # 2. Buscar saldo atual do cliente
            sql_saldo = "SELECT ISNULL(saldo, 0) FROM tab_cliente WHERE cpf = ?"
            resultado_saldo = self.db.execute_select(sql_saldo, (cpf,))

            saldo_atual = 0
            if resultado_saldo and len(resultado_saldo) > 0:
                saldo_atual = float(resultado_saldo[0][0])

            # 3. Calcular novo saldo após cancelamento
            novo_saldo = saldo_atual - saldo_restante_cancelado

            # 4. Se saldo ficar negativo, pegar mais 1 número da sorte
            if novo_saldo < 0:
                numeros_para_excluir += 1
                novo_saldo = 20 + novo_saldo  # adiciona R$20 ao saldo negativo

            # === INÍCIO DA TRANSAÇÃO ===
            self.db.execute_insert_update_delete("BEGIN TRANSACTION")
            
            try:
                # 5. Buscar números da sorte para excluir (últimos inseridos)
                if numeros_para_excluir > 0:
                    # SQL Server não permite TOP com parâmetros, usar string interpolation com validação
                    numeros_para_excluir = int(numeros_para_excluir)  # Validação para evitar SQL injection
                    sql_buscar_numeros = f"SELECT TOP {numeros_para_excluir} num_sorte FROM tab_numero_sorte WHERE cpf = ? ORDER BY dt_cadastro DESC"
                    numeros_cliente = self.db.execute_select(sql_buscar_numeros, (cpf,))

                    if numeros_cliente:
                        # 6. Excluir os números da sorte
                        numeros_ids = [str(num[0]) for num in numeros_cliente]
                        placeholders = ','.join(['?' for _ in numeros_ids])
                        sql_delete_numeros = f"DELETE FROM tab_numero_sorte WHERE num_sorte IN ({placeholders})"
                        resultado_delete = self.db.execute_insert_update_delete(sql_delete_numeros, numeros_ids)
                        
                        if not resultado_delete:
                            raise Exception("Falha ao excluir números da sorte")

                        logging.info(f"Cliente {cpf}: {len(numeros_ids)} números excluídos: {numeros_ids}")

                # 7. Atualizar saldo do cliente
                sql_update_saldo = "UPDATE tab_cliente SET saldo = ? WHERE cpf = ?"
                resultado_saldo = self.db.execute_insert_update_delete(sql_update_saldo, (novo_saldo, cpf))
                if not resultado_saldo:
                    raise Exception("Falha ao atualizar saldo do cliente")

                # 8. Marcar notas canceladas como processadas (somar = 0)
                sql_update_notas = "UPDATE tab_nota SET somar = 0 WHERE cpf = ? AND status = 3 AND somar = 1"
                resultado_notas = self.db.execute_insert_update_delete(sql_update_notas, (cpf,))
                if not resultado_notas:
                    raise Exception("Falha ao marcar notas canceladas como processadas")

                # 9. Confirmar transação
                self.db.execute_insert_update_delete("COMMIT")

                logging.info(
                    f"Cliente {cpf}: R$ {total_cancelado:.2f} cancelado, {numeros_para_excluir} números excluídos, novo saldo R$ {novo_saldo:.2f}"
                )

            except Exception as transacao_erro:
                # Desfazer transação em caso de erro
                try:
                    self.db.execute_insert_update_delete("ROLLBACK")
                    logging.warning(f"Transação de cancelamento desfeita para cliente {cpf}: {transacao_erro}")
                except:
                    logging.error(f"Falha ao executar ROLLBACK para cliente cancelado {cpf}")
                raise transacao_erro

        except Exception as e:
            logging.error(f"Erro ao processar cliente cancelado {cpf}: {e}")


# Instância global da thread
thread_verificacao = ThreadVerificacaoNotas()
