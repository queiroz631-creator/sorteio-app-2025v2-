import logging
from database import db

class InformacoesBusiness:
    """Classe de negócio para informações do sistema"""
    
    @staticmethod
    def obter_regras_sorteio():
        """
        Obter regras do sorteio do banco de dados ou retornar padrão
        
        SQL: SELECT tipo_regra, descricao, valor FROM regras_sorteio WHERE ativo = 1 ORDER BY ordem ASC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = "SELECT tipo_regra, descricao, valor FROM regras_sorteio WHERE ativo = 1 ORDER BY ordem ASC"
            resultado = db.execute_select(sql)
            
            db.close()
            
            if resultado:
                return resultado
            else:
                # Retornar regras padrão se não houver no banco
                return InformacoesBusiness.obter_regras_padrao()
            
        except Exception as e:
            logging.error(f"Erro ao obter regras: {e}")
            return InformacoesBusiness.obter_regras_padrao()
    
    @staticmethod
    def obter_regras_padrao():
        """Retornar regras padrão do sorteio"""
        return [
            ('participacao', 'Ser maior de 18 anos', '18'),
            ('participacao', 'Possuir CPF válido', '1'),
            ('participacao', 'Residir no Brasil', '1'),
            ('participacao', 'Aceitar o regulamento', '1'),
            ('geracao', 'R$ 20,00 = 1 número da sorte', '20'),
            ('geracao', 'Números gerados automaticamente', '1'),
            ('geracao', 'Sem limite de números por participante', '0'),
            ('validacao', 'Notas devem ser válidas', '1'),
            ('validacao', 'Validação em até 24 horas', '24'),
            ('validacao', 'Valor mínimo: R$ 1,00', '1')
        ]
    
    @staticmethod
    def obter_premios():
        """
        Obter lista de prêmios do banco de dados
        
        SQL: SELECT posicao, descricao, valor, detalhes FROM premios WHERE ativo = 1 ORDER BY posicao ASC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = "SELECT posicao, descricao, valor, detalhes FROM premios WHERE ativo = 1 ORDER BY posicao ASC"
            resultado = db.execute_select(sql)
            
            db.close()
            
            if resultado:
                return resultado
            else:
                # Retornar prêmios padrão se não houver no banco
                return InformacoesBusiness.obter_premios_padrao()
            
        except Exception as e:
            logging.error(f"Erro ao obter prêmios: {e}")
            return InformacoesBusiness.obter_premios_padrao()
    
    @staticmethod
    def obter_premios_padrao():
        """Retornar prêmios padrão"""
        return [
            (1, 'Carro 0KM', 80000.00, 'Veículo popular, ano atual, emplacado e com seguro'),
            (2, 'Moto 0KM', 15000.00, 'Motocicleta 160cc, ano atual, emplacada'),
            (3, 'Smartphone Premium', 2500.00, 'Celular premium, último lançamento'),
            (4, 'Smartphone Premium', 2500.00, 'Celular premium, último lançamento'),
            (5, 'Smartphone Premium', 2500.00, 'Celular premium, último lançamento'),
            (6, 'Smartphone Premium', 2500.00, 'Celular premium, último lançamento'),
            (7, 'Smartphone Premium', 2500.00, 'Celular premium, último lançamento'),
            (8, 'Smartphone Premium', 2500.00, 'Celular premium, último lançamento'),
            (9, 'Smartphone Premium', 2500.00, 'Celular premium, último lançamento'),
            (10, 'Smartphone Premium', 2500.00, 'Celular premium, último lançamento')
        ]
    
    @staticmethod
    def obter_cronograma():
        """
        Obter cronograma do sorteio do banco de dados
        
        SQL: SELECT evento, data_evento, descricao, status FROM cronograma_sorteio ORDER BY data_evento ASC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = "SELECT evento, data_evento, descricao, status FROM cronograma_sorteio ORDER BY data_evento ASC"
            resultado = db.execute_select(sql)
            
            db.close()
            
            if resultado:
                return resultado
            else:
                # Retornar cronograma padrão se não houver no banco
                return InformacoesBusiness.obter_cronograma_padrao()
            
        except Exception as e:
            logging.error(f"Erro ao obter cronograma: {e}")
            return InformacoesBusiness.obter_cronograma_padrao()
    
    @staticmethod
    def obter_cronograma_padrao():
        """Retornar cronograma padrão"""
        return [
            ('Início das Inscrições', '2024-01-01', 'Abertura para cadastro de participantes', 'concluido'),
            ('Período de Cadastro', '2024-01-01', 'Cadastre suas notas fiscais até 30/11/2024', 'em_andamento'),
            ('Encerramento', '2024-11-30', 'Última oportunidade às 23h59', 'pendente'),
            ('Sorteio', '2024-12-15', 'Transmissão ao vivo às 20h', 'pendente')
        ]
    
    @staticmethod
    def obter_contatos():
        """
        Obter informações de contato do banco de dados
        
        SQL: SELECT tipo_contato, valor, ativo FROM contatos WHERE ativo = 1
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = "SELECT tipo_contato, valor, ativo FROM contatos WHERE ativo = 1"
            resultado = db.execute_select(sql)
            
            db.close()
            
            if resultado:
                return resultado
            else:
                # Retornar contatos padrão se não houver no banco
                return InformacoesBusiness.obter_contatos_padrao()
            
        except Exception as e:
            logging.error(f"Erro ao obter contatos: {e}")
            return InformacoesBusiness.obter_contatos_padrao()
    
    @staticmethod
    def obter_contatos_padrao():
        """Retornar contatos padrão"""
        return [
            ('telefone', '(11) 0800-123-4567', 1),
            ('email', 'suporte@sorteioqz.com.br', 1),
            ('whatsapp', '(11) 99999-9999', 1)
        ]
    
    @staticmethod
    def obter_faq():
        """
        Obter perguntas frequentes do banco de dados
        
        SQL: SELECT pergunta, resposta, categoria, ordem FROM faq WHERE ativo = 1 ORDER BY categoria, ordem ASC
        """
        try:
            # Conectar ao banco
            db.connect()
            
            sql = "SELECT pergunta, resposta, categoria, ordem FROM faq WHERE ativo = 1 ORDER BY categoria, ordem ASC"
            resultado = db.execute_select(sql)
            
            db.close()
            
            if resultado:
                return resultado
            else:
                # Retornar FAQ padrão se não houver no banco
                return InformacoesBusiness.obter_faq_padrao()
            
        except Exception as e:
            logging.error(f"Erro ao obter FAQ: {e}")
            return InformacoesBusiness.obter_faq_padrao()
    
    @staticmethod
    def obter_faq_padrao():
        """Retornar FAQ padrão"""
        return [
            ('Como participar do sorteio?', 'Cadastre-se gratuitamente e registre suas notas fiscais válidas.', 'participacao', 1),
            ('Quantos números posso ter?', 'Não há limite. A cada R$ 20,00 em notas validadas você ganha 1 número.', 'participacao', 2),
            ('Como sei se minha nota foi validada?', 'Acesse seu dashboard e verifique o status na seção "Minhas Notas".', 'validacao', 1),
            ('Quanto tempo leva para validar?', 'Até 24 horas úteis após o cadastro da nota.', 'validacao', 2),
            ('Quando será o sorteio?', 'Em 15 de dezembro de 2024, às 20h, com transmissão ao vivo.', 'sorteio', 1),
            ('Como saberei se ganhei?', 'Entraremos em contato pelos dados cadastrados e publicaremos os resultados.', 'sorteio', 2)
        ]
    
    @staticmethod
    def obter_informacoes_completas():
        """
        Obter todas as informações do sistema
        """
        try:
            informacoes = {
                'regras': InformacoesBusiness.obter_regras_sorteio(),
                'premios': InformacoesBusiness.obter_premios(),
                'cronograma': InformacoesBusiness.obter_cronograma(),
                'contatos': InformacoesBusiness.obter_contatos(),
                'faq': InformacoesBusiness.obter_faq()
            }
            
            return informacoes
            
        except Exception as e:
            logging.error(f"Erro ao obter informações completas: {e}")
            return {
                'regras': [],
                'premios': [],
                'cronograma': [],
                'contatos': [],
                'faq': []
            }
    
    @staticmethod
    def calcular_valor_total_premios():
        """
        Calcular valor total dos prêmios
        """
        try:
            premios = InformacoesBusiness.obter_premios()
            total = sum(float(premio[2]) for premio in premios)
            return total
            
        except Exception as e:
            logging.error(f"Erro ao calcular valor total dos prêmios: {e}")
            return 0