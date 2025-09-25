from flask import Blueprint, render_template, request, jsonify
import logging
from business.informacoes import InformacoesBusiness

# Criar blueprint para rotas de informações
informacoes_bp = Blueprint('informacoes', __name__)

@informacoes_bp.route('/informacoes')
def informacoes():
    """Mostrar informações e regras do sorteio"""
    try:
        # Obter todas as informações
        info_completas = InformacoesBusiness.obter_informacoes_completas()
        
        return render_template('informacoes.html', 
                             regras=info_completas['regras'],
                             premios=info_completas['premios'],
                             cronograma=info_completas['cronograma'],
                             contatos=info_completas['contatos'],
                             faq=info_completas['faq'])
        
    except Exception as e:
        logging.error(f"Erro ao carregar informações: {e}")
        return render_template('informacoes.html', 
                             regras=[], premios=[], cronograma=[], 
                             contatos=[], faq=[])

@informacoes_bp.route('/api/regras')
def api_regras():
    """API para obter regras do sorteio"""
    try:
        regras = InformacoesBusiness.obter_regras_sorteio()
        
        # Organizar regras por tipo
        regras_organizadas = {}
        for regra in regras:
            tipo = regra[0]
            if tipo not in regras_organizadas:
                regras_organizadas[tipo] = []
            regras_organizadas[tipo].append({
                'descricao': regra[1],
                'valor': regra[2]
            })
        
        return jsonify({'regras': regras_organizadas})
        
    except Exception as e:
        logging.error(f"Erro ao obter regras: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@informacoes_bp.route('/api/premios')
def api_premios():
    """API para obter prêmios do sorteio"""
    try:
        premios = InformacoesBusiness.obter_premios()
        
        premios_formatados = [
            {
                'posicao': premio[0],
                'descricao': premio[1],
                'valor': float(premio[2]),
                'detalhes': premio[3]
            } for premio in premios
        ]
        
        valor_total = InformacoesBusiness.calcular_valor_total_premios()
        
        return jsonify({
            'premios': premios_formatados,
            'valor_total': valor_total
        })
        
    except Exception as e:
        logging.error(f"Erro ao obter prêmios: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@informacoes_bp.route('/api/cronograma')
def api_cronograma():
    """API para obter cronograma do sorteio"""
    try:
        cronograma = InformacoesBusiness.obter_cronograma()
        
        cronograma_formatado = [
            {
                'evento': evento[0],
                'data_evento': evento[1],
                'descricao': evento[2],
                'status': evento[3]
            } for evento in cronograma
        ]
        
        return jsonify({'cronograma': cronograma_formatado})
        
    except Exception as e:
        logging.error(f"Erro ao obter cronograma: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@informacoes_bp.route('/api/contatos')
def api_contatos():
    """API para obter contatos"""
    try:
        contatos = InformacoesBusiness.obter_contatos()
        
        contatos_formatados = {}
        for contato in contatos:
            contatos_formatados[contato[0]] = {
                'valor': contato[1],
                'ativo': bool(contato[2])
            }
        
        return jsonify({'contatos': contatos_formatados})
        
    except Exception as e:
        logging.error(f"Erro ao obter contatos: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@informacoes_bp.route('/api/faq')
def api_faq():
    """API para obter FAQ"""
    try:
        faq = InformacoesBusiness.obter_faq()
        
        # Organizar FAQ por categoria
        faq_organizadas = {}
        for item in faq:
            categoria = item[2]
            if categoria not in faq_organizadas:
                faq_organizadas[categoria] = []
            faq_organizadas[categoria].append({
                'pergunta': item[0],
                'resposta': item[1],
                'ordem': item[3]
            })
        
        # Ordenar dentro de cada categoria
        for categoria in faq_organizadas:
            faq_organizadas[categoria].sort(key=lambda x: x['ordem'])
        
        return jsonify({'faq': faq_organizadas})
        
    except Exception as e:
        logging.error(f"Erro ao obter FAQ: {e}")
        return jsonify({'erro': 'Erro interno do servidor'}), 500

@informacoes_bp.route('/faq')
def faq():
    """Página específica para FAQ"""
    try:
        faq_items = InformacoesBusiness.obter_faq()
        return render_template('faq.html', faq=faq_items)
        
    except Exception as e:
        logging.error(f"Erro ao carregar FAQ: {e}")
        return render_template('faq.html', faq=[])

@informacoes_bp.route('/regulamento')
def regulamento():
    """Página específica para regulamento completo"""
    try:
        regras = InformacoesBusiness.obter_regras_sorteio()
        premios = InformacoesBusiness.obter_premios()
        cronograma = InformacoesBusiness.obter_cronograma()
        
        return render_template('regulamento.html', 
                             regras=regras, 
                             premios=premios, 
                             cronograma=cronograma)
        
    except Exception as e:
        logging.error(f"Erro ao carregar regulamento: {e}")
        return render_template('regulamento.html', 
                             regras=[], premios=[], cronograma=[])