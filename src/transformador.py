"""
Módulo para transformação de dados
Aqui a mágica acontece - limpar, formatar, criar novas colunas
"""

import pandas as pd
from datetime import datetime

class TransformadorDados:
    """Aplica todas as transformações necessárias nos dados"""
    
    def padronizar_nomes(self, dados):
        """Padroniza os nomes dos clientes"""
        dados_transformados = dados.copy()
        
        if 'nome' not in dados_transformados.columns:
            return dados_transformados
        
        # Remove espaços extras
        dados_transformados['nome'] = dados_transformados['nome'].str.strip()
        
        # Coloca primeira letra maiúscula em cada palavra
        # Fiz essa função personalizada porque str.title() não trata bem os acentos
        def formatar_nome(nome):
            if pd.isna(nome):
                return nome
            
            # Divide o nome em partes
            partes = nome.split()
            partes_formatadas = []
            
            for parte in partes:
                if len(parte) > 0:
                    # Primeira letra maiúscula, resto minúscula
                    parte_formatada = parte[0].upper() + parte[1:].lower()
                    partes_formatadas.append(parte_formatada)
            
            return ' '.join(partes_formatadas)
        
        dados_transformados['nome'] = dados_transformados['nome'].apply(formatar_nome)
        
        return dados_transformados
    
    def gerar_mensagens(self, dados):
        """Gera mensagens personalizadas para cada cliente"""
        dados_com_mensagens = dados.copy()
        
        # Adiciona a coluna de mensagens
        dados_com_mensagens['mensagem_personalizada'] = ''
        
        # Para cada cliente, cria uma mensagem específica
        for idx, linha in dados_com_mensagens.iterrows():
            nome = linha['nome'] if 'nome' in linha else 'Cliente'
            tipo_conta = linha.get('tipo_conta', 'CONTA')
            
            # Lógica de negócio para diferentes tipos de conta
            if 'POUPANÇA' in str(tipo_conta).upper():
                mensagem = f"Olá {nome}, sua Poupança está rendendo mais que a poupança tradicional! Que tal agendar uma consulta para ver outras opções?"
            
            elif 'INVESTIMENTO' in str(tipo_conta).upper():
                mensagem = f"Prezado(a) {nome}, temos oportunidades exclusivas para sua conta Investimento. Seu patrimônio pode render ainda mais!"
            
            elif 'SALÁRIO' in str(tipo_conta).upper():
                mensagem = f"Oi {nome}! Aproveite as vantagens da conta Salário: TEDs gratuitos e cartão sem anuidade. Conheça todos os benefícios!"
            
            else:  # Conta corrente padrão
                mensagem = f"Olá {nome}! Sua conta corrente tem novidades. Acesse o app para conferir as novas funcionalidades disponíveis."
            
            dados_com_mensagens.at[idx, 'mensagem_personalizada'] = mensagem
        
        return dados_com_mensagens
    
    def criar_resumos(self, dados):
        """Cria um resumo para cada cliente"""
        dados_com_resumos = dados.copy()
        
        # Adiciona colunas de resumo
        dados_com_resumos['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Cria um ID único para este processamento
        dados_com_resumos['processamento_id'] = f"ETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Resumo em texto
        def criar_resumo_texto(linha):
            elementos = []
            
            if 'nome' in linha and pd.notna(linha['nome']):
                elementos.append(f"Nome: {linha['nome']}")
            
            if 'conta' in linha and pd.notna(linha['conta']):
                elementos.append(f"Conta: {linha['conta']}")
            
            if 'tipo_conta' in linha and pd.notna(linha['tipo_conta']):
                elementos.append(f"Tipo: {linha['tipo_conta']}")
            
            return " | ".join(elementos)
        
        dados_com_resumos['resumo_cliente'] = dados_com_resumos.apply(criar_resumo_texto, axis=1)
        
        return dados_com_resumos
    
    def calcular_metricas(self, dados):
        """Calcula algumas métricas básicas sobre os dados"""
        if dados.empty:
            return {}
        
        metricas = {
            'total_clientes': len(dados),
            'tipos_conta_unicos': dados['tipo_conta'].nunique() if 'tipo_conta' in dados.columns else 0,
            'data_processamento': datetime.now().isoformat()
        }
        
        return metricas