"""
Módulo para extração de dados
Aqui é onde começa tudo - pegar os dados de algum lugar
"""

import pandas as pd
import os

class ExtratorDados:
    """Responsável por buscar dados de várias fontes"""
    
    def carregar_csv(self, caminho_arquivo):
        """
        Tenta carregar dados de um CSV.
        Se não encontrar, retorna DataFrame vazio.
        """
        try:
            if not os.path.exists(caminho_arquivo):
                print(f"   Arquivo {caminho_arquivo} não encontrado")
                return pd.DataFrame()
            
            # Carrega o CSV
            dados = pd.read_csv(caminho_arquivo)
            
            # Verifica se tem as colunas mínimas que preciso
            colunas_necessarias = ['id', 'nome', 'conta']
            for coluna in colunas_necessarias:
                if coluna not in dados.columns:
                    print(f"   AVISO: Coluna '{coluna}' não encontrada no CSV")
            
            return dados
            
        except Exception as e:
            print(f"   Erro ao ler CSV: {e}")
            return pd.DataFrame()
    
    def criar_dados_exemplo(self):
        """
        Cria dados de exemplo quando não tem CSV disponível.
        Isso é útil para testar a pipeline sem depender de arquivos externos.
        """
        print("   Criando dados de exemplo para demonstração...")
        
        # Dados que eu mesmo criei, baseados em clientes fictícios
        dados_exemplo = {
            'id': [101, 102, 103, 104, 105],
            'nome': ['ana clara silva', 'CARLOS ALMEIDA', 'Maria Fernandes Costa', 
                    'joão pedro santos', 'Fernanda Lima Oliveira'],
            'conta': ['12345-X', '67890-Y', '11223-Z', '44556-A', '77889-B'],
            'tipo_conta': ['POUPANÇA', 'CORRENTE', 'SALÁRIO', 'CORRENTE', 'INVESTIMENTO'],
            'ultima_transacao': [1500.00, 320.50, 2800.00, 75.30, 5000.00],
            'data_cadastro': ['2022-03-15', '2023-01-20', '2021-11-30', 
                             '2023-08-10', '2020-05-25']
        }
        
        return pd.DataFrame(dados_exemplo)
    
    def validar_dados(self, dataframe):
        """Faz uma validação básica dos dados"""
        if dataframe.empty:
            return False, "DataFrame vazio"
        
        # Verifica se tem dados duplicados
        duplicados = dataframe.duplicated(subset=['id']).sum()
        if duplicados > 0:
            print(f"   AVISO: {duplicados} IDs duplicados encontrados")
        
        # Verifica valores nulos
        nulos = dataframe.isnull().sum().sum()
        if nulos > 0:
            print(f"   AVISO: {nulos} valores nulos encontrados")
        
        return True, "Dados validados"