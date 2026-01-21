"""
Módulo para carregamento de dados
Salva os resultados em diferentes formatos
"""

import pandas as pd
import json
import os
from datetime import datetime

class CarregadorDados:
    """Responsável por salvar os dados processados"""
    
    def salvar_csv(self, dados, caminho_arquivo):
        """Salva os dados em formato CSV"""
        
        # Cria o diretório se não existir
        diretorio = os.path.dirname(caminho_arquivo)
        if diretorio and not os.path.exists(diretorio):
            os.makedirs(diretorio)
        
        # Salva o CSV
        try:
            dados.to_csv(caminho_arquivo, index=False, encoding='utf-8-sig')
            return True
        except Exception as e:
            print(f"   Erro ao salvar CSV: {e}")
            return False
    
    def salvar_json(self, dados, caminho_arquivo):
        """Salva os dados em formato JSON"""
        
        diretorio = os.path.dirname(caminho_arquivo)
        if diretorio and not os.path.exists(diretorio):
            os.makedirs(diretorio)
        
        try:
            # Converte para dicionário
            dados_dict = dados.to_dict(orient='records')
            
            # Adiciona metadados
            resultado_completo = {
                'metadata': {
                    'gerado_em': datetime.now().isoformat(),
                    'total_registros': len(dados),
                    'fonte': 'Pipeline ETL - Santander Dev Week'
                },
                'dados': dados_dict
            }
            
            # Salva o JSON
            with open(caminho_arquivo, 'w', encoding='utf-8') as f:
                json.dump(resultado_completo, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            print(f"   Erro ao salvar JSON: {e}")
            return False
    
    def gerar_relatorio(self, dados):
        """Gera um relatório simples em texto"""
        
        relatorio_path = 'outputs/relatorio_processamento.txt'
        
        # Cria o diretório se não existir
        diretorio = os.path.dirname(relatorio_path)
        if diretorio and not os.path.exists(diretorio):
            os.makedirs(diretorio)
        
        try:
            with open(relatorio_path, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write("RELATÓRIO DE PROCESSAMENTO ETL\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"Data do processamento: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write(f"Total de registros processados: {len(dados)}\n\n")
                
                f.write("-" * 60 + "\n")
                f.write("COLUNAS DISPONÍVEIS:\n")
                f.write("-" * 60 + "\n")
                for coluna in dados.columns:
                    f.write(f"  • {coluna}\n")
                
                f.write("\n" + "-" * 60 + "\n")
                f.write("AMOSTRA DOS DADOS (3 primeiros registros):\n")
                f.write("-" * 60 + "\n")
                
                for i in range(min(3, len(dados))):
                    f.write(f"\nRegistro {i+1}:\n")
                    linha = dados.iloc[i]
                    
                    # Mostra algumas colunas importantes
                    if 'nome' in linha:
                        f.write(f"  Nome: {linha['nome']}\n")
                    if 'conta' in linha:
                        f.write(f"  Conta: {linha['conta']}\n")
                    if 'tipo_conta' in linha:
                        f.write(f"  Tipo de conta: {linha['tipo_conta']}\n")
                    if 'mensagem_personalizada' in linha:
                        mensagem = linha['mensagem_personalizada']
                        if len(mensagem) > 100:
                            mensagem = mensagem[:100] + "..."
                        f.write(f"  Mensagem: {mensagem}\n")
                
                f.write("\n" + "=" * 60 + "\n")
                f.write("FIM DO RELATÓRIO\n")
                f.write("=" * 60 + "\n")
            
            print(f"   ✅ Relatório gerado: {relatorio_path}")
            return True
            
        except Exception as e:
            print(f"   Erro ao gerar relatório: {e}")
            return False