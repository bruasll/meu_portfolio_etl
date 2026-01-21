#!/usr/bin/env python3
"""
Pipeline ETL - Santander Dev Week
Autor: [Seu Nome Aqui]
Data: 15/01/2024

Este é o código principal que orquestra todo o processo ETL.
Fiz modular para ser fácil de entender e manter.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extrator import ExtratorDados
from src.transformador import TransformadorDados
from src.carregador import CarregadorDados

def main():
    print("=" * 60)
    print("INICIANDO MINHA PIPELINE ETL")
    print("=" * 60)
    
    try:
        # 1. EXTRAÇÃO - Pegar os dados de algum lugar
        print("\n🔍 Passo 1: Extraindo dados...")
        extrator = ExtratorDados()
        dados_brutos = extrator.carregar_csv('dados/clientes.csv')
        
        if dados_brutos.empty:
            print("AVISO: CSV vazio ou não encontrado, usando dados de exemplo...")
            dados_brutos = extrator.criar_dados_exemplo()
        
        print(f"   ✅ {len(dados_brutos)} registros carregados")
        
        # Mostra como os dados chegaram
        print("\n   Primeiras linhas dos dados brutos:")
        print("   " + "-" * 40)
        for i, row in dados_brutos.head(3).iterrows():
            print(f"   ID: {row['id']} | Nome: {row['nome']} | Conta: {row['conta']}")
        
        # 2. TRANSFORMAÇÃO - Limpar e processar
        print("\n🔄 Passo 2: Transformando dados...")
        transformador = TransformadorDados()
        
        # Aplicando as transformações uma por uma
        print("   • Padronizando nomes...")
        dados_limpos = transformador.padronizar_nomes(dados_brutos)
        
        print("   • Gerando mensagens personalizadas...")
        dados_com_mensagens = transformador.gerar_mensagens(dados_limpos)
        
        print("   • Criando resumos dos clientes...")
        dados_finais = transformador.criar_resumos(dados_com_mensagens)
        
        print(f"   ✅ Dados transformados: {len(dados_finais)} registros processados")
        
        # 3. CARREGAMENTO - Salvar resultados
        print("\n💾 Passo 3: Carregando dados processados...")
        carregador = CarregadorDados()
        
        # Salvando em CSV
        arquivo_csv = 'outputs/clientes_processados.csv'
        carregador.salvar_csv(dados_finais, arquivo_csv)
        print(f"   ✅ CSV salvo: {arquivo_csv}")
        
        # Salvando em JSON também
        arquivo_json = 'outputs/clientes_processados.json'
        carregador.salvar_json(dados_finais, arquivo_json)
        print(f"   ✅ JSON salvo: {arquivo_json}")
        
        # Gerando um relatório simples
        carregador.gerar_relatorio(dados_finais)
        
        print("\n" + "=" * 60)
        print("🎉 PIPELINE CONCLUÍDA COM SUCESSO!")
        print("=" * 60)
        
        # Mostra um preview do resultado
        print("\n📋 PREVIEW DO RESULTADO:")
        print("-" * 60)
        colunas_para_mostrar = ['id', 'nome', 'conta', 'mensagem_personalizada']
        for i, row in dados_finais.head(3).iterrows():
            print(f"\nCliente {row['id']}: {row['nome']}")
            print(f"Conta: {row['conta']}")
            print(f"Mensagem: {row['mensagem_personalizada'][:80]}...")
            print("-" * 40)
        
        print(f"\n📊 Total de clientes processados: {len(dados_finais)}")
        print("📍 Verifique a pasta 'outputs/' para os arquivos completos")
        
    except Exception as e:
        print(f"\n❌ OPS! Algo deu errado: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())