import os
import pandas as pd
import mysql.connector
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def carregar_clientes_mysql(cursor):
    """Carrega dados de clientes do CSV para MySQL"""
    try:
        df = pd.read_csv("etl/clientes.csv")
        print("📋 Dados de clientes encontrados:")
        print(df.head())
        
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO Clientes (nome, email, telefone)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    nome = VALUES(nome),
                    telefone = VALUES(telefone)
            """, (row['nome'], row['email'], row['telefone']))
        print(f"✅ {len(df)} clientes carregados/atualizados")
        return True
    except Exception as e:
        print(f"❌ Erro ao carregar clientes: {e}")
        return False

def carregar_produtos_vendidos_mongo():
    """Carrega relatório de produtos mais vendidos para MongoDB"""
    try:
        df = pd.read_csv("etl/csv_produtos_mais_vendidos.csv")
        print("\n📋 Dados de produtos mais vendidos:")
        print(df.head())
        
        # Converter para formato de documentos MongoDB
        docs = []
        for _, row in df.iterrows():
            doc = {
                "tipo_relatorio": "produtos_mais_vendidos",
                "data_geracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "produto": row['produto'],
                "quantidade_vendida": int(row['quantidade_vendida']),
                "categoria": "Geral"  # Valor padrão
            }
            docs.append(doc)
        
        client = MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=5000)
        db = client["loja_roupas"]
        result = db["relatorios"].insert_many(docs)
        print(f"✅ {len(result.inserted_ids)} produtos mais vendidos carregados")
        return True
    except Exception as e:
        print(f"❌ Erro ao carregar produtos mais vendidos: {e}")
        return False

def carregar_receita_mensal_mongo():
    """Carrega relatório de receita mensal para MongoDB"""
    try:
        df = pd.read_csv("etl/csv_receita_mensal.csv")
        print("\n📋 Dados de receita mensal:")
        print(df.head())
        
        doc = {
            "tipo_relatorio": "receita_mensal",
            "data_geracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dados": df.to_dict('records')
        }
        
        client = MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=5000)
        db = client["loja_roupas"]
        result = db["relatorios"].insert_one(doc)
        print(f"✅ Receita mensal carregada (ID: {result.inserted_id})")
        return True
    except Exception as e:
        print(f"❌ Erro ao carregar receita mensal: {e}")
        return False

def carregar_satisfacao_clientes_mongo():
    """Carrega dados de satisfação para MongoDB - Versão ajustada para seus CSVs"""
    try:
        df = pd.read_csv("etl/csv_satisfacao_clientes.csv")
        print("\n📋 Dados de satisfação encontrados:")
        print(df.head())
        
        # Análise de sentimentos baseada na nota
        df['sentimento'] = df['nota_satisfacao'].apply(
            lambda nota: 'positivo' if nota >= 4 else ('neutro' if nota == 3 else 'negativo')
        )
        
        stats = {
            "tipo_relatorio": "satisfacao_clientes",
            "data_geracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_avaliacoes": len(df),
            "media_avaliacoes": round(df['nota_satisfacao'].mean(), 2),
            "percentual_positivo": round(len(df[df['sentimento'] == 'positivo']) / len(df) * 100, 2),
            "avaliacoes": df.to_dict('records')
        }
        
        client = MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=5000)
        db = client["loja_roupas"]
        result = db["pesquisas_satisfacao"].insert_one(stats)
        
        print(f"✅ {len(df)} avaliações processadas. Média: {stats['media_avaliacoes']}/5")
        print(f"   ID do documento: {result.inserted_id}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao processar satisfação: {str(e)}")
        return False

def testar_conexao_mongo():
    """Testa a conexão com o MongoDB"""
    try:
        client = MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=5000)
        client.server_info()  # Força uma operação no servidor
        print("🟢 Conexão com MongoDB estabelecida com sucesso")
        return True
    except Exception as e:
        print(f"🔴 Falha na conexão com MongoDB: {e}")
        return False

def main():
    print("\n" + "="*50)
    print(" INICIANDO PROCESSO ETL ".center(50, "="))
    print("="*50 + "\n")
    
    # Testar conexões primeiro
    if not testar_conexao_mongo():
        return

    mysql_conn = None
    cursor = None
    
    try:
        # Conexão com MySQL
        mysql_conn = mysql.connector.connect(
            host="mysql",
            user="root",
            password=os.getenv("MYSQL_ROOT_PASSWORD"),
            database="loja_roupas"
        )
        cursor = mysql_conn.cursor()
        print("🟢 Conexão com MySQL estabelecida com sucesso")

        # Processar dados
        resultados = {
            "clientes": carregar_clientes_mysql(cursor),
            "produtos": carregar_produtos_vendidos_mongo(),
            "receita": carregar_receita_mensal_mongo(),
            "satisfacao": carregar_satisfacao_clientes_mongo()
        }

        if mysql_conn:
            mysql_conn.commit()

        print("\n" + "="*50)
        print(" RESUMO DA EXECUÇÃO ".center(50, "="))
        for etapa, sucesso in resultados.items():
            print(f"{etapa.upper():<15} {'✅' if sucesso else '❌'}")
        print("="*50 + "\n")

    except Exception as e:
        print(f"\n🔴 ERRO GRAVE: {e}")
    finally:
        if cursor:
            cursor.close()
        if mysql_conn:
            mysql_conn.close()
        print("Conexões com bancos de dados fechadas")

if __name__ == "__main__":
    main()