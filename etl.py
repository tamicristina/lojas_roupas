import os
import pandas as pd
import mysql.connector
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
print("Diretório atual:", os.getcwd())
print("Arquivos na pasta:", os.listdir('etl'))


def carregar_clientes_mysql(cursor):
    try:
        caminho_csv = os.path.join('etl', 'clientes.csv')
        print(f"Tentando ler arquivo em: {caminho_csv}")
        
        df = pd.read_csv(caminho_csv, encoding='utf-8-sig')
        print(f"Dados encontrados ({len(df)} linhas):\n", df.head())
        
        for _, row in df.iterrows():
            try:
                cursor.execute(
                    "INSERT INTO Clientes (nome, email, telefone) VALUES (%s, %s, %s)",
                    (row['nome'], row['email'], row['telefone'])
                )
            except Exception as insert_error:
                print(f"Erro ao inserir linha {_}: {insert_error}")
                continue
                
        print(f"✅ {len(df)} clientes carregados com sucesso!")
        return True
        
    except Exception as e:
        print(f"❌ Erro grave no ETL: {str(e)}")
        return False

def carregar_produtos_mysql(cursor):
    try:
        df = pd.read_csv("etl/csv_produtos_mais_vendidos.csv", encoding='utf-8-sig')
        print("\n📋 Dados de produtos encontrados:")
        print(df.head())
        
        produtos_unicos = df['produto'].unique()
        for produto in produtos_unicos:
            cursor.execute(
                "INSERT IGNORE INTO Produtos (nome) VALUES (%s)",
                (produto,)
            )
        
        for _, row in df.iterrows():
            cursor.execute(
                """INSERT INTO Vendas (produto_id, quantidade, data_venda)
                   SELECT id, %s, CURDATE() FROM Produtos WHERE nome = %s""",
                (row['quantidade_vendida'], row['produto'])
            )
        
        print(f"✅ {len(df)} registros de vendas carregados!")
        return True
    except Exception as e:
        print(f"❌ Erro ao carregar produtos/vendas: {e}")
        return False

def carregar_satisfacao_mysql(cursor):
    try:
        df = pd.read_csv("etl/csv_satisfacao_clientes.csv", encoding='utf-8-sig')
        print("\n📋 Dados de satisfação encontrados:")
        print(df.head())
        
        for _, row in df.iterrows():
            cursor.execute(
                """INSERT INTO Satisfacao (produto_id, nota, data_avaliacao)
                   SELECT id, %s, %s FROM Produtos WHERE nome = %s""",
                (row['nota_satisfacao'], row['data'], row['produto'])
            )
        
        print(f"✅ {len(df)} avaliações carregadas!")
        return True
    except Exception as e:
        print(f"❌ Erro ao carregar satisfação: {e}")
        return False    


# ===== Mongo DB ==== 
def carregar_produtos_vendidos_mongo():
    """Carrega relatório de produtos mais vendidos para MongoDB"""
    try:
        df = pd.read_csv("etl/csv_produtos_mais_vendidos.csv")
        print("\n📋 Dados de produtos mais vendidos:")
        print(df.head())
        
        docs = []
        for _, row in df.iterrows():
            doc = {
                "tipo_relatorio": "produtos_mais_vendidos",
                "data_geracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "produto": row['produto'],
                "quantidade_vendida": int(row['quantidade_vendida']),
                "categoria": "Geral"  
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
        client.server_info()  
        print("🟢 Conexão com MongoDB estabelecida com sucesso")
        return True
    except Exception as e:
        print(f"🔴 Falha na conexão com MongoDB: {e}")
        return False

def main():
    print("\n" + "="*50)
    print(" INICIANDO PROCESSO ETL ".center(50, "="))
    print("="*50 + "\n")
    
    if not testar_conexao_mongo():
        return

    mysql_conn = None
    cursor = None
    
    try:
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
            "produtos": carregar_produtos_mysql(cursor),
            "satisfacao": carregar_satisfacao_mysql(cursor),
            "produtos_mongo": carregar_produtos_vendidos_mongo(),
            "receita_mongo": carregar_receita_mensal_mongo(),
            "satisfacao_mongo": carregar_satisfacao_clientes_mongo()
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
        if mysql_conn:
            mysql_conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if mysql_conn:
            mysql_conn.close()
        print("Conexões com bancos de dados fechadas")
        
if __name__ == "__main__":
    main()
    
    