import sys
import os

# --- CORREÇÃO DO ERRO DE CONEXÃO (PYTHON 3.12+) ---
try:
    # O pacote 'pyasyncore' instala-se com o nome 'asyncore'
    import asyncore
except ImportError:
    # Se der erro, é porque não está instalado mesmo
    print("ERRO CRÍTICO: A biblioteca de correção não foi encontrada.")
    print("Por favor, execute no terminal: pip install pyasyncore")
    sys.exit(1)
# --------------------------------------------------

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import config  # Importa suas senhas

# Importa as funções dos outros arquivos
from cadastrar import (
    insertCliente,
    insertVendedor,
    insertCompra,
    insertProduto,
    insertFavorito,
    insertEndereco,
)
from atualizar import updateDados, updateEndereco, updateProduto
from deletar import deleteEndereco, deleteFavorito, deleteCompra
from listar import findProdutos

def get_connection():
    """Configura a conexão segura com o Astra DB"""
    cloud_config = {"secure_connect_bundle": config.BUNDLE_PATH}
    auth_provider = PlainTextAuthProvider(config.CLIENT_ID, config.CLIENT_SECRET)
    
    # protocol_version=4 ajuda a evitar erros de timeout na conexão inicial
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session = cluster.connect()
    session.set_keyspace(config.KEYSPACE)
    return cluster, session

def create_schema(session):
    """Cria as tabelas iguais ao repositório original"""
    print("Verificando tabelas...")
    
    # Tabela Cliente (Usa Map para endereços, favoritos e compras)
    session.execute("""
        CREATE TABLE IF NOT EXISTS cliente (
            id uuid PRIMARY KEY,
            nome text, 
            cpf text, 
            telefone text, 
            email text, 
            senha text,
            enderecos map<text, text>,     
            favoritos map<text, text>,     
            compras map<text, text>        
        );
    """)

    # Tabela Vendedor
    session.execute("""
        CREATE TABLE IF NOT EXISTS vendedor (
            id uuid PRIMARY KEY,
            nome text, 
            cpf text, 
            telefone text, 
            email text, 
            senha text,
            enderecos map<text, text>,
            produtos map<text, text>       
        );
    """)

    # Tabela Compra
    session.execute("""
        CREATE TABLE IF NOT EXISTS compra (
            id uuid PRIMARY KEY,
            cliente_id uuid,
            produtos map<text, text>,
            total decimal,
            data timestamp
        );
    """)
    
    # Índices para busca (IMPORTANTE para as funções de busca funcionarem)
    session.execute("CREATE INDEX IF NOT EXISTS ON cliente (cpf);")
    session.execute("CREATE INDEX IF NOT EXISTS ON cliente (email);")
    session.execute("CREATE INDEX IF NOT EXISTS ON vendedor (cpf);")
    session.execute("CREATE INDEX IF NOT EXISTS ON vendedor (email);")
    
    print("Schema verificado com sucesso!")

def limpar():
    os.system("cls" if os.name == "nt" else "clear")

def escolher_cliente(session):
    clientes = list(session.execute("SELECT * FROM cliente;"))
    if not clientes:
        print("Nenhum cliente cadastrado.")
        return None
    for index, cliente in enumerate(clientes, 1):
        print(f"{index} | {cliente.nome} | {cliente.email}")
    id_sel = input("\nSelecione o cliente: ").strip()
    if not id_sel.isdigit() or not (1 <= int(id_sel) <= len(clientes)):
        print("Seleção inválida.")
        return None
    return clientes[int(id_sel) - 1]

def escolher_vendedor(session):
    vendedores = list(session.execute("SELECT * FROM vendedor;"))
    if not vendedores:
        print("Nenhum vendedor cadastrado.")
        return None
    for index, vendedor in enumerate(vendedores, 1):
        print(f"{index} | {vendedor.nome} | {vendedor.email}")
    id_sel = input("\nSelecione o vendedor: ").strip()
    if not id_sel.isdigit() or not (1 <= int(id_sel) <= len(vendedores)):
        print("Seleção inválida.")
        return None
    return vendedores[int(id_sel) - 1]

# --- PROGRAMA PRINCIPAL ---
if __name__ == "__main__":
    cluster = None
    try:
        print("Conectando ao banco...")
        cluster, session = get_connection()
        create_schema(session)
        limpar()

        while True:
            print("""
      ➕ INSERT
      ------------------------------------------------------------
      |  1  | 👤 Cliente        |  2  | 🧑‍💼 Vendedor
      |  3  | 📦 Produto        |  4  | 🛒 Compra

      ✏️ EDITAR
      ------------------------------------------------------------
      |  5  | 👤 Cliente (Dados / Endereços / Favoritos)

      🔍 SEARCH
      ------------------------------------------------------------
      |  6  | 📦 Produto

      🗑️ DELETE
      ------------------------------------------------------------
      |  7  | 🧾 Compra
      ============================================================
      🔚 SISTEMA
      ------------------------------------------------------------
      | 0  | 🚪 Sair
      ============================================================
            """)
            opcao = input("Digite a opção desejada: ").strip()
            limpar()

            if opcao == "1":
                print("CADASTRAR CLIENTE")
                insertCliente(session)
            elif opcao == "2":
                print("CADASTRAR VENDEDOR")
                insertVendedor(session)
            elif opcao == "3":
                print("CADASTRAR PRODUTO")
                vendedor = escolher_vendedor(session)
                if vendedor:
                    insertProduto(session, vendedor.id)
            elif opcao == "4":
                print("CADASTRAR COMPRA")
                cliente = escolher_cliente(session)
                if not cliente:
                    continue
                # A função findProdutos precisa retornar algo para insertCompra funcionar
                produtos = findProdutos(session, retorno="compra") or []
                if produtos:
                    insertCompra(session, cliente.id, produtos)
            elif opcao == "5":
                print("ATUALIZAR CLIENTE")
                cliente = escolher_cliente(session)
                if not cliente:
                    continue
                limpar()
                while True:
                    print("""
      1 | Atualizar Dados Pessoais
      2 | Adicionar Endereços
      3 | Favoritar Produtos
      4 | Deletar Endereços
      5 | Deletar Favoritos
      0 | Voltar
                    """)
                    sub = input("Digite a opção desejada: ").strip()
                    limpar()
                    if sub == "0":
                        break
                    elif sub == "1":
                        updateDados(session, cliente)
                        break
                    elif sub == "2":
                        insertEndereco(session, "cliente", cliente.id)
                        break
                    elif sub == "3":
                        produtos = findProdutos(session, retorno="lista") or []
                        insertFavorito(session, cliente.id, produtos)
                        break
                    elif sub == "4":
                        deleteEndereco(session, "cliente", cliente.id, dict(cliente.enderecos or {}))
                        break
                    elif sub == "5":
                        deleteFavorito(session, cliente.id, dict(cliente.favoritos or {}))
                        break
            elif opcao == "6":
                print("BUSCAR PRODUTOS")
                findProdutos(session, retorno="lista")
            elif opcao == "7":
                print("DELETAR COMPRA")
                cliente = escolher_cliente(session)
                if not cliente:
                    continue
                deleteCompra(session, cliente.id, dict(cliente.compras or {}))
            elif opcao == "0":
                print("Saindo do sistema...")
                break
            else:
                limpar()

    except Exception as e:
        print(f"ERRO FATAL: {e}")
    finally:
        if cluster:
            cluster.shutdown()