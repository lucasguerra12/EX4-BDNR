import hashlib
import datetime
import uuid
import json
from decimal import Decimal
from collections import defaultdict

def _dumps(obj):
    return json.dumps(obj, default=str, ensure_ascii=False)

def insertCliente(session):
    nome = input("Nome: ").strip()
    while True:
        cpf = input("CPF: ").strip()
        # Nota: Create Index foi adicionado no main para isso funcionar
        if session.execute("SELECT id FROM cliente WHERE cpf = %s ALLOW FILTERING;", (cpf,)).one():
            print("CPF já cadastrado.")
            if input("Tentar novamente? (s/n): ").lower() != "s": return
        else: break
            
    while True:
        email = input("Email: ").strip()
        if session.execute("SELECT id FROM cliente WHERE email = %s ALLOW FILTERING;", (email,)).one():
            print("Email já cadastrado.")
            if input("Tentar novamente? (s/n): ").lower() != "s": return
        else: break
            
    senha = hashlib.sha256(input("Senha: ").strip().encode("utf-8")).hexdigest()
    telefone = input("Telefone: ").strip()

    enderecos_map = {}
    while True:
        if input("Adicionar endereço? (s/n): ").lower() != "s": break
        end_id = str(uuid.uuid4())
        enderecos_map[end_id] = _dumps({
            "id": end_id,
            "logradouro": input("   Logradouro: ").strip(),
            "numero": input("   Número: ").strip(),
            "complemento": input("   Complemento: ").strip(),
            "bairro": input("   Bairro: ").strip(),
            "cidade": input("   Cidade: ").strip(),
            "estado": input("   Estado: ").strip(),
        })

    try:
        session.execute(
            """
            INSERT INTO cliente (id, nome, cpf, telefone, email, senha, enderecos, favoritos, compras)
            VALUES (%s, %s, %s, %s, %s, %s, %s, {}, {});
            """,
            (uuid.uuid4(), nome, cpf, telefone, email, senha, enderecos_map),
        )
        print("Cliente cadastrado com sucesso.")
    except Exception as e:
        print(f"Erro ao cadastrar cliente: {e}")

def insertVendedor(session):
    nome = input("Nome: ").strip()
    # Logica simplificada para vendedor
    cpf = input("CPF/CNPJ: ").strip()
    email = input("Email: ").strip()
    senha = hashlib.sha256(input("Senha: ").strip().encode("utf-8")).hexdigest()
    telefone = input("Telefone: ").strip()
    
    enderecos_map = {} # Pode adicionar lógica de endereço aqui se quiser
    produtos_map = {}

    try:
        session.execute(
            """
            INSERT INTO vendedor (id, nome, cpf, telefone, email, senha, enderecos, produtos)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (uuid.uuid4(), nome, cpf, telefone, email, senha, enderecos_map, produtos_map),
        )
        print("Vendedor cadastrado com sucesso.")
    except Exception as e:
        print(f"Erro ao cadastrar vendedor: {e}")

def insertEndereco(session, usuario, id):
    novos = {}
    while True:
        end_id = str(uuid.uuid4())
        dados_json = _dumps({
            "id": end_id,
            "logradouro": input("Logradouro: ").strip(),
            "numero": input("Número: ").strip(),
            "complemento": input("Complemento: ").strip(),
            "bairro": input("Bairro: ").strip(),
            "cidade": input("Cidade: ").strip(),
            "estado": input("Estado: ").strip(),
        })
        if input("Confirmar endereço? (s/n): ").lower() == "s":
            novos[end_id] = dados_json
        if input("\nDeseja adicionar outro endereço? (s/n): ").lower() != "s": break

    if novos:
        session.execute(f"UPDATE {usuario} SET enderecos = enderecos + %s WHERE id = %s;", (novos, id))
        print("Endereço(s) cadastrado(s).")

def insertProduto(session, id):
    novos = {}
    while True:
        prod_id = str(uuid.uuid4())
        dados_json = _dumps({
            "id": prod_id,
            "nome": input("Nome do Produto: ").strip(),
            "descricao": input("Descrição: ").strip(),
            "preco": str(Decimal(input("Preço: R$").strip())),
            "estoque": int(input("Estoque: ").strip()),
        })
        if input("Confirmar produto? (s/n): ").lower() == "s":
            novos[prod_id] = dados_json
        if input("Outro produto? (s/n): ").lower() != "s": break

    if novos:
        session.execute("UPDATE vendedor SET produtos = produtos + %s WHERE id = %s;", (novos, id))
        print("Produtos adicionados.")

def insertCompra(session, id, produtos_disponiveis):
    itens = {}
    total = Decimal(0)
    print("\nDigite o ID do produto e a quantidade desejada. Digite '0' para cancelar.")
    
    # 1. Seleção dos Itens
    while True:
        produto_id = input("ID do Produto (índice da lista): ").strip()
        if produto_id == "0":
            if not itens:
                print("Operação cancelada.")
                return
            break # Sai do loop de adicionar itens
            
        if not produto_id.isdigit():
            print("ID inválido.")
            continue
            
        idx = int(produto_id) - 1
        if not (0 <= idx < len(produtos_disponiveis)):
            print("ID não encontrado na lista.")
            continue

        p = produtos_disponiveis[idx]
        print(f"Selecionado: {p['nome']} | Preço: R${p['preco']} | Estoque Atual: {p['estoque']}")
        
        quantidade_str = input("Quantidade: ").strip()
        if not quantidade_str.isdigit() or int(quantidade_str) <= 0:
            print("Quantidade inválida.")
            continue
        
        quantidade = int(quantidade_str)

        if p["estoque"] < quantidade:
            print(f"Estoque insuficiente (Disponível: {p['estoque']}).")
            continue

        # Adiciona ao carrinho temporário
        item_id = str(uuid.uuid4())
        itens[item_id] = _dumps(
            {
                "produto_id": str(p["id"]),
                "vendedor_id": str(p["vendedor_id"]),
                "vendedor_nome": p.get("vendedor_nome", "Desconhecido"), # Garante que não quebra se faltar
                "nome": p["nome"],
                "preco": str(p["preco"]),
                "quantidade": quantidade,
            }
        )
        total += p["preco"] * quantidade
        print(f"Item adicionado! Subtotal: R${total:.2f}")

        if input("Deseja adicionar outro produto? (s/n): ").lower() != "s":
            break

    if not itens:
        return

    # 2. Registrar a Compra (Tabelas Compra e Cliente)
    compra_id = uuid.uuid4()
    dados_compra = {
        "id": compra_id,
        "cliente_id": id,
        "produtos": itens,
        "total": total,
        "data": datetime.datetime.utcnow(),
    }

    try:
        print("\nProcessando compra...")
        # Inserir na tabela Compra
        session.execute(
            """
            INSERT INTO compra (id, cliente_id, produtos, total, data)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (dados_compra["id"], dados_compra["cliente_id"], dados_compra["produtos"], dados_compra["total"], dados_compra["data"]),
        )

        # Atualizar resumo no Cliente
        resumo = {
            "id": str(compra_id),
            "data": dados_compra["data"].isoformat(),
            "total": str(total),
        }
        session.execute(
            "UPDATE cliente SET compras = compras + %s WHERE id = %s;",
            ({str(compra_id): _dumps(resumo)}, id),
        )

        # 3. Atualizar Estoque do Vendedor (A parte crítica)
        print("Atualizando estoques...")
        itens_por_vendedor = defaultdict(list)
        for item_json in itens.values():
            it = json.loads(item_json)
            itens_por_vendedor[it["vendedor_id"]].append(it)

        for vendedor_id_str, itens_v in itens_por_vendedor.items():
            vend_id = uuid.UUID(vendedor_id_str)
            
            # Busca os produtos atuais do vendedor no banco
            row = session.execute("SELECT produtos FROM vendedor WHERE id = %s;", (vend_id,)).one()
            
            if not row or not row.produtos:
                print(f"ERRO: Vendedor {vend_id} não encontrado ou sem produtos.")
                continue
                
            produtos_map = dict(row.produtos) # Mapa <ID_Produto, JSON_Produto>
            
            # Cria índice reverso para achar a chave correta no map
            # (Caso o ID no JSON seja diferente da chave do Map, embora devessem ser iguais)
            index_produto_key = {} 
            for k, v in produtos_map.items():
                pj = json.loads(v)
                index_produto_key[pj.get("id")] = k

            # Atualiza cada item comprado
            for it in itens_v:
                prod_id_buscado = it["produto_id"]
                p_key = index_produto_key.get(prod_id_buscado)
                
                if not p_key:
                    print(f"ERRO: Produto {it['nome']} (ID {prod_id_buscado}) não encontrado no banco do vendedor.")
                    continue
                
                # Ler o JSON atual do banco
                atual_json_str = produtos_map[p_key]
                atual = json.loads(atual_json_str)
                
                estoque_antigo = int(atual.get("estoque", 0))
                qtd_comprada = int(it["quantidade"])
                novo_estoque = max(0, estoque_antigo - qtd_comprada)
                
                atual["estoque"] = novo_estoque
                novo_json_str = _dumps(atual)
                
                # Atualiza no banco
                session.execute(
                    "UPDATE vendedor SET produtos[%s] = %s WHERE id = %s;",
                    (p_key, novo_json_str, vend_id),
                )
                
                # Atualiza também no mapa local para o caso de ter outro item do mesmo produto na lista
                produtos_map[p_key] = novo_json_str
                
                print(f"--> Estoque de '{it['nome']}' atualizado: {estoque_antigo} -> {novo_estoque}")

        print(f"\nCompra registrada com sucesso!\nTotal: R${float(total):.2f}\n")
        return resumo

    except Exception as e:
        print(f"\nFALHA CRÍTICA ao registrar a compra: {e}\n")

def insertFavorito(session, cliente_id, produtos_disponiveis):
    # Lógica simplificada de favoritos
    sel = input("Digite o ID do produto para favoritar (índice): ")
    if sel.isdigit():
        idx = int(sel) - 1
        if 0 <= idx < len(produtos_disponiveis):
            p = produtos_disponiveis[idx]
            fav_id = str(uuid.uuid4())
            fav_data = { "id": fav_id, "nome": p["nome"], "preco": str(p["preco"]) }
            session.execute("UPDATE cliente SET favoritos = favoritos + %s WHERE id = %s;", ({fav_id: _dumps(fav_data)}, cliente_id))
            print("Favoritado!")