import hashlib
import json
from decimal import Decimal

def updateDados(session, usuario):
    print(
        "Dados atuais:",
        f"\n    --------------------------------",
        f"\n    Nome: {usuario.nome}",
        f"\n    CPF: {usuario.cpf}",
        f"\n    Email: {usuario.email}",
        f"\n    Telefone: {usuario.telefone}",
    )

    print("\nNovos dados (deixe vazio para manter o atual):")
    print("    --------------------------------")
    nome = input("    Nome: ").strip()
    cpf = input("    CPF: ").strip()
    email = input("    Email: ").strip()
    senha = input("    Senha: ").strip()
    telefone = input("    Telefone: ").strip()

    # Prepara os dados, mantendo os antigos se o usuário deixar vazio
    dados = {
        "nome": nome if nome else usuario.nome,
        "cpf": cpf if cpf else usuario.cpf,
        "email": email if email else usuario.email,
        "senha": (
            hashlib.sha256(senha.encode("utf-8")).hexdigest()
            if senha
            else usuario.senha
        ),
        "telefone": telefone if telefone else usuario.telefone,
    }

    # Executa o UPDATE no Cassandra
    try:
        session.execute(
            """
            UPDATE cliente
            SET nome = %s, cpf = %s, email = %s, senha = %s, telefone = %s
            WHERE id = %s
            """,
            (
                dados["nome"],
                dados["cpf"],
                dados["email"],
                dados["senha"],
                dados["telefone"],
                usuario.id,
            ),
        )
        print("\nDados atualizados com sucesso.")
    except Exception as e:
        print(f"\nErro ao atualizar: {e}")


def updateEndereco(session, tabela, id, enderecos):
    enderecos = dict(enderecos or {})
    numero_enderecos = len(enderecos)
    if numero_enderecos == 0:
        print("Nenhum endereço cadastrado.")
        return

    pares = list(enderecos.items())
    print("Endereço atual:" if numero_enderecos == 1 else "Endereços atuais:")
    for idx, (k, v) in enumerate(pares, 1):
        e = json.loads(v)
        if numero_enderecos != 1:
            print(f"\n  ID: {idx}")
        print(
            f"\n    Logradouro: {e.get('logradouro','')}",
            f"\n    Número: {e.get('numero','')}",
            f"\n    Complemento: {e.get('complemento','')}",
            f"\n    Bairro: {e.get('bairro','')}",
            f"\n    Cidade: {e.get('cidade','')}",
            f"\n    Estado: {e.get('estado','')}",
        )
        if numero_enderecos != 1:
            print(f"  --------------------------------")

    # Seleção do endereço a editar
    if numero_enderecos != 1:
        while True:
            sel = input(
                "\nDigite o ID do endereço que deseja atualizar (0 para cancelar): "
            ).strip()
            if sel == "0":
                print("Operação cancelada.")
                return
            if sel.isdigit() and 1 <= int(sel) <= len(pares):
                i = int(sel) - 1
                break
            print("ID inválido.")
    else:
        if input("\nDeseja atualizar esse endereço? (s/n): ").strip().lower() != "s":
            print("Operação cancelada.")
            return
        i = 0

    k, atual_json = pares[i]
    atual = json.loads(atual_json)
    
    print("\nNovos dados (digite '-' para manter o atual):")
    print("--------------------------------")
    logradouro = input("Logradouro: ").strip()
    numero = input("Número: ").strip()
    complemento = input("Complemento: ").strip()
    bairro = input("Bairro: ").strip()
    cidade = input("Cidade: ").strip()
    estado = input("Estado: ").strip()

    atualizado = {
        "id": atual.get("id"),
        "logradouro": logradouro if logradouro != "-" else atual.get("logradouro", ""),
        "numero": numero if numero != "-" else atual.get("numero", ""),
        "complemento": (complemento if complemento != "-" else atual.get("complemento", "")),
        "bairro": bairro if bairro != "-" else atual.get("bairro", ""),
        "cidade": cidade if cidade != "-" else atual.get("cidade", ""),
        "estado": estado if estado != "-" else atual.get("estado", ""),
    }

    # Atualiza o Map no Cassandra
    session.execute(
        f"""
        UPDATE {tabela}
        SET enderecos[%s] = %s
        WHERE id = %s;
        """,
        (k, json.dumps(atualizado, ensure_ascii=False), id),
    )
    print("\nEndereço atualizado com sucesso.")
    return atualizado


def updateProduto(session, id, produtos):
    # Esta função é usada se quiseres editar produtos do vendedor
    produtos = dict(produtos or {})
    numero_produtos = len(produtos)
    if numero_produtos == 0:
        print("Nenhum produto cadastrado.")
        return

    pares = list(produtos.items())
    print("Produto atual:" if numero_produtos == 1 else "Produtos atuais:")
    for i, (k, v) in enumerate(pares, 1):
        p = json.loads(v)
        if numero_produtos != 1:
            print(f"\n  ID: {i}")
        print(
            f"\n    Nome: {p.get('nome','')}",
            f"\n    Descrição: {p.get('descricao','')}",
            f"\n    Preço: R${float(p.get('preco','0')):.2f}",
            f"\n    Estoque: {int(p.get('estoque',0))}",
        )
        if numero_produtos != 1:
            print(f"  --------------------------------")

    if numero_produtos != 1:
        while True:
            sel = input(
                "\nDigite o ID do produto que deseja atualizar (0 para cancelar): "
            ).strip()
            if sel == "0":
                print("Operação cancelada.")
                return
            if sel.isdigit() and 1 <= int(sel) <= len(pares):
                idx = int(sel) - 1
                break
            print("ID inválido.")
    else:
        if input("\nDeseja atualizar esse produto? (s/n): ").strip().lower() != "s":
            print("Operação cancelada.")
            return
        idx = 0

    print("\nNovos dados (deixe vazio para manter o atual):")
    print("--------------------------------")
    nome = input("Nome: ").strip()
    descricao = input("Descrição: ").strip()
    preco = input("Preço: R$").strip()
    estoque = input("Estoque: ").strip()

    k, atual_json = pares[idx]
    p_atual = json.loads(atual_json)

    atualizado = {
        "id": p_atual.get("id"),
        "nome": nome if nome else p_atual.get("nome", ""),
        "descricao": descricao if descricao else p_atual.get("descricao", ""),
        "preco": str(Decimal(preco)) if preco else str(p_atual.get("preco", "0")),
        "estoque": int(estoque) if estoque else int(p_atual.get("estoque", 0)),
    }

    session.execute(
        """
        UPDATE vendedor
        SET produtos[%s] = %s
        WHERE id = %s;
        """,
        (k, json.dumps(atualizado, ensure_ascii=False), id),
    )
    print("\nProduto atualizado com sucesso.")
    return atualizado