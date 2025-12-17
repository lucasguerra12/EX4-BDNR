import json
import uuid

def deleteEndereco(session, tabela, usuario_id, enderecos):
    enderecos = dict(enderecos or {})
    numero_enderecos = len(enderecos)
    if numero_enderecos == 0:
        print("Nenhum endereço cadastrado.")
        return

    pares = list(enderecos.items())
    print("Endereço atual:" if numero_enderecos == 1 else "Endereços atuais:")
    for index, (k, v) in enumerate(pares, 1):
        e = json.loads(v)
        if numero_enderecos != 1:
            print(f"\n  ID: {index}")
        print(
            f"\n    Logradouro: {e.get('logradouro','')}",
            f"\n    Número: {e.get('numero','')}",
            f"\n    Complemento: {e.get('complemento','')}",
            f"\n    Bairro: {e.get('bairro','')}",
            f"\n    Cidade: {e.get('cidade','')}",
            f"\n    Estado: {e.get('estado','')}",
        )
        if numero_enderecos != 1:
            print("  --------------------------------")

    if numero_enderecos != 1:
        while True:
            ids = (
                input(
                    "\nDigite os IDs dos endereços que deseja deletar (0 para cancelar): "
                )
                .strip()
                .split()
            )
            if "0" in ids:
                print("Operação cancelada.")
                return
            if ids and all(x.isdigit() and 1 <= int(x) <= len(pares) for x in ids):
                break
            print("ID inválido")
    else:
        if input("\nDeseja deletar esse endereço? (s/n): ").strip().lower() != "s":
            print("Operação cancelada.")
            return
        ids = ["1"]

    ids_idx = sorted({int(x) - 1 for x in ids})
    deletados = [json.loads(pares[i][1]) for i in ids_idx]
    
    for i in ids_idx:
        k = pares[i][0]
        # Deleta a chave específica dentro do Map do Cassandra
        session.execute(
            f"DELETE enderecos[%s] FROM {tabela} WHERE id = %s;",
            (k, usuario_id),
        )
    print("\nEndereço(s) deletado(s) com sucesso!")
    return deletados


def deleteFavorito(session, cliente_id, favoritos):
    favoritos = dict(favoritos or {})
    numero_favoritos = len(favoritos)
    if numero_favoritos == 0:
        print("Nenhum favorito cadastrado.")
        return

    pares = list(favoritos.items())
    print("Favorito atual:" if numero_favoritos == 1 else "Favoritos atuais:")
    for index, (k, v) in enumerate(pares, 1):
        f = json.loads(v)
        if numero_favoritos != 1:
            print(f"\n  ID: {index}")
        print(
            f"\n    Vendedor: {f.get('vendedor','')}",
            f"\n    Produto: {f.get('produto','')}",
            f"\n    Preço: R${float(f.get('preco','0')):.2f}",
        )
        if numero_favoritos != 1:
            print("  --------------------------------")

    if numero_favoritos != 1:
        while True:
            ids = (
                input(
                    "\nDigite os IDs dos favoritos que deseja deletar (0 para cancelar): "
                )
                .strip()
                .split()
            )
            if "0" in ids:
                print("Operação cancelada.")
                return
            if ids and all(x.isdigit() and 1 <= int(x) <= len(pares) for x in ids):
                break
            print("ID inválido.")
    else:
        if input("\nDeseja deletar esse favorito? (s/n): ").strip().lower() != "s":
            print("Operação cancelada.")
            return
        ids = ["1"]

    ids_idx = sorted({int(x) - 1 for x in ids})
    deletados = [json.loads(pares[i][1]) for i in ids_idx]

    for i in ids_idx:
        k = pares[i][0]
        session.execute(
            "DELETE favoritos[%s] FROM cliente WHERE id = %s;",
            (k, cliente_id),
        )
    print("Favorito(s) deletado(s) com sucesso!")
    return deletados


def deleteCompra(session, cliente_id, compras_resumo):
    compras_resumo = dict(compras_resumo or {})
    numero = len(compras_resumo)
    if numero == 0:
        print("Nenhuma compra vinculada ao cliente.")
        return

    pares = list(compras_resumo.items())
    print("Compras:")
    for index, (k, v) in enumerate(pares, 1):
        c = json.loads(v)
        try:
            total = float(c.get("total", 0) or 0)
        except (ValueError, TypeError):
            total = 0.0
        comp_id = c.get("id", "") or ""
        date = c.get("data", "") or ""
        short_id = comp_id if len(comp_id) <= 12 else comp_id[:12] + "..."
        print(f"{index:>2}. ID: {short_id:<15}  Data: {date:<16}  Total: R${total:,.2f}")

    sel = input(
        "\nDigite o ID da compra que deseja deletar (0 para cancelar): "
    ).strip()
    if sel == "0":
        print("Operação cancelada.")
        return
    if not sel.isdigit() or not (1 <= int(sel) <= len(pares)):
        print("ID inválido.")
        return

    idx = int(sel) - 1
    comp_key, comp_json = pares[idx]
    comp = json.loads(comp_json)

    # Remove da tabela 'compra'
    try:
        session.execute("DELETE FROM compra WHERE id = %s;", (uuid.UUID(comp_key),))
    except Exception:
        # Tenta pegar ID de dentro do JSON se a chave do map não for o ID direto
        try:
            session.execute(
                "DELETE FROM compra WHERE id = %s;", (uuid.UUID(comp.get("id")),)
            )
        except Exception:
            pass

    # Remove do resumo dentro do cliente
    session.execute(
        "DELETE compras[%s] FROM cliente WHERE id = %s;",
        (comp_key, cliente_id),
    )

    print("Compra deletada com sucesso!")
    return True