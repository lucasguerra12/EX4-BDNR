import json
import uuid
from decimal import Decimal

def _to_decimal(x):
    return x if isinstance(x, Decimal) else Decimal(str(x))

def findProdutos(session, retorno):
    rows = session.execute("SELECT id, nome, produtos FROM vendedor;")
    dados_produtos = []
    
    for vend in rows:
        produtos_map = dict(vend.produtos or {})
        for _, prod_json in produtos_map.items():
            prod = json.loads(prod_json)
            dados_produtos.append({
                "id": uuid.UUID(prod["id"]),
                "nome": prod.get("nome", ""),
                "descricao": prod.get("descricao", ""),
                "preco": _to_decimal(prod.get("preco", "0")),
                "estoque": int(prod.get("estoque", 0)),
                "vendedor_id": vend.id
            })

    if not dados_produtos:
        print("Nenhum produto encontrado.")
        return []

    print("\n--- PRODUTOS ---")
    for i, p in enumerate(dados_produtos, 1):
        print(f"{i}. {p['nome']} | R${p['preco']} | Est: {p['estoque']}")
        print(f"   Desc: {p['descricao']}")
        print("---")
        
    return dados_produtos