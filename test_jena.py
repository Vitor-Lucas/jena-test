"""
Script de teste para TurtleLoader e SparqlQuery
Execute após subir o Fuseki com docker-compose up -d
"""

from TurtleLoader import TurtleLoader
from SparqlQuery import SparqlQuery
import json


def print_result(result, title="Resultado"):
    """Imprime o resultado de forma detalhada"""
    print(f"\n{'=' * 60}")
    print(f"{title}")
    print('=' * 60)

    if result.get('success'):
        print("✓ SUCESSO")
        print(f"Mensagem: {result.get('message', 'N/A')}")
    else:
        print("✗ ERRO")
        print(f"Mensagem: {result.get('message', 'N/A')}")

        # Imprimir detalhes do erro
        if 'error' in result:
            print(f"\nDetalhes do erro:")
            print(result['error'])

        if 'traceback' in result:
            print(f"\nTraceback completo:")
            print(result['traceback'])

        if 'status_code' in result:
            print(f"\nHTTP Status Code: {result['status_code']}")

    # Imprimir dados adicionais
    if 'count' in result:
        print(f"Total de resultados: {result['count']}")

    if 'results' in result and result.get('success'):
        print(f"\nResultados:")
        for i, row in enumerate(result['results'][:5], 1):  # Mostra até 5 resultados
            print(f"  {i}. {json.dumps(row, indent=4, ensure_ascii=False)}")
        if len(result['results']) > 5:
            print(f"  ... e mais {len(result['results']) - 5} resultados")


def main():
    print("=" * 60)
    print("TESTE: Apache Jena Fuseki com Python")
    print("=" * 60)

    # Verificar conexão básica
    print("\n0. Verificando conexão com Fuseki...")
    try:
        import requests
        response = requests.get("http://localhost:3030", timeout=5)
        print(f"   ✓ Fuseki está respondendo (Status: {response.status_code})")
    except requests.exceptions.ConnectionError:
        print("   ✗ ERRO: Não foi possível conectar ao Fuseki!")
        print("   Verifique se o Docker está rodando: docker-compose up -d")
        return
    except Exception as e:
        print(f"   ✗ ERRO inesperado ao verificar conexão: {e}")
        return

    # Inicializa as classes
    loader = TurtleLoader(fuseki_url="http://localhost:3030", dataset="ds")
    sparql = SparqlQuery(fuseki_url="http://localhost:3030", dataset="ds")

    # ========================================
    # 1. LIMPAR DATASET (começar do zero)
    # ========================================
    print("\n1. Limpando dataset...")
    result = loader.clear_dataset()
    print_result(result, "1. Resultado da Limpeza")

    if not result['success']:
        print("\n⚠️  ATENÇÃO: Falha ao limpar dataset. Verifique se o dataset 'ds' existe no Fuseki.")
        print("   Acesse http://localhost:3030 e crie um dataset chamado 'ds' se necessário.")
        return

    # ========================================
    # 2. CARREGAR DADOS TURTLE
    # ========================================
    print("\n2. Carregando dados Turtle...")

    ttl_data = """
    @prefix ex: <http://example.org/> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    
    ex:Pessoa rdf:type rdfs:Class ;
              rdfs:label "Pessoa" .
    
    ex:Empresa rdf:type rdfs:Class ;
               rdfs:label "Empresa" .
    
    ex:pessoa1 rdf:type ex:Pessoa ;
               ex:nome "João Silva" ;
               ex:idade 30 ;
               ex:email "joao@example.com" ;
               ex:trabalhaEm ex:empresa1 .
    
    ex:pessoa2 rdf:type ex:Pessoa ;
               ex:nome "Maria Santos" ;
               ex:idade 25 ;
               ex:email "maria@example.com" ;
               ex:trabalhaEm ex:empresa1 .
    
    ex:pessoa3 rdf:type ex:Pessoa ;
               ex:nome "Carlos Oliveira" ;
               ex:idade 35 ;
               ex:email "carlos@example.com" ;
               ex:trabalhaEm ex:empresa2 .
    
    ex:empresa1 rdf:type ex:Empresa ;
                ex:nome "Tech Solutions" ;
                ex:cnpj "12.345.678/0001-99" .
    
    ex:empresa2 rdf:type ex:Empresa ;
                ex:nome "Data Corp" ;
                ex:cnpj "98.765.432/0001-11" .
    """

    result = loader.load_from_string(ttl_data)
    print_result(result, "2. Resultado do Carregamento")

    if not result['success']:
        print("\n⚠️  Parando testes devido a erro no carregamento.")
        return

    # ========================================
    # 3. CONSULTA SELECT - Listar todas as pessoas
    # ========================================
    print("\n3. Consultando todas as pessoas...")

    query = """
    PREFIX ex: <http://example.org/>
    
    SELECT ?pessoa ?nome ?idade ?email
    WHERE {
        ?pessoa a ex:Pessoa ;
                ex:nome ?nome ;
                ex:idade ?idade ;
                ex:email ?email .
    }
    ORDER BY ?nome
    """

    result = sparql.select(query)
    print_result(result, "3. Resultado da Consulta SELECT")

    # ========================================
    # 4. CONSULTA SELECT - Pessoas e suas empresas
    # ========================================
    print("\n4. Consultando pessoas e suas empresas...")

    query = """
    PREFIX ex: <http://example.org/>
    
    SELECT ?nomePessoa ?nomeEmpresa ?cnpj
    WHERE {
        ?pessoa a ex:Pessoa ;
                ex:nome ?nomePessoa ;
                ex:trabalhaEm ?empresa .
        
        ?empresa ex:nome ?nomeEmpresa ;
                 ex:cnpj ?cnpj .
    }
    ORDER BY ?nomeEmpresa ?nomePessoa
    """

    result = sparql.select(query)
    print_result(result, "4. Resultado Pessoas x Empresas")

    # ========================================
    # 5. CONSULTA ASK - Verificar existência
    # ========================================
    print("\n5. Verificando se João Silva existe...")

    query = """
    PREFIX ex: <http://example.org/>
    
    ASK {
        ?pessoa ex:nome "João Silva" .
    }
    """

    result = sparql.ask(query)
    print_result(result, "5. Resultado da Consulta ASK")

    # ========================================
    # 6. UPDATE - Inserir nova pessoa
    # ========================================
    print("\n6. Inserindo nova pessoa via SPARQL UPDATE...")

    update_query = """
    PREFIX ex: <http://example.org/>
    
    INSERT DATA {
        ex:pessoa4 a ex:Pessoa ;
                   ex:nome "Ana Costa" ;
                   ex:idade 28 ;
                   ex:email "ana@example.com" ;
                   ex:trabalhaEm ex:empresa2 .
    }
    """

    result = sparql.update(update_query)
    print_result(result, "6. Resultado do INSERT")

    # ========================================
    # 7. CONSULTA SELECT - Contar pessoas
    # ========================================
    print("\n7. Contando total de pessoas após inserção...")

    query = """
    PREFIX ex: <http://example.org/>
    
    SELECT (COUNT(?pessoa) as ?total)
    WHERE {
        ?pessoa a ex:Pessoa .
    }
    """

    result = sparql.select(query)
    print_result(result, "7. Resultado da Contagem")

    # ========================================
    # 8. CONSULTA SELECT - Filtro por idade
    # ========================================
    print("\n8. Consultando pessoas com idade >= 30...")

    query = """
    PREFIX ex: <http://example.org/>
    
    SELECT ?nome ?idade
    WHERE {
        ?pessoa a ex:Pessoa ;
                ex:nome ?nome ;
                ex:idade ?idade .
        FILTER (?idade >= 30)
    }
    ORDER BY DESC(?idade)
    """

    result = sparql.select(query)
    print_result(result, "8. Resultado do Filtro por Idade")

    print("\n" + "=" * 60)
    print("TESTES CONCLUÍDOS!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("\n" + "=" * 60)
        print("ERRO FATAL NO SCRIPT")
        print("=" * 60)
        print(f"Erro: {e}")
        print(f"\nTraceback completo:")
        print(traceback.format_exc())