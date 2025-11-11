import os
import sys

import requests
from typing import Optional
from requests.auth import HTTPBasicAuth

class TurtleLoader:
    """
    Classe para carregar arquivos Turtle (.ttl) no Apache Jena Fuseki
    """

    def __init__(self, fuseki_url: str = "http://localhost:3030", dataset: str = "ds",
                 auth_user: str = "admin", auth_pass: str = "admin123"):
        """
        Inicializa o loader com a URL do Fuseki e o dataset.

        Args:
            fuseki_url: URL base do servidor Fuseki (padrão: http://localhost:3030)
            dataset: Nome do dataset no Fuseki (padrão: ds)
        """
        self.fuseki_url = fuseki_url.rstrip('/')
        self.dataset = dataset
        self.data_endpoint = f"{self.fuseki_url}/{dataset}/data"
        self.auth = HTTPBasicAuth(auth_user, auth_pass) if auth_user and auth_pass else None
        # print(self.auth.username)
        # print(self.auth.password)

    def load_from_directory(self, dir_path: str, graph_uri: Optional[str] = None) -> dict:
        print(f'Arquivos serão carregados pelo diretório {dir_path}')
        for dir, _, file_names in os.walk(dir_path):
            for file_name in file_names:
                file_path = os.path.join(dir, file_name)
                print(f'Arquivo selecionado: {file_path}')
                result = self.load_from_file(file_path=file_path, graph_uri=graph_uri)
                print(result)

    def load_from_file(self, file_path: str, graph_uri: Optional[str] = None) -> dict:
        """
        Carrega um arquivo .ttl no Fuseki.

        Args:
            file_path: Caminho para o arquivo .ttl
            graph_uri: URI do grafo nomeado (opcional, se None usa o grafo padrão)

        Returns:
            dict com status da operação
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                ttl_content = file.read()
                print('Arquivo lido!')
            return self.load_from_string(ttl_content, graph_uri)

        except FileNotFoundError:
            return {
                "success": False,
                "message": f"Arquivo não encontrado: {file_path}"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Erro ao ler arquivo: {str(e)}"
            }

    def load_from_string(self, ttl_content: str, graph_uri: Optional[str] = None) -> dict:
        """
        Carrega conteúdo Turtle (string) no Fuseki.

        Args:
            ttl_content: Conteúdo Turtle como string
            graph_uri: URI do grafo nomeado (opcional)

        Returns:
            dict com status da operação
        """
        print(f'String lida {ttl_content[:300]}')
        headers = {
            'Content-Type': 'text/turtle; charset=utf-8'
        }

        # Se especificar graph_uri, usa named graph
        params = {}
        if graph_uri:
            params['graph'] = graph_uri

        try:
            print('Fazendo a requisição!')
            response = requests.post(
                self.data_endpoint,
                data=ttl_content.encode('utf-8'),
                headers=headers,
                params=params,
                auth=self.auth
            )

            if response.status_code in [200, 201, 204]:
                print('Dados carregados com sucesso!')
                return {
                    "success": True,
                    "message": "Dados carregados com sucesso",
                    "status_code": response.status_code
                }
            else:
                print(f'Código de resposta não positivo. Código: {response.status_code}')
                return {
                    "success": False,
                    "message": f"Erro ao carregar dados: {response.text}",
                    "status_code": response.status_code
                }

        except requests.exceptions.ConnectionError as e:
            print('Não foi possivel conectar ao Fuseki')
            return {
                "success": False,
                "message": "Não foi possível conectar ao Fuseki. Verifique se está rodando.",
                "error": str(e)
            }
        except Exception as e:
            print('Erro inesperado!!')
            import traceback
            return {
                "success": False,
                "message": f"Erro inesperado: {str(e)}",
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    def clear_dataset(self, graph_uri: Optional[str] = None) -> dict:
        """
        Limpa todos os dados do dataset ou de um grafo específico.

        Args:
            graph_uri: URI do grafo para limpar (se None, limpa o grafo padrão)

        Returns:
            dict com status da operação
        """
        params = {}
        if graph_uri:
            params['graph'] = graph_uri

        try:
            response = requests.delete(
                self.data_endpoint,
                params=params,
                auth=self.auth
            )

            if response.status_code in [200, 204]:
                return {
                    "success": True,
                    "message": "Dataset limpo com sucesso"
                }
            else:
                # print('Caiu aqui')
                # print(response.text)
                return {
                    "success": False,
                    "message": f"Erro ao limpar dataset: {response.text}",
                    "status_code": response.status_code
                }

        except Exception as e:
            import traceback
            return {
                "success": False,
                "message": f"Erro ao limpar dataset: {str(e)}",
                "error": str(e),
                "traceback": traceback.format_exc()
            }


# Exemplo de uso
if __name__ == "__main__":
    # Inicializa o loader
    loader = TurtleLoader(dataset='airdata')

    result = loader.clear_dataset()
    print(result['message'])
    sys.exit()
    # Exemplo 1: Carregar de arquivo
    loader.load_from_directory(r'turtles')
    sys.exit()
    result = loader.load_from_file(r"C:\Coding\AirData\jena-test\ontology_airdata.ttl")
    print(result)

    # Exemplo 2: Carregar string diretamente
    # ttl_data = """
    # @prefix ex: <http://example.org/> .
    # @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    #
    # ex:pessoa1 rdf:type ex:Pessoa ;
    #            ex:nome "João Silva" ;
    #            ex:idade 30 .
    # """
    # result = loader.load_from_string(ttl_data)
    # print(result)
    #
    # # Exemplo 3: Carregar em grafo nomeado
    # result = loader.load_from_file("dados.ttl", graph_uri="http://example.org/graph1")
    # print(result)