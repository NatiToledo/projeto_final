# Importa módulos:
try:
    import os
except Exception as e:
    print("Há algum problema na importação de módulos no arquivo path_and_information:".format(str(e)))


def cria_e_retorna_enderecos_dos_diretorios(diretorio):
    try:
        # Armazena o endereço raiz cujas pastas contendo os dados do projeto serão armazenadas:
        caminho_raiz = r"(Inserir o caminho do arquivo)"
        
        # Junta o endereço raiz com um diretório/pasta que se deseja criar:
        caminho_completo = os.path.join(caminho_raiz, diretorio)
        
        if not os.path.exists(caminho_completo):
            # Cria diretório:
            os.makedirs(caminho_completo)
            print(f"\nDiretório {diretorio} criado com sucesso!\n")
        else:
            print(f"\n\nDiretório {diretorio} já existe!\n\n")
            pass
    except Exception as e:
        print("\n\nErro no método cria_e_retorna_enderecos_dos_diretorios:", str(e))
    else:       
        # Retorna endereço completo do diretório:
        return caminho_completo


def enderecos_csv():
    try:
        # Nomes das pastas/diretórios que irão armazenar os arquivos csv's:
        diretorio_fato = "1_Tabela_fato"
        diretorio_dimensao = "1_Tabela_dimensao"
        
        # Criam-se diretórios para armazenar todos os datasets:
        caminho_completo_fato = cria_e_retorna_enderecos_dos_diretorios(diretorio_fato)
        caminho_completo_dimensao = cria_e_retorna_enderecos_dos_diretorios(diretorio_dimensao)
        
        # Armazena o endereco completo das pastas criadas:
        endereco_fato_em_csv = fr"{caminho_completo_fato}"
        endereco_dimensao_em_csv = fr"{caminho_completo_dimensao}"
    except Exception as e:
        print("\n\nErro na função enderecos_csv:", str(e), end="\n\n")
    else:
        # Retorna o endereco dos arquivos csv para serem lidos ou gravados:
        return endereco_fato_em_csv, endereco_dimensao_em_csv




def enderecos_parquet_para_postgre():
    try:
        # Nomes das pastas/diretórios que irão armazenar os arquivos parquet's cujos dados serão inseridos no PostgreSQL:
        diretorio_postgre = "2_Parquets_para_postgre"
        diretorio_para_formatados = r"2_Parquets_para_postgre\Formatados"

        # Cria diretório:
        caminho_completo_sem_tratamento_postgre = cria_e_retorna_enderecos_dos_diretorios(diretorio_postgre)

        # Cria diretórios para armazernar os parquets cujos dados tiveram que ser tratados antes de inserí-los no PostgreSQL:
        caminho_completo_formatados_postgre = cria_e_retorna_enderecos_dos_diretorios(diretorio_para_formatados)
        
        # Armazena o endereco completo das pastas criadas:
        parquet_para_postgre_sem_tratamento = fr"{caminho_completo_sem_tratamento_postgre}"
        parquet_para_postgre_com_tratamentos = fr"{caminho_completo_formatados_postgre}"
    except Exception as e:
        print("\n\nErro na função enderecos_parquet_para_postgre:", str(e), end="\n\n")
    else:
        # Retorna o endereco dos arquivos parquet's para serem lidos ou gravados:
        return parquet_para_postgre_sem_tratamento, parquet_para_postgre_com_tratamentos




def enderecos_parquet_para_cassandra():
    try:
        # Nomes das pastas/diretórios que irão armazenar os arquivos parquet's cujos dados serão inseridos no Cassandra:
        diretorio_cassandra = "3_Parquets_para_cassandra"

        # Cria diretório:
        caminho_completo_cassandra = cria_e_retorna_enderecos_dos_diretorios(diretorio_cassandra)
        
        # Armazena o endereco completo das pastas criadas:
        parquet_para_cassandra = fr"{caminho_completo_cassandra}"
    except Exception as e:
        print("\n\nErro na função enderecos_parquet_para_cassandra:", str(e), end="\n\n")
    else:
        # Retorna o endereco dos arquivos parquet's para serem lidos ou gravados:
        return parquet_para_cassandra
    



def enderecos_parquet_para_power_bi():
    try:
        # Nomes das pastas/diretórios que irão armazenar os arquivos parquet's cujos dados serão inseridos no Power BI:
        diretorio_power_bi = "4_Parquets_para_Power_BI"

        # Cria diretório:
        caminho_completo_power_bi = cria_e_retorna_enderecos_dos_diretorios(diretorio_power_bi)

        # Armazena o endereco completo das pastas criadas:
        parquet_para_power_bi = fr"{caminho_completo_power_bi}"
    except Exception as e:
        print("\n\nErro na função enderecos_parquet_para_cassandra:", str(e), end="\n\n")
    else:
        # Retorna o endereco dos arquivos parquet's para serem lidos ou gravados:
        return parquet_para_power_bi




def info_postgre():
    try:
        # Dados para realizar query's no PostgreSQL via python:
        usuario_postgre = "postgres"
        senha_postgre = "postgres"
        host = "127.0.0.1"
        banco_de_dados_postgre = "logistica_internacional_desafio"
    except Exception as e:
        print("\n\nErro na função info_postgre:", str(e), end="\n\n")
    else:
        return usuario_postgre, senha_postgre, host, banco_de_dados_postgre




def info_cassandra():
    try:
        # Dados para realizar query's no Cassandra via python:
        ip_externo = "34.151.211.111"
        banco_de_dados_cassandra = "logistica_internacional_desafio"
    except Exception as e:
        print("\n\nErro na função info_postgre:", str(e), end="\n\n")
    else:
        return ip_externo, banco_de_dados_cassandra
