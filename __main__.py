# Importa os módulos:
try:
    from modules.connector_spark import conversao_arquivos
    from modules.connector_postgre import Criacoes_e_insercoes_no_postgre
    from modules.connector_cassandra import Criacoes_e_insercoes_no_cassandra
    from modules.path_and_information import *
    import datetime
except Exception as e:
    print("Há algum problema na importação de módulos no arquivo main:".format(str(e)))
    

 
if __name__ == "__main__":
    
    try:
        # Endereços dos arquivos csv e parquet's:
        fato_em_csv, dimensao_em_csv = enderecos_csv()
        parquet_para_postgre_sem_tratamento, parquet_para_postgre_com_tratamentos = enderecos_parquet_para_postgre()
        parquet_para_cassandra = enderecos_parquet_para_cassandra()
        parquet_para_power_bi = enderecos_parquet_para_power_bi()
        
        # Dados de conexão aos SGBD's PostgreSQL e Cassandra:
        usuario_postgre, senha_postgre, host, banco_de_dados_postgre = info_postgre()
        ip_externo, banco_de_dados_cassandra = info_cassandra()    
    except Exception as e:
        print("\n\nErro ao retornar endereços de arquivos e dados dos SGBD's no main:", str(e), end="\n\n")
    

    #############################
    # Instanceamento das classes:
    #############################
    try:
        # Instância a classe:        
        interface_cria_banco_de_dados_postgre = Criacoes_e_insercoes_no_postgre(usuario_postgre, senha_postgre, host)
    except Exception as e:
        print("\n\nErro ao instanciar objeto em Criacoes_no_Postgre no main:", str(e), end="\n\n")


    try:
        # Instancia a classe:        
        interface_cria_tabelas_postgre = Criacoes_e_insercoes_no_postgre(usuario_postgre, senha_postgre, host, banco_de_dados_postgre)
    except Exception as e:
        print("\n\nErro ao instanciar objeto em Criacoes_no_Postgre no main para se conectar ao banco de dados:", str(e),end="\n\n")
    
    
    try:
        # Instância a classe:        
        interface_para_deletar_banco_de_dados_do_postgre = Criacoes_e_insercoes_no_postgre(usuario_postgre, senha_postgre, host)
    except Exception as e:
        print("\n\nErro ao instanciar objeto em Criacoes_no_Postgre no main:", str(e), end="\n\n")
        
    
    try:
        # Instância a classe:        
        interface_para_deletar_tabelas_do_postgre = Criacoes_e_insercoes_no_postgre(usuario_postgre, senha_postgre, host, banco_de_dados_postgre)
    except Exception as e:
        print("\n\nErro ao instanciar objeto em Criacoes_no_Postgre no main:", str(e), end="\n\n")


    try:
        # Instância a classe:        
        interface_cria_banco_de_dados_no_cassandra = Criacoes_e_insercoes_no_cassandra()
    except Exception as e:
        print("\n\nErro ao instanciar objeto em Criacoes_no_cassandra pelo arquivo main:", str(e), end="\n\n")
        

    try:
        # Instancia a classe:        
        interface_cria_tabelas_no_cassandra = Criacoes_e_insercoes_no_cassandra()
    except Exception as e:
        print("\n\nErro ao instanciar objeto em Criacoes_no_cassandra pelo arquivo main para se conectar ao banco de dados:", str(e),end="\n\n")


    try:
        # Instância a classe:        
        interface_para_deletar_banco_de_dados_do_cassandra = Criacoes_e_insercoes_no_cassandra()
    except Exception as e:
        print("\n\nErro ao instanciar objeto em Criacoes_no_cassandra no main:", str(e), end="\n\n")
    
    
    try:
        # Instância a classe:        
        interface_para_deletar_tabelas_do_cassandra = Criacoes_e_insercoes_no_cassandra()
    except Exception as e:
        print("\n\nErro ao instanciar objeto em Criacoes_no_cassandra no main:", str(e), end="\n\n")


    try:
        # Instancia a classe:
        interface_spark_para_converter_arquivos = conversao_arquivos()
    except Exception as e:
        print("\n\nErro ao instanciar classe Interface_spark no main:", str(e), end="\n\n")


    try:
        # Instancia classe:
        interface_postgre_para_inserir_dados = Criacoes_e_insercoes_no_postgre(usuario_postgre, senha_postgre, \
                                                                                  host, banco_de_dados_postgre)
    except Exception as e:
        print("\n\nErro ao instanciar classe no main:", str(e), end="\n\n")


    try:
        # Instancia classe:
        interface_postgre_para_extrair_dados = Criacoes_e_insercoes_no_postgre(usuario_postgre, senha_postgre, \
                                                                                  host, banco_de_dados_postgre)
    except Exception as e:
        print("\n\nErro ao instanciar classe no main:", str(e), end="\n\n")


    try:
        # Instancia classe:
        interface_cassandra_para_inserir_dados = Criacoes_e_insercoes_no_cassandra()
    except Exception as e:
        print("\n\nErro ao instanciar classe no main:", str(e), end="\n\n")


    try:
        # Instancia classe:
        interface_postgre_para_criar_trigger_e_functions = Criacoes_e_insercoes_no_postgre(usuario_postgre, senha_postgre, \
                                                                                             host, banco_de_dados_postgre)
    except Exception as e:
        print("\n\nErro ao instanciar classe no main:", str(e), end="\n\n")
    
    
    
    #################################################
    # Loop para cumprir com as ativadades do projeto:
    #################################################
    while True:
        print("\n")
        print("========================================================")
        print("                    SEJA BEM-VINDO(A)!")
        print("========================================================")
        print("""
                MENU DE OPÇÕES

                0-Encerrar programa
                
                1-Criar banco de dados no PostreSQL
                2-Criar tabela no PostgreSQL
                3-Criar trigger e functions no PostgreSQL
                
                4-Deletar banco de dados no PostgreSQL
                5-Deletar tabela no PostgreSQL
                6-Deletar trigger e functions no PostgreSQL
                
                7-Criar banco de dados no Cassandra
                8-Criar tabela no Cassandra
                
                9-Deletar banco de dados no Cassandra
                10-Deletar tabela no Cassandra
                
                11-Converter arquivos csv para parquet
                12-Inserir dados dos parquets no PostgreSQL
                
                13-Exportar dados do PostgreSQL em formato parquet
                14-Inserir dados no Cassandra da Google Cloud
            """)
    
        print("========================================================")
        option = input("Selecione uma das opções: ")
        print("\n")



        if option == "0":
            print("========================================================")
            print("               PROGRAMA FINALIZADO!")
            print("========================================================")
            break
    
    
    
    
        ##############################################
        # Conecta ao PostgreSQL e cria banco de dados:
        ##############################################
        if option == "1":
                    
            try:
                # Cria banco de dados:
                interface_cria_banco_de_dados_postgre.cria_banco_logistica_internacional_desafio_no_postgre(banco_de_dados_postgre)
            except Exception as e:
                print(f"\n\nErro ao criar banco de dados {banco_de_dados_postgre} no main:", str(e), end="\n\n")
            
        
        
        
        ###############
        # Cria tabelas:
        ###############
        if option == "2":
            
            print("Iniciando criação das tabelas no PostgreSQL", end="\n\n")
            main_tempo1 = datetime.datetime.now()
            
            
            try:
                # Cria tabela exportações:
                interface_cria_tabelas_postgre.cria_tabela_EXPORTACOES_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_EXPORTACOES_no_postgre no main:", str(e), end="\n\n") 


            try:
                # Cria tabela importações:
                interface_cria_tabelas_postgre.cria_tabela_IMPORTACOES_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_IMPORTACOES_no_postgre no main:", str(e), end="\n\n")


            try:
                # Cria tabela NCM:
                interface_cria_tabelas_postgre.cria_tabela_NCM_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_NCM_no_postgre no main:", str(e), end="\n\n")


            try:
                # Cria tabela país_bloco:
                interface_cria_tabelas_postgre.cria_tabela_BLOCO_PAIS_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_BLOCO_PAIS_no_postgre no main:", str(e), end="\n\n")  


            try:
                # Cria tabela país:
                interface_cria_tabelas_postgre.cria_tabela_PAIS_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_PAIS_no_postgre no main:", str(e), end="\n\n")


            try:
                # Cria tabela UF_Mun:
                interface_cria_tabelas_postgre.cria_tabela_UF_MUN_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_UF_MUN_no_postgre no main:", str(e), end="\n\n")


            try:
                # Cria tabela UF:
                interface_cria_tabelas_postgre.cria_tabela_UF_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_UF_no_postgre no main:", str(e), end="\n\n")


            try:
                # Cria tabela URF:
                interface_cria_tabelas_postgre.cria_tabela_URF_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_URF_no_postgre no main:", str(e), end="\n\n")


            try:
                # Cria tabela Via:
                interface_cria_tabelas_postgre.cria_tabela_VIA_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_VIA_no_postgre no main:", str(e), end="\n\n")


            try:
                # Cria tabela Via:
                interface_cria_tabelas_postgre.cria_tabela_exportacoes_normalizada_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_exportacoes_normalizada_no_postgre no main:", str(e), end="\n\n")


            try:
                # Cria tabela Via:
                interface_cria_tabelas_postgre.cria_tabela_importacoes_normalizada_no_postgre()
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_importacoes_normalizada_no_postgre no main:", str(e), end="\n\n")


            main_tempo2 = datetime.datetime.now()
            main_total = main_tempo2 - main_tempo1
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
            print("O tempo total para criação das tabelas no PostgreSQL foi de: ", main_total)
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n\n")
            
        
        
        
        #####################################################
        # Conecta no PostgreSQL e cria trigger's e functions: 
        #####################################################
        if option == "3":
            
            try:
                interface_postgre_para_criar_trigger_e_functions.cria_trigger_e_function_exportacoes_normalizada_no_postgre()
            except Exception as e:
                print("Erro no método cria_trigger_e_function_exportacoes_normalizada_no_postgre no main:", str(e), end="\n\n")
            
            
            try:
                interface_postgre_para_criar_trigger_e_functions.cria_trigger_e_function_importacoes_normalizada_no_postgre()
            except Exception as e:
                print("Erro no método cria_trigger_e_function_importacoes_normalizada_no_postgre no main:", str(e), end="\n\n")

        
        
        
        ################################################
        # Conecta ao PostgreSQL e deleta banco de dados:
        ################################################
        if option == "4":
                    
            try:
                # Cria banco de dados:
                interface_para_deletar_banco_de_dados_do_postgre.deleta_banco_de_dados_do_postgre(banco_de_dados_postgre)
            except Exception as e:
                print(f"\n\nErro ao deletar banco de dados {banco_de_dados_postgre} no main:", str(e), end="\n\n")
    



        #########################################
        # Conecta ao PostgreSQL e deleta tabelas:
        #########################################
        if option == "5":
                    
            try:
                # Deleta tabela:
                interface_para_deletar_tabelas_do_postgre.deleta_tabela_do_postgre()
            except Exception as e:
                print(f"\n\nErro ao deletar banco de dados {banco_de_dados_postgre} do PostgreSQL pelo arquivo main:", str(e), end="\n\n")
        
        
        
        
        #######################################################
        # Conecta no PostgreSQL e deleta trigger's e functions: 
        #######################################################
        if option == "6":
            
            try:
                interface_postgre_para_criar_trigger_e_functions.deleta_trigger_e_function_exportacoes_normalizada_no_postgre()
            except Exception as e:
                print("Erro no método deleta_trigger_e_function_exportacoes_normalizada_no_postgre no main:", str(e), end="\n\n")
            
            
            try:
                interface_postgre_para_criar_trigger_e_functions.deleta_trigger_e_function_importacoes_normalizada_no_postgre()
            except Exception as e:
                print("Erro no método deleta_trigger_e_function_importacoes_normalizada_no_postgre no main:", str(e), end="\n\n")




        #############################################
        # Conecta ao Cassandra e cria banco de dados:
        #############################################
        if option == "7":
                    
            try:
                # Cria banco de dados:
                interface_cria_banco_de_dados_no_cassandra.cria_banco_logistica_internacional_desafio_no_cassandra(banco_de_dados_cassandra, \
                                                                                                                   ip_externo)
            except Exception as e:
                print(f"\n\nErro ao criar banco de dados {banco_de_dados_cassandra} do Cassandra pelo arquivo main:", str(e), end="\n\n")
            
        
        
        
        ###############
        # Cria tabelas:
        ###############
        if option == "8":
            
            print("Iniciando criação das tabelas no Cassandra", end="\n\n")
            main_tempo1 = datetime.datetime.now()


            try:
                # Cria tabela importações:
                interface_cria_tabelas_no_cassandra.cria_tabela_EXPORTACOES_normalizada_no_cassandra(banco_de_dados_cassandra, ip_externo)
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_EXPORTACOES_normalizada_no_cassandra pelo arquivo main:", str(e), end="\n\n")


            try:
                # Cria tabela importações:
                interface_cria_tabelas_no_cassandra.cria_tabela_IMPORTACOES_normalizada_no_cassandra(banco_de_dados_cassandra, ip_externo)
            except Exception as e:
                print(f"\n\nErro ao usar método cria_tabela_IMPORTACOES_normalizada_no_cassandra pelo arquivo main:", str(e), end="\n\n")


            main_tempo2 = datetime.datetime.now()
            main_total = main_tempo2 - main_tempo1
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
            print("O tempo total para criação das tabelas no Cassandra foi de: ", main_total)
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n\n")

        
        
        
        ###############################################
        # Conecta ao Cassandra e deleta banco de dados:
        ###############################################
        if option == "9":
                    
            try:
                # Cria banco de dados:
                interface_para_deletar_banco_de_dados_do_cassandra.deleta_banco_de_dados_do_cassandra(banco_de_dados_cassandra, ip_externo)
            except Exception as e:
                print(f"\n\nErro ao deletar banco de dados {banco_de_dados_cassandra} do Cassandra pelo arquivo main:", str(e), end="\n\n")
    



        #########################################
        # Conecta ao Cassandra e deleta tabelas:
        #########################################
        if option == "10":
                    
            try:
                # Deleta tabela:
                interface_para_deletar_tabelas_do_cassandra.deleta_tabela_do_cassandra(banco_de_dados_cassandra, ip_externo)
            except Exception as e:
                print(f"\n\nErro ao deletar banco de dados {banco_de_dados_cassandra} do Cassandra pelo arquivo main:", str(e), end="\n\n")
        
        
        
        
        #########################################################################################
        # Conecta ao Pyspark para carregar dados brutos (arquivos csv) e convertê-los em parquet:
        #########################################################################################
        if option == "11":
            
            # Início da leitura e conversão dos dados para parquet:
            print("Iniciando criação dos arquivos Parquet's", end="\n\n")
            main_tempo1 = datetime.datetime.now()
            
            
            try:
                # Converte arquivos csv referentes às tabelas dimensões (ex.: NCM, PAIS...) para parquet:
                arquivo_parquet = interface_spark_para_converter_arquivos.conversao_de_um_csv_para_parquet(dimensao_em_csv, \
                                                                                                           parquet_para_postgre_sem_tratamento)
            except Exception as e:
                print("\n\nErro ao criar os arquivos parquet referente às tabelas dimensão no main:", str(e), end="\n\n")
            
            
            try:
                # Converte arquivos csv referentes à exportações para parquet:
                interface_spark_para_converter_arquivos.conversao_de_muitos_arquivos_exportacoes_para_parquet(fato_em_csv, \
                                                                                                              parquet_para_postgre_sem_tratamento)
            except Exception as e:
                print("\n\nErro ao criar os arquivos parquet referente às tabelas exportações no main:", str(e), end="\n\n")
            
            
            try:
                # Converte arquivos csv referentes à importacões para parquet:
                interface_spark_para_converter_arquivos.conversao_de_muitos_arquivos_importacoes_para_parquet(fato_em_csv, \
                                                                                                              parquet_para_postgre_sem_tratamento)
            except Exception as e:
                print("\n\nErro ao criar os arquivos parquet referente às tabelas importações no main:", str(e), end="\n\n")
            
            
            main_tempo2 = datetime.datetime.now()
            main_total = main_tempo2 - main_tempo1
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
            print("O tempo total para converter arquivos csv para Parquet's foi de: ", main_total)
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n\n")




        ###################################################
        # Conecta no PostgreSQL e insere dados via Pyspark: 
        ###################################################
        if option == "12":
                    
                    
            # Inserção dos dados Parquet no Postgre via Pyspark:
            print("\n\nIniciando inserção de dados Parquet no PostgreSQL!", end="\n\n")
            main_tempo1 = datetime.datetime.now()
            

            try:
                interface_postgre_para_inserir_dados.insere_parquet_no_postgre(parquet_para_postgre_sem_tratamento, \
                                                                               parquet_para_postgre_com_tratamentos, \
                                                                               banco_de_dados_postgre)
            except Exception as e:
                print("Erro ao inserir dados no postgre utilizando parquet:", str(e))
            
            
            main_tempo2 = datetime.datetime.now()
            main_total = main_tempo2 - main_tempo1
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
            print("O tempo total para inserção de dados no PostgreSQL foi de: ", main_total)
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n\n")
        
        


        ############################################################
        # Busca e exporta dados do Postgre para parquet via Pyspark:
        ############################################################
        if option == "13":
            
            print("Iniciando extração de dados no PostgreSQL", end="\n\n")
            main_tempo1 = datetime.datetime.now()
            
            
            try:
                # Seleciona e extrai dados do PostgreSQL em formato parquet:
                interface_postgre_para_extrair_dados.extrai_dados_do_postgre(banco_de_dados_postgre, parquet_para_cassandra)
            except Exception as e:
                print("Erro ao inserir dados no postgre utilizando parquet:", str(e))

            
            main_tempo2 = datetime.datetime.now()
            main_total = main_tempo2 - main_tempo1
            print("O tempo total para extração de dados no PostgreSQL foi de: ", main_total, end="\n\n")
    
    
    
    
        ##################################################
        # Conecta no Cassandra e insere dados via Pyspark: 
        ##################################################
        if option == "14":
            
            print("Iniciando inserção de dados no Cassandra", end="\n\n")
                
                
            try:

                interface_cassandra_para_inserir_dados.insere_parquet_no_cassandra(banco_de_dados_cassandra, \
                                                                                   parquet_para_cassandra, \
                                                                                   ip_externo)
                        
            except Exception as e:
                print("Erro ao inserir dados no Cassandra utilizando parquet:", str(e))