# Importando os módulos necessários:
try:
    from modules.connector_spark import Interface_spark
except Exception as e:
    print("Há algum problema na importação de módulos no arquivo connector_cassandra:".format(str(e)))


class Interface_cassandra_nuvem(Interface_spark):
        
    def cria_banco_de_dados_no_cassandra(self, banco_de_dados_cassandra, ip_externo):
        """Cria banco de dados no Cassandra

        Args:
            banco_de_dados_cassandra (string): nome do banco de dados a ser criado no Cassandra
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_cassandra:
            spark_cassandra = self.instancia_spark_cassandra(ip_externo)
            spark_cassandra.conf.set("spark.sql.catalog.desafio", "com.datastax.spark.connector.datasource.CassandraCatalog")
            
            # Cria banco de dados no Cassandra
            spark_cassandra.sql(f"CREATE DATABASE IF NOT EXISTS desafio.{banco_de_dados_cassandra} \
                                        WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='1')")
        except Exception as e:
            print("\n\nErro no método cria_banco_de_dados_no_cassandra da classe Interface_cassandra_nuvem:", str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Banco de dados {banco_de_dados_cassandra} criado com sucesso no Cassandra!")  
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
            return None




    def deleta_banco_de_dados_do_cassandra(self, banco_de_dados_cassandra, ip_externo):
        """Deleta banco de dados no Cassandra

        Args:
            banco_de_dados_cassandra (string): nome do banco de dados a ser deletado no Cassandra
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_cassandra:
            spark_cassandra = self.instancia_spark_cassandra(ip_externo)
            spark_cassandra.conf.set("spark.sql.catalog.desafio", "com.datastax.spark.connector.datasource.CassandraCatalog")
            
            # Deleta banco de dados no Cassandra
            spark_cassandra.sql(fr"DROP DATABASE IF EXISTS desafio.{banco_de_dados_cassandra} CASCADE;")                  
        except Exception as e:
            print("\n\nErro no método deleta_banco_de_dados_do_cassandra da classe Interface_cassandra_nuvem:", str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Banco de dados {banco_de_dados_cassandra} deletado com sucesso do Cassandra!")  
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
            return None




    def cria_tabela_no_cassandra(self, banco_de_dados_cassandra, nome_tabela, variaveis_declaradas, coluna_id, ip_externo):
        """Cria tabela no Cassandra

        Args:
            banco_de_dados_cassandra (string): nome do banco de dados no Cassandra a ser conectado
            nome_tabela (string): nome da tabela a ser criada no Cassandra
            variaveis_declaradas (string): nome das colunas e dos seus tipos a serem criadas no Cassandra
            coluna_id (string): nome da coluna primary key da tabela
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_cassandra:
            spark_cassandra = self.instancia_spark_cassandra(ip_externo)
            spark_cassandra.conf.set("spark.sql.catalog.desafio", "com.datastax.spark.connector.datasource.CassandraCatalog")
            
            # Cria tabela no Cassandra
            spark_cassandra.sql(f"CREATE TABLE desafio.{banco_de_dados_cassandra}.{nome_tabela} ({variaveis_declaradas}) \
                                                                        USING cassandra PARTITIONED BY ({coluna_id});")
        except Exception as e:
            print("\n\nErro no método cria_tabela_no_cassandra da classe Interface_cassandra_nuvem:",str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Tabela {nome_tabela} criada com sucesso no Cassandra!")
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
            return None
            
            


    def deleta_tabela_do_cassandra(self, banco_de_dados_cassandra, ip_externo):
        """Deleta tabela no Cassandra

        Args:
            banco_de_dados_cassandra (string): nome do banco de dados no Cassandra a ser conectado
            nome_tabela (string): nome da tabela a ser deletada no Cassandra
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_cassandra:
            spark_cassandra = self.instancia_spark_cassandra(ip_externo)
            spark_cassandra.conf.set("spark.sql.catalog.desafio", "com.datastax.spark.connector.datasource.CassandraCatalog")
            
            # Usuário digite qual tabela deseja deletar:
            nome_tabela = input("Qual tabela deseja deletar: ")
            
            # Deleta tabela no Cassandra
            if nome_tabela:
                spark_cassandra.sql(fr"DROP TABLE IF EXISTS desafio.{banco_de_dados_cassandra}.{nome_tabela};")
        except Exception as e:
            print("\n\nErro no método deleta_tabela_do_cassandra da classe Interface_cassandra_nuvem:",str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Tabela {nome_tabela} deletada com sucesso do Cassandra!")  
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
            return None




    def insere_dados_no_cassandra(self, lista_com_nome_de_arquivos, banco_de_dados, caminho_arquivo, ip_externo, extensao='parquet'):
        """Método para inserir dados nas tabelas.

        Args:
            lista_com_nome_de_arquivos (string): lista com os nomes dos arquivos cujos nomes são iguais os das tabelas
            banco_de_dados (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
            caminho_arquivo (string): caminho completo onde está o arquivo para ser inserido no Cassandra
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_cassandra:
            spark_cassandra = self.instancia_spark_cassandra(ip_externo)
            
                        
            # Acessa e importa dados no Cassandra para uma variável:
            for nome in lista_com_nome_de_arquivos: 
                local_onde_esta_o_parquet = fr"{caminho_arquivo}\Arquivos_{extensao}_{nome}"
                df_arquivo = spark_cassandra.read.format(extensao)\
                                                 .load(local_onde_esta_o_parquet)
                
                df_arquivo.write.format("org.apache.spark.sql.cassandra")\
                                .mode("overwrite")\
                                .option("confirm.truncate","true")\
                                .option("keyspace", banco_de_dados)\
                                .option("table", nome)\
                                .save()
                
                print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                print(f"Dados inseridos na tabela {nome} com sucesso!")
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n\n")
        except Exception as e:
            print("\n\nErro no método insere_parquet_no_cassandra da classe Interface_spark:", str(e), end="\n\n")
        else:
            return None
            



# Cria classe para criar banco de dados e tabelas. A classe Criacoes_no_cassandra herda classe Interface_cassandra_nuvem.
class Criacoes_e_insercoes_no_cassandra(Interface_cassandra_nuvem): 
    
    def cria_banco_logistica_internacional_desafio_no_cassandra(self, banco_de_dados_cassandra, ip_externo):
        """Método para criar banco de dados no Cassandra. Esse método aciona um método genérico, o cria_banco_de_dados_no_cassandra 

        Args:
            banco_de_dados_cassandra (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        """
        try:
            # Instância a classe:        
            interface_para_criar_banco_de_dados_no_cassandra = Criacoes_e_insercoes_no_cassandra()
        except Exception as e:
            print("\n\nErro no método cria_banco_logistica_internacional_desafio ao instanciar objeto:", str(e),end="\n\n")
                      
        try:
            # Cria banco de dados:
            interface_para_criar_banco_de_dados_no_cassandra.cria_banco_de_dados_no_cassandra(banco_de_dados_cassandra, ip_externo)
        except Exception as e:
            print(f"\n\nErro ao criar banco de dados ({banco_de_dados_cassandra}) pelo método cria_banco_logistica_internacional_desafio_no_cassandra:", str(e), end="\n\n")
        else:
            return None
            # Mensagem de que o banco de dados foi criado com sucesso aparece devido ao método cria_banco_de_dados_no_cassandra da classe Interface_cassandra_nuvem
   
    
    
    
    def cria_tabela_EXPORTACOES_normalizada_no_cassandra(self, banco_de_dados_cassandra, ip_externo):
        """Método para criar a tabela EXPORTACOES_normalizada no Cassandra. Esse método aciona um método genérico, o cria_tabela_no_cassandra. 

        Args:
            banco_de_dados_cassandra (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        """
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_cassandra = Criacoes_e_insercoes_no_cassandra()
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_EXPORTACOES_no_cassandra em Criacoes_no_cassandra:", str(e),end="\n\n")
        
        try:
            # Cria tabela exportações:
            nome_da_tabela = "exportacoes_normalizada"
            coluna_id = "id_exportacao"
            variaveis_declaradas = "id_exportacao bigint,\
                                    ano int,\
                                    produto string,\
                                    pais string,\
                                    bloco string,\
                                    via string,\
                                    kg_liquido bigint,\
                                    valor_fob bigint"
                                    
            interface_para_criar_tabelas_no_cassandra.cria_tabela_no_cassandra(banco_de_dados_cassandra, \
                                                                               nome_da_tabela, \
                                                                               variaveis_declaradas, \
                                                                               coluna_id, \
                                                                               ip_externo)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_EXPORTACOES_no_cassandra:", str(e), end="\n\n") 
        else:
            return None
            # Mensagem de que a tabela foi criada com sucesso aparece devido ao método cria_tabela_no_cassandra da classe Interface_cassandra_nuvem       
            
    
    
    
    def cria_tabela_IMPORTACOES_normalizada_no_cassandra(self, banco_de_dados_cassandra, ip_externo):
        """Método para criar a tabela EXPORTACOES_normalizada no Cassandra. Esse método aciona um método genérico, o cria_tabela_no_cassandra.

        Args:
            banco_de_dados_cassandra (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        """
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_cassandra = Criacoes_e_insercoes_no_cassandra()
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_IMPORTACOES_no_cassandra em Criacoes_no_cassandra:", str(e),end="\n\n")
            
        try:            
            # Cria tabela importações:
            nome_da_tabela = "importacoes_normalizada"
            coluna_id = "id_importacao"
            variaveis_declaradas = "id_importacao bigint,\
                                    ano int,\
                                    produto string,\
                                    pais string,\
                                    bloco string,\
                                    via string,\
                                    kg_liquido bigint,\
                                    valor_fob bigint"
                                    
            interface_para_criar_tabelas_no_cassandra.cria_tabela_no_cassandra(banco_de_dados_cassandra, \
                                                                               nome_da_tabela, \
                                                                               variaveis_declaradas, \
                                                                               coluna_id, \
                                                                               ip_externo)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_IMPORTACOES_no_cassandra:", str(e), end="\n\n")
        else:
            return None



    def insere_parquet_no_cassandra(self, banco_de_dados_cassandra, parquet_para_cassandra, ip_externo):
        """Método que aciona um método genérico, o insere_parquet_no_cassandra 

        Args:
            banco_de_dados_cassandra (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
            parquet_para_cassandra (list): lista com os nomes dos arquivos cujos nomes são iguais os das tabelas
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        """
        try:
            # Lista com os nomes das tabelas que serão abastecidas com os dados do parquet:
            lista_com_nome_de_arquivos = ["importacoes_normalizada", "exportacoes_normalizada"]
            
            # Insere dados no Cassandra:
            self.insere_dados_no_cassandra(lista_com_nome_de_arquivos, \
                                           banco_de_dados_cassandra, \
                                           parquet_para_cassandra, \
                                           ip_externo)
        except Exception as e:
            print(f"\n\nErro ao inserir dados na tabela ({lista_com_nome_de_arquivos}) pelo método cria_tabela_IMPORTACOES_no_cassandra:", str(e), end="\n\n")
        else:
            return None
            # Mensagem de que a tabela foi abastecida com sucesso aparece pelo método insere_parquet_no_cassandra da classe Interface_cassandra_nuvem