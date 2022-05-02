# Importa módulos:
try:
    import psycopg2
    from modules.connector_spark import Interface_spark
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    import datetime
except Exception as e:
    print("Há algum problema na importação de módulos no arquivo connector_postgre:".format(str(e)))


# Cria classe para conectar e processar dados armazenados no PostgreSQL via python:
class Interface_postgre (Interface_spark):
    

    def __init__(self, usuario_postgre="", senha_postgre="", host="", banco_de_dados_postgre=""):
        """Construtor da classe Interface_postgre

        Args:
            usuario_postgre (string): recebe o nome do usuário para conectar com o banco de dados\n
            senha_postgre (string): recebe a senha do usuário para acessar o banco de dados\n
            host (string): recebe "localhost" ou o seu número\n
            banco_de_dados_postgre (string): recebe nome do banco de dados\n
        """

        try:
            self.usuario_postgre = usuario_postgre
            self.senha_postgre = senha_postgre
            self.host = host
            self.banco_de_dados_postgre = banco_de_dados_postgre
        except Exception as e:
            print("\n\nErro no construtor da classe Interface_postgre:", str(e), end="\n\n")
    
    
    
    
    def conecta_postgre(self):
        """Realiza a conexão com o postgreSQL para criar banco de dados

        Returns:
            conexao: requer nome do usuário, senha, localhost e nome do banco de dados\n
            cursor: realiza a conexão com o postgreSQL\n
        """
        try:
            # Conecta ao PostgreSQL
            conexao = psycopg2.connect(user = self.usuario_postgre,
                                       password = self.senha_postgre,
                                       host = self.host)
            
            # Isola transação:
            conexao.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            # Habilita cursor
            cursor = conexao.cursor()
        except Exception as e:
            print("\n\nErro no método conecta_postgre da classe interface_postgre:", str(e), end="\n\n")
        else:
            return conexao, cursor



    
    def desconecta_postgre(self, conexao, cursor):
        """Realiza a desconexão com o postgreSQL"""
        try:
            cursor.close() # Desabilita cursor
            conexao.commit() # Salva modificações
            conexao.close() # Desconecta do PostgreSQL
        except Exception as e:
            print("\n\nErro no método desconecta_postgre da classe interface_postgre:", str(e), end="\n\n") 
        else:
            return None




    def conecta_ao_banco_de_dados_do_postgre(self):
        """Realiza a conexão com o postgreSQL

        Returns:
            conexao: requer nome do usuário, senha, localhost e nome do banco de dados\n
            cursor: realiza a conexão com o postgreSQL\n
        """
        try:
            # Conecta ao PostgreSQL:
            conexao = psycopg2.connect(user = self.usuario_postgre,
                                       password = self.senha_postgre,
                                       host = self.host,
                                       database = self.banco_de_dados_postgre)
            
            # Habilita cursor:
            cursor = conexao.cursor()
        except Exception as e:
            print("\n\nErro no método conecta_ao_banco_de_dados_do_postgre da classe interface_postgre:", str(e), end="\n\n")
        else:
            return conexao, cursor




    def cria_banco_de_dados_no_postgre(self, banco_de_dados_postgre):
        """Cria banco de dados no PostgreSQL

        Args:
            banco_de_dados_postgre (string): nome do banco de dados a ser criado no PostgreSQL
        """
        try:
            # Conecta ao PostgreSQL e habilita cursor:
            conexao, cursor = self.conecta_postgre()
            
            # Comando SQL para criar banco de dados no PostgreSQL:
            comando_sql_cria_banco_de_dados = f"CREATE DATABASE {banco_de_dados_postgre};"
            
            # Cria banco de dados no PostgreSQL:
            cursor.execute(comando_sql_cria_banco_de_dados)                      
        except Exception as e:
            print("\n\nErro no método cria_banco_de_dados_no_postgre da classe interface_postgre:", str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Banco de dados {banco_de_dados_postgre} criado com sucesso no PostgreSQL!")  
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        finally:
            # Desconecta do PostgreSQL:
            self.desconecta_postgre(conexao, cursor)
            return None




    def deleta_banco_de_dados_do_postgre(self, banco_de_dados_postgre):
        """Deleta banco de dados no PostgreSQL

        Args:
            banco_de_dados_postgre (string): nome do banco de dados a ser apagado no PostgreSQL
        """
        try:
            # Conecta ao PostgreSQL e habilita cursor:
            conexao, cursor = self.conecta_postgre()
            
            # Comando SQL para deletar banco de dados no PostgreSQL:
            comando_sql_deleta_banco_de_dados = f"DROP DATABASE IF EXISTS {banco_de_dados_postgre};"
            
            # Deleta banco de dados do PostgreSQL:
            cursor.execute(comando_sql_deleta_banco_de_dados)                      
        except Exception as e:
            print("\n\nErro no método deleta_banco_de_dados_do_postgre da classe interface_postgre:", str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Banco de dados {banco_de_dados_postgre} deletado com sucesso do PostgreSQL!")  
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        finally:
            # Desconecta do PostgreSQL:
            self.desconecta_postgre(conexao, cursor)
            return None
    
    
    

    def cria_tabela_no_postgre(self, nome_tabela, variaveis_declaradas):
        """Cria tabela no PostgreSQL

        Args:
            nome_tabela (string): nome da tabela a ser criada no PostgreSQL
            variaveis_declaradas (string): nome das colunas e dos seus tipos a serem criadas no PostgreSQL
        """
        try:
            # Conecta ao PostgreSQL e habilita cursor:
            conexao, cursor = self.conecta_ao_banco_de_dados_do_postgre()
            
            # Comando SQL para criar tabela:
            comando_sql_cria_tabelas = f"CREATE TABLE IF NOT EXISTS {nome_tabela} ({variaveis_declaradas});"
            
            # Cria tabela:
            cursor.execute(comando_sql_cria_tabelas)
        except Exception as e:
            print("\n\nErro no método cria_tabela_no_postgre da classe interface_postgre:",str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Tabela {nome_tabela} criada com sucesso no PostgreSQL!")
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        finally:
            # Desconecta do PostgreSQL:
            self.desconecta_postgre(conexao, cursor)
            return None




    def deleta_tabela_do_postgre(self):
        """Deleta tabela no PostgreSQL"""

        try:
            # Conecta ao PostgreSQL e habilita cursor:
            conexao, cursor = self.conecta_ao_banco_de_dados_do_postgre()
            
            # Usuário digite qual tabela deseja deletar:
            nome_tabela = input("Qual tabela deseja deletar: ")
            
            # Deleta tabela no Cassandra:
            if nome_tabela:
                comando_sql_deleta_tabelas = f"DROP TABLE IF EXISTS {nome_tabela};"
                cursor.execute(comando_sql_deleta_tabelas)
        except Exception as e:
            print("\n\nErro no método deleta_tabela_do_postgre da classe interface_postgre:",str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Tabela {nome_tabela} deletada com sucesso!")  
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        finally:
            # Desconecta do PostgreSQL:
            self.desconecta_postgre(conexao, cursor)
            return None
        
        
        
        
    def insere_dados_no_postgre(self, lista_com_nome_de_arquivos, banco_de_dados, caminho_parquet, extensao='parquet'):
        """Método para inserir dados nas tabelas.

        Args:
            lista_com_nome_de_arquivos (string): lista com os nomes dos arquivos cujos nomes são iguais os das tabelas
            banco_de_dados (string): nome do banco de dados que possui as tabelas a serem abastecidas com os dados
            caminho_parquet (string): caminho completo onde está o arquivo parquet os quais os dados serão inseridos no postgreSQL
            extensao (string): formato cujos dados serão carregados (ex.: parquet)
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_postgre:
            spark_postgre = self.instancia_spark_postgre()
            
            # Acessa e importa dados no PostgreSQL para uma variável:
            endereco_jdbc_parcial = "jdbc:postgresql://localhost:5432"
            driver = "org.postgresql.Driver"
            for arquivo in lista_com_nome_de_arquivos:
                # Marca tempo de inserção inicial de dados no PostgreSQL:
                print(f"\n\nIniciando inserção de dados da tabela {arquivo} no PostgreSQL!", end="\n\n")
                main_time1 = datetime.datetime.now()
                
                # Lê arquivo parquet:
                df_arquivo = spark_postgre.read.format(extensao).load(fr"{caminho_parquet}\Arquivos_{extensao}_{arquivo}")
                
                # Grava arquivos parquet:
                df_arquivo.write.format("jdbc") \
                                .mode("overwrite") \
                                .option("truncate", "true") \
                                .option("driver", driver) \
                                .option("url", f"{endereco_jdbc_parcial}/{banco_de_dados}") \
                                .option("dbtable", arquivo) \
                                .option("user", self.usuario_postgre) \
                                .option("password", self.senha_postgre) \
                                .save()
                                
                print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                print(f"Dados inseridos com sucesso na tabela {arquivo}!")
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n\n")
                
                # Marca tempo de inserção final de dados no PostgreSQL:
                main_time2 = datetime.datetime.now()
                main_total = main_time2 - main_time1
                print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                print(f"O tempo total para inserção de dados da tabela {arquivo} no PostgreSQL foi de: ", main_total)
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n\n")
        except Exception as e:
            print("\n\nErro no método insere_parquet_no_postgre da classe Interface_postgre:", str(e), end="\n\n")
        else:
            return None
    
    
    
    
    def seleciona_dados_do_postgre_e_converte_para_parquet(self, banco_de_dados, lista_com_nome_de_arquivos, \
                                                           caminho_parquet_apos_postgre, extensao='parquet'):
        """Método para selecionar dados das tabelas no postgre e exportá-los para formato parquet.

        Args:
            banco_de_dados (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
            lista_com_nome_de_arquivos (string): lista com os nomes dos arquivos cujos nomes são iguais os das tabelas
            caminho_parquet (string): caminho completo onde ficará o arquivo parquet após extração de dados do postgreSQL
            extensao (string): formato cujos dados serão salvos (ex.: parquet)
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_postgre:
            spark_postgre = self.instancia_spark_postgre()
            
            # Acessa e importa dados no PostgreSQL para uma variável:
            endereco_jdbc_parcial = r"jdbc:postgresql://localhost:5432"
            driver = "org.postgresql.Driver"
            for arquivo in lista_com_nome_de_arquivos:
                # Seleciona dados do PostgreSQL:
                df_arquivo = spark_postgre.read.format("jdbc") \
                                               .option("url", f"{endereco_jdbc_parcial}/{banco_de_dados}") \
                                               .option("dbtable", arquivo) \
                                               .option("user", self.usuario_postgre) \
                                               .option("password", self.senha_postgre) \
                                               .option("driver", driver) \
                                               .load()
                
                # Grava arquivos parquet na máquina local:
                df_arquivo.write.format(extensao) \
                                .mode("overwrite") \
                                .option("truncate", "true") \
                                .save(f"{caminho_parquet_apos_postgre}/Arquivos_{extensao}_{arquivo}")
                
                print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                print(f"Dados da tabela {arquivo} convertidos em Parquet com sucesso!")
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n\n")
        except Exception as e:
            print("\n\nErro no método seleciona_dados_do_postgre_e_converte_para_parquet da classe Interface_postgre:", str(e), end="\n\n")
        else:
            return None
    
    
    
    
# Cria classe para criar banco de dados e tabelas. A classe Criacoes_no_postgre herda classe Interface_postgre.
class Criacoes_e_insercoes_no_postgre(Interface_postgre): 
    
    def cria_banco_logistica_internacional_desafio_no_postgre(self, banco_de_dados_postgre):
        """Método para criar banco de dados no PostgreSQL. Esse método aciona um método genérico, o cria_banco_de_dados_no_postgre 

        Args:
            banco_de_dados_postgre (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
        """
        try:
            # Instância a classe:        
            interface_para_criar_banco_de_dados_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, self.senha_postgre, self.host)
        except Exception as e:
            print("\n\nErro no método cria_banco_logistica_internacional_desafio_no_postgre ao instanciar objeto:", str(e),end="\n\n")
                      
        try:
            # Cria banco de dados:
            interface_para_criar_banco_de_dados_no_postgre.cria_banco_de_dados_no_postgre(banco_de_dados_postgre)
        except Exception as e:
            print(f"\n\nErro ao criar banco de dados ({banco_de_dados_postgre}) pelo método cria_banco_logistica_internacional_desafio:", str(e), end="\n\n")
        else:
            return None
            # Mensagem de que o banco de dados foi criado com sucesso aparece devido ao método cria_banco_de_dados_no_postgre da classe Interface_postgre
   
    
    
    
    def cria_tabela_EXPORTACOES_no_postgre(self):
        """Método para criar a tabela exportações no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                                      self.senha_postgre, \
                                                                                      self.host, \
                                                                                      self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_EXPORTACOES_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
        
        try:
            # Cria tabela exportações:
            nome_da_tabela = "EXPORTACOES"
            variaveis_declaradas = "ID_EXP serial PRIMARY KEY,\
                                    CO_ANO int,\
                                    CO_MES int,\
                                    CO_NCM bigint,\
                                    CO_UNID int,\
                                    CO_PAIS int,\
                                    SG_UF_NCM text,\
                                    CO_VIA bigint,\
                                    CO_URF bigint,\
                                    QT_ESTAT bigint,\
                                    KG_LIQUIDO bigint,\
                                    VL_FOB bigint"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_EXPORTACOES_no_postgre:", str(e), end="\n\n")
        else:
            return None
            # Mensagem de que a tabela foi criada com sucesso aparece devido ao método cria_tabela_no_postgre da classe Interface_postgre      
            
    
    
    
    def cria_tabela_IMPORTACOES_no_postgre(self):
        """Método para criar a tabela importações no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                                      self.senha_postgre, \
                                                                                      self.host, \
                                                                                      self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_IMPORTACOES_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:            
            # Cria tabela importações:
            nome_da_tabela = "IMPORTACOES"
            variaveis_declaradas =  "ID_IMP serial PRIMARY KEY,\
                                    CO_ANO int,\
                                    CO_MES int,\
                                    CO_NCM bigint,\
                                    CO_UNID int,\
                                    CO_PAIS int,\
                                    SG_UF_NCM text,\
                                    CO_VIA bigint,\
                                    CO_URF bigint,\
                                    QT_ESTAT bigint,\
                                    KG_LIQUIDO bigint,\
                                    VL_FOB bigint,\
                                    VL_FRETE bigint,\
                                    VL_SEGURO bigint"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_IMPORTACOES_no_postgre:", str(e), end="\n\n")
        else:
            return None 
            
            
            
        
    def cria_tabela_NCM_no_postgre(self):
        """Método para criar a tabela NCM no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_NCM_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:
            # Cria tabela NCM:
            nome_da_tabela = "NCM"
            variaveis_declaradas = "ID_NCM serial PRIMARY KEY,\
                                    CO_NCM bigint,\
                                    CO_UNID bigint,\
                                    CO_SH6 bigint,\
                                    CO_PPE bigint,\
                                    CO_PPI bigint,\
                                    CO_FAT_AGREG bigint,\
                                    CO_CUCI_ITEM bigint,\
                                    CO_CGCE_N3 bigint,\
                                    CO_SIIT bigint,\
                                    CO_ISIC_CLASSE bigint,\
                                    CO_EXP_SUBSET bigint,\
                                    NO_NCM_POR text,\
                                    NO_NCM_ESP text,\
                                    NO_NCM_ING text"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_NCM_no_postgre:", str(e), end="\n\n")
        else:
            return None
            



    def cria_tabela_BLOCO_PAIS_no_postgre(self):
        """Método para criar a tabela bloco_país no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_BLOCO_PAIS_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:            
            # Cria tabela país_bloco:
            nome_da_tabela = "PAIS_BLOCO"
            variaveis_declaradas = "ID_PAIS_BLOCO serial PRIMARY KEY,\
                                    CO_PAIS bigint,\
                                    CO_BLOCO bigint,\
                                    NO_BLOCO text,\
                                    NO_BLOCO_ING text,\
                                    NO_BLOCO_ESP text"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_BLOCO_PAIS_no_postgre:", str(e), end="\n\n")
        else:
            return None
            
            
    
    
    def cria_tabela_PAIS_no_postgre(self):
        """Método para criar a tabela país no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_PAIS_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:
            # Cria tabela país:
            nome_da_tabela = "PAIS"
            variaveis_declaradas = "ID_PAIS serial PRIMARY KEY,\
                                    CO_PAIS bigint,\
                                    CO_PAIS_ISON3 bigint,\
                                    CO_PAIS_ISOA3 text,\
                                    NO_PAIS text,\
                                    NO_PAIS_ING text,\
                                    NO_PAIS_ESP text"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_PAIS_no_postgre:", str(e), end="\n\n")
        else:
            return None
            
            
            
            
    def cria_tabela_UF_MUN_no_postgre(self):
        """Método para criar a tabela UF_MUN no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_UF_MUN_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:
            # Cria tabela UF_Mun:
            nome_da_tabela = "UF_MUN"
            variaveis_declaradas = "ID_UF_MUN serial PRIMARY KEY,\
                                    CO_MUN_GEO bigint,\
                                    NO_MUN text,\
                                    NO_MUN_MIN text,\
                                    SG_UF text"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_UF_MUN_no_postgre:", str(e), end="\n\n")
        else:
            return None
            
            
    
    
    def cria_tabela_UF_no_postgre(self):
        """Método para criar a tabela UF no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_UF_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:
            # Cria tabela UF:
            nome_da_tabela = "UF"
            variaveis_declaradas = "ID_UF serial PRIMARY KEY,\
                                    CO_UF bigint,\
                                    SG_UF text,\
                                    NO_UF text,\
                                    NO_REGIAO text"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_UF_no_postgre:", str(e), end="\n\n")
        else:
            return None
            
    
    
    
    def cria_tabela_URF_no_postgre(self):
        """Método para criar a tabela URF no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_URF_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:
            # Cria tabela URF:
            nome_da_tabela = "URF"
            variaveis_declaradas = "ID_URF serial PRIMARY KEY,\
                                    CO_URF bigint,\
                                    NO_URF text"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_URF_no_postgre:", str(e), end="\n\n")
        else:
            return None
            
            
            
    
    def cria_tabela_VIA_no_postgre(self):
        """Método para criar a tabela VIA no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_VIA_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:
            # Cria tabela Via:
            nome_da_tabela = "VIA"
            variaveis_declaradas = "ID_VIA serial PRIMARY KEY,\
                                    CO_VIA bigint,\
                                    NO_VIA text"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_VIA_no_postgre:", str(e), end="\n\n")
        else:
            return None
        
        
        
        
    def cria_tabela_exportacoes_normalizada_no_postgre(self):
        """Método para criar a tabela VIA no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_VIA_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:
            # Cria tabela exportacoes_normalizada:
            nome_da_tabela = "exportacoes_normalizada"
            variaveis_declaradas = "id_exportacao serial PRIMARY KEY,\
                                    ano int,\
                                    produto text,\
                                    pais text,\
                                    bloco text,\
                                    via text,\
                                    kg_liquido bigint,\
                                    valor_fob bigint"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_exportacoes_normalizada_no_postgre:", str(e), end="\n\n")
        else:
            return None
        
        
        
        
    def cria_tabela_importacoes_normalizada_no_postgre(self):
        """Método para criar a tabela VIA no PostgreSQL. Esse método aciona um método genérico, o cria_tabela_no_postgre."""
        try:
            # Instância a classe:        
            interface_para_criar_tabelas_no_postgre = Criacoes_e_insercoes_no_postgre(self.usuario_postgre, \
                                                                          self.senha_postgre, \
                                                                          self.host, \
                                                                          self.banco_de_dados_postgre)
        except Exception as e:
            print("\n\nErro ao instanciar objeto no método cria_tabela_VIA_no_postgre em Criacoes_no_postgre:", str(e),end="\n\n")
            
        try:
            # Cria tabela importacoes_normalizada:
            nome_da_tabela = "importacoes_normalizada"
            variaveis_declaradas = "id_importacao serial PRIMARY KEY,\
                                    ano int,\
                                    produto text,\
                                    pais text,\
                                    bloco text,\
                                    via text,\
                                    kg_liquido bigint,\
                                    valor_fob bigint"
            interface_para_criar_tabelas_no_postgre.cria_tabela_no_postgre(nome_da_tabela, variaveis_declaradas)
        except Exception as e:
            print(f"\n\nErro ao criar tabela ({nome_da_tabela}) pelo método cria_tabela_importacoes_normalizada_no_postgre:", str(e), end="\n\n")
        else:
            return None
    
        
        
        
    def cria_trigger_e_function_exportacoes_normalizada_no_postgre(self):
        """Cria Trigger e Function exportacoes_normalizada no PostgreSQL"""
        try:            
            # Conecta ao PostgreSQL e habilita cursor:
            conexao, cursor = self.conecta_ao_banco_de_dados_do_postgre()
            
            # Variáveis para armazenar nomes da função e da trigger:
            nome_da_funcao = "normaliza_exportacoes"
            nome_da_trigger = "trg_normaliza_exportacoes"
            nome_da_tabela = "exportacoes"
            nome_da_tabela_normalizada = "exportacoes_normalizada"
            
            # Query para criar function
            comando_sql_cria_function = f"""CREATE OR REPLACE FUNCTION {nome_da_funcao}()
                                            RETURNS TRIGGER AS $$
                                                DECLARE
                                                    nome_produto  text;
                                                    nome_pais     text;
                                                    nome_bloco    text;
                                                    nome_via      text;
                                                BEGIN
                                                    nome_produto  := (SELECT n.no_ncm_por FROM ncm n WHERE n.co_ncm = NEW.co_ncm);
                                                    nome_pais     := (SELECT p.no_pais FROM pais p WHERE p.co_pais = NEW.co_pais);
                                                    nome_bloco    := (SELECT DISTINCT b.no_bloco FROM pais_bloco b WHERE b.co_pais = NEW.co_pais LIMIT 1);
                                                    nome_via      := (SELECT v.no_via FROM via v WHERE v.co_via = NEW.co_via);
                                                    INSERT INTO {nome_da_tabela_normalizada}(ANO, PRODUTO, PAIS, BLOCO, VIA, KG_LIQUIDO, VALOR_FOB)
                                                    VALUES
                                                    (NEW.co_ano, nome_produto, nome_pais, nome_bloco, nome_via, NEW.kg_liquido, NEW.vl_fob);
                                                    RETURN NEW;
                                                END;
                                            $$
                                            language 'plpgsql';"""
            
            # Query para criar trigger:
            comando_sql_cria_trigger = f"""CREATE TRIGGER {nome_da_trigger}
                                                AFTER INSERT ON {nome_da_tabela}
                                                    FOR EACH ROW 
                                                        EXECUTE PROCEDURE {nome_da_funcao}();"""
	
            
            # Cria Trigger e Function normaliza_exportacoes:
            cursor.execute(comando_sql_cria_function)
            cursor.execute(comando_sql_cria_trigger)
        except Exception as e:
            print(f"\n\nErro ao criar a função {nome_da_funcao} e a trigger {nome_da_trigger} pelo método cria_trigger_e_function_exportacoes_normalizada_no_postgre:", str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Função {nome_da_funcao} e trigger {nome_da_trigger} criadas com sucesso!")
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        finally:
            # Desconecta do PostgreSQL:
            self.desconecta_postgre(conexao, cursor)
            return None
        
        
        
        
    def cria_trigger_e_function_importacoes_normalizada_no_postgre(self):
        """Cria Trigger e Function exportacoes_normalizada no PostgreSQL"""
        try:
            # Conecta ao PostgreSQL e habilita cursor:
            conexao, cursor = self.conecta_ao_banco_de_dados_do_postgre()
            
            # Variáveis para armazenar nomes da função e da trigger:
            nome_da_funcao = "normaliza_importacoes"
            nome_da_trigger = "trg_normaliza_importacoes"
            nome_da_tabela = "importacoes"
            nome_da_tabela_normalizada = "importacoes_normalizada"
            
            # Query para criar function
            comando_sql_cria_function = f"""CREATE OR REPLACE FUNCTION {nome_da_funcao}()
                                            RETURNS TRIGGER AS $$
                                                DECLARE 
                                                    nome_produto  text;
                                                    nome_pais     text;
                                                    nome_bloco    text;
                                                    nome_via      text;
                                                BEGIN
                                                    nome_produto  := (SELECT n.no_ncm_por FROM ncm n WHERE n.co_ncm = NEW.co_ncm);
                                                    nome_pais     := (SELECT p.no_pais FROM pais p WHERE p.co_pais = NEW.co_pais);
                                                    nome_bloco    := (SELECT DISTINCT b.no_bloco FROM pais_bloco b WHERE b.co_pais = NEW.co_pais LIMIT 1);
                                                    nome_via      := (SELECT v.no_via FROM via v WHERE v.co_via = NEW.co_via);
                                                    INSERT INTO {nome_da_tabela_normalizada}(ANO, PRODUTO, PAIS, BLOCO, VIA, KG_LIQUIDO, VALOR_FOB)
                                                    VALUES
                                                    (NEW.co_ano, nome_produto, nome_pais, nome_bloco, nome_via, NEW.kg_liquido, NEW.vl_fob);
                                                    RETURN NEW;
                                                END;
                                            $$
                                            language 'plpgsql';"""
            
            # Query para criar trigger:
            comando_sql_cria_trigger = f"""CREATE TRIGGER {nome_da_trigger} 
                                                AFTER INSERT ON {nome_da_tabela}
                                                    FOR EACH ROW 
                                                        EXECUTE PROCEDURE {nome_da_funcao}();"""
	
            
            # Cria Trigger e Function normaliza_exportacoes:
            cursor.execute(comando_sql_cria_function)
            cursor.execute(comando_sql_cria_trigger)
        except Exception as e:
            print(f"\n\nErro ao criar a função {nome_da_funcao} e a trigger {nome_da_trigger} pelo método cria_trigger_e_function_para_importacoes_normalizada_no_postgre:", str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Função {nome_da_funcao} e trigger {nome_da_trigger} criadas com sucesso!")
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        finally:
            # Desconecta do PostgreSQL:
            self.desconecta_postgre(conexao, cursor)
            return None
    
        
        
        
    def deleta_trigger_e_function_exportacoes_normalizada_no_postgre(self):
        """Deleta Trigger e Function exportacoes_normalizada no PostgreSQL"""
        try:            
            # Conecta ao PostgreSQL e habilita cursor:
            conexao, cursor = self.conecta_ao_banco_de_dados_do_postgre()
            
            # Variáveis para armazenar nomes da função e da trigger:
            nome_da_funcao = "normaliza_exportacoes"
            nome_da_trigger = "trg_normaliza_exportacoes"
            nome_da_tabela = "exportacoes_normalizada"
            
            # Query para criar function
            comando_sql_deleta_function = f"DROP FUNCTION IF EXISTS {nome_da_funcao} CASCADE;"
            
            # Query para criar trigger:
            comando_sql_deleta_trigger = f"DROP TRIGGER IF EXISTS {nome_da_trigger} ON {nome_da_tabela} CASCADE;"
	
            
            # Cria Trigger e Function normaliza_exportacoes:
            cursor.execute(comando_sql_deleta_function)
            cursor.execute(comando_sql_deleta_trigger)
        except Exception as e:
            print(f"\n\nErro ao deletar a função {nome_da_funcao} e a trigger {nome_da_trigger} pelo método deleta_trigger_e_function_exportacoes_normalizada_no_postgre:", str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Função {nome_da_funcao} e trigger {nome_da_trigger} deletadas com sucesso!")
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        finally:
            # Desconecta do PostgreSQL:
            self.desconecta_postgre(conexao, cursor)
            return None
    
        
        
        
    def deleta_trigger_e_function_importacoes_normalizada_no_postgre(self):
        """Deleta Trigger e Function importacoes_normalizada no PostgreSQL"""
        try:            
            # Conecta ao PostgreSQL e habilita cursor:
            conexao, cursor = self.conecta_ao_banco_de_dados_do_postgre()
            
            # Variáveis para armazenar nomes da função e da trigger:
            nome_da_funcao = "normaliza_importacoes"
            nome_da_trigger = "trg_normaliza_importacoes"
            nome_da_tabela = "importacoes_normalizada"
            
            # Query para criar function
            comando_sql_deleta_function = f"DROP FUNCTION IF EXISTS {nome_da_funcao} CASCADE;"
            
            # Query para criar trigger:
            comando_sql_deleta_trigger = f"DROP TRIGGER IF EXISTS {nome_da_trigger} ON {nome_da_tabela} CASCADE;"
	
            
            # Cria Trigger e Function normaliza_exportacoes:
            cursor.execute(comando_sql_deleta_function)
            cursor.execute(comando_sql_deleta_trigger)
        except Exception as e:
            print(f"\n\nErro ao deletar a função {nome_da_funcao} e a trigger {nome_da_trigger} pelo método deleta_trigger_e_function_importacoes_normalizada_no_postgre:", str(e), end="\n\n")
        else:
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Função {nome_da_funcao} e trigger {nome_da_trigger} deletadas com sucesso!")
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        finally:
            # Desconecta do PostgreSQL:
            self.desconecta_postgre(conexao, cursor)
            return None
        
        
        

    def tratamento_na_tabela_NCM(self, caminho_parquet, caminho_parquet_formatado):
        """Método para realizar tratamento nos dados da tabela NCM.

        Args:
            caminho_parquet (string): caminho completo onde está o arquivo parquet sem formatação
            caminho_parquet_formatado (string): caminho completo onde ficará o arquivo parquet formatado
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_postgre:
            spark_postgre = self.instancia_spark_postgre()
            
            # Tratamento da tabela NCM:
            df_NCM = spark_postgre.read.format("parquet")\
                                       .option("header", "true")\
                                       .option("InferSchema", "true")\
                                       .option("encoding", "ISO-8859-1")\
                                       .load(fr"{caminho_parquet}\Arquivos_Parquet_NCM")

            # Converte os tipos dos atributos da tabela:
            df_NCM = df_NCM.select(df_NCM["CO_NCM"].cast("bigint"),\
                                   df_NCM["CO_UNID"].cast("bigint"),\
                                   df_NCM["CO_SH6"].cast("bigint"),\
                                   df_NCM["CO_PPE"].cast("bigint"),\
                                   df_NCM["CO_PPI"].cast("bigint"),\
                                   df_NCM["CO_FAT_AGREG"].cast("bigint"),\
                                   df_NCM["CO_CUCI_ITEM"].cast("bigint"),\
                                   df_NCM["CO_CGCE_N3"].cast("bigint"),\
                                   df_NCM["CO_SIIT"].cast("bigint"),\
                                   df_NCM["CO_ISIC_CLASSE"].cast("bigint"),\
                                   df_NCM["CO_EXP_SUBSET"].cast("bigint"),\
                                   df_NCM["NO_NCM_POR"],\
                                   df_NCM["NO_NCM_ESP"],\
                                   df_NCM["NO_NCM_ING"])
        except Exception as e:
            print("\n\nErro ao tratar a tabela NCM pelo método tratamento_na_tabela_NCM da classe Interface_spark:", str(e), end="\n\n")
        else:
            # Tabela dimensão convertida em parquet:
            df_NCM.write.parquet(path=f"{caminho_parquet_formatado}\Arquivos_Parquet_NCM", mode="overwrite")
            return None
            
    
    
    
    def tratamento_na_tabela_PAIS(self, caminho_parquet, caminho_parquet_formatado):
        """Método para realizar tratamento nos dados da tabela PAIS.

        Args:
            caminho_parquet (string): caminho completo onde está o arquivo parquet sem formatação
            caminho_parquet_formatado (string): caminho completo onde ficará o arquivo parquet formatado
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_postgre:
            spark_postgre = self.instancia_spark_postgre()
            
            # Tratamento da tabela PAIS:
            df_PAIS = spark_postgre.read.format("parquet")\
                                        .option("header", "true")\
                                        .option("InferSchema", "true")\
                                        .option("encoding", "ISO-8859-1")\
                                        .load(fr"{caminho_parquet}\Arquivos_Parquet_PAIS")
                                       
            # Converte os tipos dos atributos da tabela:
            df_PAIS = df_PAIS.select(df_PAIS["CO_PAIS"].cast("bigint"),\
                                     df_PAIS["CO_PAIS_ISON3"].cast("bigint"),\
                                     df_PAIS["CO_PAIS_ISOA3"],\
                                     df_PAIS["NO_PAIS"],\
                                     df_PAIS["NO_PAIS_ING"],\
                                     df_PAIS["NO_PAIS_ESP"])           
            
        except Exception as e:
            print("\n\nErro ao tratar a tabela NCM pelo método tratamento_na_tabela_PAIS da classe Interface_spark:", str(e), end="\n\n")
        else:
            # Tabela dimensão convertida em parquet:
            df_PAIS.write.parquet(path=f"{caminho_parquet_formatado}\Arquivos_Parquet_PAIS", mode="overwrite")
            return None




    def insere_parquet_no_postgre(self, parquet_para_postgre_sem_tratamento, parquet_para_postgre_com_tratamentos, banco_de_dados_postgre):
        """Método para inserir dados nas tabelas do PostgreSQL. Esse método um método genérico, o insere_parquet_no_postgre 

        Args:
            parquet_para_postgre_sem_tratamento (list): lista com os nomes dos arquivos NCM e PAIS
            parquet_para_postgre_com_tratamentos (list): lista com os nomes dos demais arquivos do projeto
            banco_de_dados_postgre (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
        """
        
        try:
            # Trata as tabelas NCM (Motivo: Erro no tipo de variável. Todos os dados desses arquivos vieram como string).
            self.tratamento_na_tabela_NCM(parquet_para_postgre_sem_tratamento, parquet_para_postgre_com_tratamentos)
        except Exception as e:
            print(f"\n\nErro ao tratar dados da tabela NCM pelo método insere_dados_no_postgre:", str(e), end="\n\n")
        
        try:
            # Trata as tabelas PAIS (Mesmo motivo da tabela NCM).
            self.tratamento_na_tabela_PAIS(parquet_para_postgre_sem_tratamento, parquet_para_postgre_com_tratamentos)
        except Exception as e:
            print(f"\n\nErro ao tratar dados da tabela PAIS pelo método insere_dados_no_postgre:", str(e), end="\n\n")
        
        
        try:
            # Lista com os nomes das tabelas que precisaram de tratamento para serem inseridos no PostgreSQL:
            tabelas_que_precisam_de_tratamento = ["NCM", "PAIS"]
            
            # Insere dados no PostgreSQL:
            self.insere_dados_no_postgre(tabelas_que_precisam_de_tratamento, \
                                         banco_de_dados_postgre, \
                                         parquet_para_postgre_com_tratamentos) # Insere os dados nas tabelas NCM e PAIS
            
        except Exception as e:
            print(f"\n\nErro ao inserir dados na tabela ({tabelas_que_precisam_de_tratamento}) pelo método insere_dados_no_postgre:\n", str(e), end="\n\n")  
            # Mensagem de que a tabela foi abastecida com sucesso aparece pelo método insere_parquet_no_postgre da classe Interface_cassandra_nuvem

        try:
            # Lista com os nomes das tabelas que NÃO precisaram de tratamento para serem inseridos no PostgreSQL:
            lista_com_nomes_das_tabelas = ["PAIS_BLOCO", "UF", "UF_MUN", "URF", "VIA", "EXPORTACOES", "IMPORTACOES"]
            self.insere_dados_no_postgre(lista_com_nomes_das_tabelas, \
                                           banco_de_dados_postgre, \
                                           parquet_para_postgre_sem_tratamento) # Insere os dados nas tabelas fato e algumas dimensões
        except Exception as e:
            print(f"\n\nErro ao inserir dados na tabela ({lista_com_nomes_das_tabelas}) pelo método insere_dados_no_postgre:\n", str(e), end="\n\n")
            # Mensagem de que a tabela foi abastecida com sucesso aparece pelo método insere_parquet_no_postgre da classe Interface_cassandra_nuvem

        return None
    



    def extrai_dados_do_postgre(self, banco_de_dados_postgre, parquet_para_cassandra):
        """Método para extrair dados nas tabelas do PostgreSQL. Esse método um método genérico, o seleciona_dados_do_postgre_e_converte_para_parquet 

        Args:
            banco_de_dados_postgre (string): nome do banco de dados que possui as tabelas a serem abastecidas com dados
            parquet_para_cassandra (string): caminho completo onde ficará o arquivo parquet após extração de dados do postgreSQL
        """
        try:
            lista_de_nomes_das_tabelas_normalizadas = ["importacoes_normalizada",  "exportacoes_normalizada"]
            self.seleciona_dados_do_postgre_e_converte_para_parquet(banco_de_dados_postgre, \
                                                                    lista_de_nomes_das_tabelas_normalizadas, \
                                                                    parquet_para_cassandra)
        except Exception as e:
            print(f"\n\nErro ao extrair dados na tabela ({lista_de_nomes_das_tabelas_normalizadas}) pelo método extrai_dados_do_postgre:\n", str(e), end="\n\n")
            
        return None