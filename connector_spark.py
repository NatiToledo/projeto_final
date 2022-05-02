# Importa módulos:
try:
    from pyspark.sql import SparkSession
except Exception as e:
    print("Há algum problema na importação de módulos no arquivo connector_spark:".format(str(e)))
    


# Cria classe para conectar e processar dados armazenados no Cassandra via python:
class Interface_spark():        
        
    
    def instancia_spark_postgre(self):
        """
        Instancia a classe Interface_spark para processar dados no SGBD PostgreSQL.
        
        Returns:
            spark_postgre: objeto da classe Interface_spark
        """
        try:
            # instancea classe:
            # Obs.: linha 20 -> aumenta memória de processamento para gravar grandes volumes de dados em parquet. 
            spark_postgre = SparkSession.builder.appName("Projeto_Final_Importacao_Exportacao_postgre")\
                                                .config("spark.jars", r"/C:\postgresql-42.3.1.jar")\
                                                .config("spark.driver.memory", "50g")\
                                                .getOrCreate()
        except Exception as e:
            print("\n\nErro no método instancia_spark_postgre da classe Interface_spark:", str(e), end="\n\n")
        else:
            return spark_postgre
        
        
        
    
    
    def instancia_spark_cassandra(self, ip_externo):
        """
        Instancia a classe Interface_spark para processar dados no SGBD Cassandra.
        
        Args:
            ip_externo (string): ip externo do Cluster Cassandra na nuvem
        
        Returns:
            spark_cassandra: objeto da classe Interface_spark
        """
        try:            
            # Instanciando a sessão Spark com os parâmetros de conexão com a instância do Cassandra na Google Cloud
            spark_cassandra = SparkSession.builder.appName("Projeto_Final_Importacao_Exportacao_cassandra")\
                                                  .config("spark.cassandra.connection.host", ip_externo)\
                                                  .config("spark.cassandra.connection.port","9042")\
                                                  .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0")\
                                                  .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
                                                  .config("spark.driver.memory", "50g")\
                                                  .getOrCreate()
            # .config("spark.sql.catalog.desafio.spark.cassandra.connection.host", ip_externo)\
        except Exception as e:
            print("\n\nErro no método instancia_spark da classe Interface_spark:", str(e), end="\n\n")
        else:
            return spark_cassandra
        



    def le_um_arquivo_e_converte_para_parquet(self, caminho_arquivo, lista_com_nome_de_arquivos, \
                                              caminho_parquet, extensao_arquivo="csv", separador=";", \
                                              extensao_arquivo_convertido="parquet"):
        """Método para ler os arquivos cujos dados são dimensão (ou seja, um desses arquivos pode ser um dicionário dos produtos)

        Args:
            caminho_arquivo (string): caminho completo de onde está seu arquivo
            lista_com_nome_de_arquivos (string): lista com os nomes dos arquivos
            extensao_arquivo (string): extensão do arquivo (ex.: csv)
            caminho_parquet (string): caminho completo de onde ficará o arquivo parquet
            separador (string): separador utilizado entre as colunas do arquivo (ex. comuns: ';' e ',')
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_postgre:
            spark_postgre = self.instancia_spark_postgre()
            
            for arquivo in lista_com_nome_de_arquivos: 
                # Lê arquivo csv:
                arquivo_lido = spark_postgre.read.format(extensao_arquivo)\
                                                 .option("header", "true")\
                                                 .option("delimiter", separador)\
                                                 .option("InferSchema", "true")\
                                                 .option("encoding", "ISO-8859-1")\
                                                 .load(f"{caminho_arquivo}/{arquivo}.{extensao_arquivo}").limit(100)
                                                                
                # Converte arquivo de csv para parquet:
                arquivo_lido.write.format(extensao_arquivo_convertido)\
                                  .mode("overwrite")\
                                  .save(f"{caminho_parquet}/Arquivos_{extensao_arquivo_convertido}_{arquivo}")
                
                
                print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
                print(f"Arquivo {arquivo} convertido para parquet com sucesso!")
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
        except Exception as e:
            print("\n\nErro no método le_um_arquivo_e_converte_para_parquet da classe Interface_spark:", str(e), end="\n")
            print(f"Erro ao converter o arquivo {arquivo} para parquet:", str(e), end="\n\n")
        else:
            return None
        
            
            

    def le_muitos_arquivos_e_converte_para_parquet(self, caminho_arquivo, lista_com_nome_de_arquivos, caminho_parquet, \
                                                   arquivos_usados_para_tabelas_fatos, extensao_arquivo="csv", separador=";", \
                                                   extensao_arquivo_convertido="parquet"):
        """Método para ler os arquivos de exportação e importação

        Args:
            caminho_arquivo (string): caminho completo de onde está seu arquivo \n
            lista_com_nome_de_arquivos (string): lista com os nomes dos arquivos \n
            extensao_arquivo (string): extensão do arquivo (ex.: csv) \n
            caminho_parquet (string): caminho completo onde ficará o arquivo parquet \n
            arquivos_usados_para_tabelas_fatos (string): nome em comum dos arquivos (ex.: sigla IMP é importacoes) \n
            separador (string): separador utilizado entre as colunas do arquivo (ex. comuns: ';' e ',')
        """
        try:
            # Acessa uma sessão no PySpark ao criar o objeto spark_postgre:
            spark_postgre = self.instancia_spark_postgre()
                        
            # Lê arquivos csv's:
            tam_lista = len(lista_com_nome_de_arquivos)
            for i in range(tam_lista):
                arquivo_lido = spark_postgre.read.format(extensao_arquivo)\
                                                 .option("header", "true")\
                                                 .option("delimiter", separador)\
                                                 .option("InferSchema", "true")\
                                                 .option("encoding", "ISO-8859-1")\
                                                 .load(f"{caminho_arquivo}/{lista_com_nome_de_arquivos[i]}.{extensao_arquivo}").limit(100)
                # Unindo datasets:
                if i < 1:
                    arquivo_completo = arquivo_lido
                else:
                    arquivo_completo = arquivo_completo.unionAll(arquivo_lido)
        except Exception as e:
            print("\n\nErro no método le_muitos_arquivos_e_converte_para_parquet da classe Interface_spark:", str(e), end="\n")
        else:
            # Converte arquivos para parquet:
            arquivo_completo.write.format(extensao_arquivo_convertido)\
                                  .mode("overwrite")\
                                  .save(f"{caminho_parquet}/Arquivos_{extensao_arquivo_convertido}_{arquivos_usados_para_tabelas_fatos}")
            
            print("\n\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Arquivo {arquivos_usados_para_tabelas_fatos} convertido para parquet com sucesso!")
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n")
            return None

        
        
        
        
class conversao_arquivos (Interface_spark):
    
    
    def conversao_de_um_csv_para_parquet(self, caminho_arquivo, caminho_parquet):
        lista_de_nomes_dos_arquivos_csv = ["NCM", "PAIS", "PAIS_BLOCO", "UF", "UF_MUN", "URF", "VIA"]
        self.le_um_arquivo_e_converte_para_parquet(caminho_arquivo, lista_de_nomes_dos_arquivos_csv, caminho_parquet)
        
        return None
    
    
    
    def conversao_de_muitos_arquivos_exportacoes_para_parquet(self, caminho_arquivo, caminho_parquet):
        fonte_fato_exp = "EXPORTACOES"
        lista_de_nomes_dos_arquivos_csv = ["EXP_1997", "EXP_1998", "EXP_1999", "EXP_2000", "EXP_2001",
                                           "EXP_2002", "EXP_2003", "EXP_2004", "EXP_2005", "EXP_2006",
                                           "EXP_2007", "EXP_2008", "EXP_2009", "EXP_2010", "EXP_2011",
                                           "EXP_2012", "EXP_2013", "EXP_2014", "EXP_2015", "EXP_2016",
                                           "EXP_2017", "EXP_2018", "EXP_2019", "EXP_2020", "EXP_2021"]
        self.le_muitos_arquivos_e_converte_para_parquet(caminho_arquivo, lista_de_nomes_dos_arquivos_csv, \
                                                        caminho_parquet, fonte_fato_exp)
    
        return None
        
        
        

    def conversao_de_muitos_arquivos_importacoes_para_parquet(self, caminho_arquivo, caminho_parquet):
        fonte_fato_imp = "IMPORTACOES"
        lista_de_nomes_dos_arquivos_csv = ["IMP_1997", "IMP_1998", "IMP_1999", "IMP_2000", "IMP_2001",
                                           "IMP_2002", "IMP_2003", "IMP_2004", "IMP_2005", "IMP_2006",
                                           "IMP_2007", "IMP_2008", "IMP_2009", "IMP_2010", "IMP_2011",
                                           "IMP_2012", "IMP_2013", "IMP_2014", "IMP_2015", "IMP_2016", 
                                           "IMP_2017", "IMP_2018", "IMP_2019", "IMP_2020", "IMP_2021"]
        self.le_muitos_arquivos_e_converte_para_parquet(caminho_arquivo, lista_de_nomes_dos_arquivos_csv, \
                                                        caminho_parquet, fonte_fato_imp)
    
        return None