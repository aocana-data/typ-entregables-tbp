import os
import os.path
import time
import datetime
import logging
import json
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from io import StringIO, BytesIO


# Configuración del log
logging.basicConfig(
    filename="error.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

class Boto3Connect:
    def __init__(self, log: bool = False, profile_name='development'):
        """
        Inicializa los valores de la clase Boto3Connect
        :param log: Indica si se deben imprimir mensajes de log (opcional, por defecto False)
        :param profile_name: Nombre del perfil a utilizar en la conexión a boto3 (opcional, por defecto 'development')
        """
        self.log = log
        self.profile_name = profile_name

    def restore_connection(self):
        """
        Restaura la conexión a boto3 en caso de haber terminado la sesión
        :return: int igual a 0 si la conexión es restaurada correctamente
        """
        q_status = 'SELECT 1 FROM "information_schema"."schemata" LIMIT 1'
        boto3.setup_default_session(profile_name=self.profile_name)
        client = boto3.client('athena', region_name='us-east-1')
        try:
            client.start_query_execution(
                QueryString=q_status,
                QueryExecutionContext={"Database": "caba-piba-raw-zone-db"},
                ResultConfiguration={
                    "OutputLocation": 's3://development-athena-queries-workgroups-piba-dl/modeler/athena/',
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                }
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "ExpiredTokenException":
                if self.log:
                    print("Restaurando sesión")
                os.system('aws-azure-login --no-prompt -p development')
                if self.log:
                    print("Sesión restaurada")
                    # Creación del diccionario con los datos del error
                    error_dict = {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "timestamp": str(datetime.datetime.now())
                    }
                    # Conversión del diccionario a formato JSON
                    error_json = json.dumps(error_dict)
                    # Registro del error en el log
                    logging.error(error_json)

        return 0

    def run_query(self, query):
        """
        Ejecuta una sentencia DDL o DML en boto3
        :param query: Sentencia DDL o DML a ejecutar
        :return: DataFrame con los resultados en caso de ser una sentencia SELECT, de lo contrario devuelve el estado de la ejecución
        """
        try:
            self.restore_connection()

            # Verifica que la query contenga una palabra permitida como primer elemento
            tipos_permitidos = ['with', 'select', 'insert', 'update', 'detele', 'create', 'drop']
            tipo = query.split()[0].lower()
            if tipo not in tipos_permitidos:
                raise SyntaxError(
                    "La query debe contener como primer palabra una de las siguientes: {}".format(tipos_permitidos))

            # Configura la sesión de Boto3
            boto3.setup_default_session(profile_name=self.profile_name)
            client = boto3.client('athena', region_name='us-east-1')

            # Inicia la ejecución de la query
            response = client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": "caba-piba-raw-zone-db"},
                ResultConfiguration={
                    "OutputLocation": 's3://development-athena-queries-workgroups-piba-dl/modeler/athena/',
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                }
            )
            query_execution_id = response["QueryExecutionId"]

            # Espera hasta que la ejecución de la query haya finalizado
            query_state = client.get_query_execution(QueryExecutionId=query_execution_id)
            while query_state["QueryExecution"]["Status"]["State"] != "SUCCEEDED":
                if query_state["QueryExecution"]["Status"]["State"] in ["QUEUED", "RUNNING"]:
                    time.sleep(1)
                    query_state = client.get_query_execution(QueryExecutionId=query_execution_id)
                else:
                    break

            # Devuelve los resultados o un mensaje de error
            if query_state["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
                if tipo == 'select' or tipo == 'with':
                    s3path = query_state["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
                    name = s3path.split("/")[-1]

                    df_response = boto3.client("s3").get_object(
                        Bucket='development-athena-queries-workgroups-piba-dl',
                        Key='modeler/athena/' + name
                    )
                    dataframe = pd.read_csv(df_response.get("Body"))
                    return dataframe
                else:
                    return query_state["QueryExecution"]["Status"]["State"]
            else:
                raise Exception("Error al realizar query en metodo run_query\n\nQuery:\n{}".format(query))

        except SyntaxError as ex:
            print(ex)
            raise

        except Exception as ex:
            print('\nQuery con error:\n' + query)

    def save_df_s3(self, df, format='parquet', filename='test', path_key='modeler/athena/',
                   bucket='development-athena-queries-workgroups-piba-dl'):
        """
        Guarda un dataframe en un bucket S3 en el formato especificado (parquet o csv).

        Parámetros
        df : pandas.DataFrame
           Dataframe a guardar.
        format : str, opcional
           Formato en el que se guardará el dataframe, por defecto 'parquet'.
        filename : str, opcional
           Nombre del archivo a guardar, por defecto 'test'.
        path_key : str, opcional
           Carpeta dentro del bucket donde se guardará el dataframe, por defecto 'modeler/athena/'.
        bucket : str, opcional
           Nombre del bucket donde se guardará el dataframe, por defecto 'development-athena-queries-workgroups-piba-dl'.

        Retorno
        None
        """
        self.restore_connection()

        boto3.setup_default_session(profile_name=self.profile_name)
        filename = filename + '.' + format

        s3 = boto3.client("s3")

        if format == 'parquet':
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            s3.put_object(Bucket=bucket, Key=path_key + filename, Body=out_buffer.getvalue())

        elif format == 'csv':
            out_buffer = StringIO()
            df.to_csv(out_buffer)
            s3.put_object(Bucket=bucket, Key=path_key + filename, Body=out_buffer.getvalue())

    def delete_file_s3(self, format='parquet', filename='test', path_key='modeler/athena/',
                   bucket='development-athena-queries-workgroups-piba-dl'):
        """
        Elimina un archivo en un bucket S3.

        Parámetros
        format : str, opcional
            Formato del archivo a eliminar, por defecto 'parquet'.
        filename : str, opcional
            Nombre del archivo a eliminar, por defecto 'test'.
        path_key : str, opcional
            Carpeta dentro del bucket donde se encuentra el archivo a eliminar, por defecto 'modeler/athena/'.
        bucket : str, opcional
            Nombre del bucket donde se encuentra el archivo a eliminar, por defecto 'development-athena-queries-workgroups-piba-dl'.

        Retorno
        None
        """
        self.restore_connection()

        s3 = boto3.client("s3")
        boto3.setup_default_session(profile_name=self.profile_name)

        filename = filename + '.' + format

        result = s3.delete_object(Bucket=bucket, Key=path_key + filename)
        if self.log:
            print(result)

    def create_table_from_df(self, df, schema='caba-piba-staging-zone-db', table='borrar_tabla'):
        """
        Crea una tabla en Athena a partir de un dataframe.

        Parámetros
        df : pandas.DataFrame
          Dataframe a partir del cual se creará la tabla.
        schema : str, opcional
          Nombre del esquema en Athena donde se creará la tabla, por defecto 'caba-piba-staging-zone-db'.
        table : str, opcional
          Nombre de la tabla a crear, por defecto 'borrar_tabla'.

        Retorno
        None
        """
        nombres_columnas = df.columns.tolist()
        q_create = 'CREATE TABLE "' + schema + '"."' + table + '" AS SELECT '
        for i in range(len(nombres_columnas)):
            q_create += 'CAST(\'\' AS VARCHAR) ' + nombres_columnas[i]
            if i != (len(nombres_columnas) - 1):
                q_create += ', '
        return self.run_query(q_create)

    def drop_table(self, schema='caba-piba-staging-zone-db', table='borrar_tabla'):
        """
        Elimina una tabla específica en la base de datos seleccionada.

        Parámetros:
        - schema (str): Nombre del esquema al que pertenece la tabla. Por defecto, el valor es "caba-piba-staging-zone-db".
        - table (str): Nombre de la tabla a eliminar. Por defecto, el valor es "borrar_tabla".

        Retorno:
        El resultado de la ejecución del query para la eliminación de la tabla.
        """
        q_drop = 'DROP TABLE IF EXISTS `' + schema + '`.`' + table + '`'
        return self.run_query(q_drop)

    def get_n_insert_from_df(self, df, desde=0, n=100):
        """
        Genera la cadena de texto para la inserción de datos en una tabla de una cantidad determinada de filas a partir de una posición específica en un dataframe.

        Parámetros:
        - df (pandas.DataFrame): Dataframe con los datos a insertar en la tabla.
        - desde (int): Posición desde la cual se inicia la generación de la cadena para la inserción de datos en la tabla. Por defecto, es 0.
        - n (int): Cantidad de filas a incluir en la cadena de texto para la inserción de datos. Por defecto, el valor es 100.

        Retorno:
        La cadena de texto resultante de la generación para la inserción de datos en la tabla.
        """
        sql_texts = ''
        n_aux = 0
        for index, row in df.iterrows():
            n_aux += 1
            if index < desde:
                continue
            else:
                sql_texts += str(tuple(row.values))
                if index != (len(df.index)-1) and n_aux <= (desde + n - 1):
                    sql_texts += ', '
                else:
                    return sql_texts

    def insert_from_dataframe_to_athena(self, df, schema, table,  n=100):
        """
        Inserta los datos de un DataFrame en una tabla de Athena.

        Este método permite insertar los datos de un DataFrame en una tabla de Athena,
        con un control opcional sobre la cantidad de filas insertadas en una sola ejecución de consulta SQL.

        Parámetros:
        df (pandas.DataFrame): El DataFrame que contiene los datos a insertar.
        schema (str): El nombre del esquema en Athena en el que se encuentra la tabla.
        table (str): El nombre de la tabla en Athena en la que se insertarán los datos.
        n (int, opcional): El número de filas que se insertarán en una sola ejecución de consulta SQL. Por defecto, n=100.

        Retorno:
        None

        """
        target_table = '"' + schema + '"."' + table + '"'

        sql_texts = 'INSERT INTO '+target_table+' ('+ str(', '.join(df.columns))+ ') VALUES '

        partes = len(df.index) // n
        ultimo_n = len(df.index) % n

        for i in range(partes):
            q_insert = sql_texts + self.get_n_insert_from_df(df, i*n, n)
            self.run_query(q_insert)

        if ultimo_n > 0:
            q_insert = sql_texts + self.get_n_insert_from_df(df, partes * n, ultimo_n)
            self.run_query(q_insert)