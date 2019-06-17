import utils
import pandas as pd
import os
import logging
from datetime import date
import getpass
import errors as error
from datetime import datetime


def extract_source_data(extraction_id, enc):
    """
    Extacts source and source database connection details from
    extraction_details and connection_detalis tables respectively
    and extracts source data and store it in provided delimited file.
    :param extraction_id: source extraction_id
    :param enc : AESCipher object
    """
    try:
        logging.getLogger().setLevel(logging.INFO)
        connection = utils.get_oracle_connection_obj()

        config = utils.get_config_parser()
        extraction_query = get_select_query(config.get('Database', 'extraction_columnlist'),
                                            config.get('Database', 'tablespace'),
                                            config.get('Database', 'extraction_tablename'),
                                            "EXTRACTION_ID=" + extraction_id.__str__())

        logging.info("Extraction_details query = "+extraction_query)

        extraction_details_df = pd.read_sql(extraction_query, con=connection)

        # Get connection details for given connection_id corresponding to extraction id
        connection_id = extraction_details_df.iloc[0]['CONNECTION_ID']
        connection_query = get_select_query(config.get('Database', 'connection_columnlist'),
                                            config.get('Database', 'tablespace'),
                                            config.get('Database', 'connection_tablename'),
                                            "CONNECTION_ID=" + connection_id.__str__())

        logging.info("Connection_details query = " + connection_query)
        connection_details_df = pd.read_sql(connection_query, con=connection)

        # create connection object for extraction table db

        password_file = connection_details_df.iloc[0]['PASSWORD_FILE']
        password = enc.decrypt_file(password_file)
        connection_src_extraction = utils.get_oracle_connection_obj(connection_details_df.iloc[0]['USERNAME'], password,
                                                                    connection_details_df.iloc[0]['CONNECTION_STRING'])

        # Get current incremental column's max value from source data
        max_value = get_max(connection_src_extraction, extraction_details_df)

        source_extraction_query = get_select_query(extraction_details_df.iloc[0]['COLUMN_LIST'],
                                                   extraction_details_df.iloc[0]['DATABASE_NAME'],
                                                   extraction_details_df.iloc[0]['TABLE_NAME'],
                                                   extraction_details_df.iloc[0]['INCREMENTAL_COLUMN'] + " > "
                                                   + extraction_details_df.iloc[0]['INCREMENTAL_VALUE'].__str__()
                                                   + " AND " + extraction_details_df.iloc[0]['INCREMENTAL_COLUMN']
                                                   + " <= " + max_value.__str__())

        logging.info("Source extraction query = " + source_extraction_query)
        source_extraction_df = pd.read_sql(source_extraction_query, con=connection_src_extraction)

        logging.info("copying " + extraction_details_df.iloc[0]['FILENAME'] + " file to "
                     + config.get('Directory', 'inbound') + " path")

        source_extraction_df.to_csv(config.get('Directory', 'inbound') +
                                    extraction_details_df.iloc[0]['FILENAME'],
                                    index=False,
                                    sep=extraction_details_df.iloc[0]['FILE_DELIMITER'])

        logging.info("Updating File_details oracle table")
        today = date.today().strftime("%Y-%m-%d")
        insert_file_details(connection, config.get('Database', 'tablespace'),
                            config.get('Database', 'file_details_tablename'),
                            f"0,'{connection_details_df.iloc[0]['SYSTEM']}', "
                            f"'{extraction_details_df.iloc[0]['DATABASE_NAME']}', "
                            f"'{extraction_details_df.iloc[0]['TABLE_NAME']}', "
                            f"'{extraction_details_df.iloc[0]['FILENAME']}', "
                            f"TO_DATE('{datetime.now().strftime('%Y%m%d%H%M%S')}','yyyymmddhh24miss'),"
                            f"{source_extraction_df.shape[0]}, 'False', 'False', 'None', date'{today}',"
                            f" '{getpass.getuser()}',"
                            f" date'{today}', '{getpass.getuser()}'")

        logging.info("Successfully extracted data for " + extraction_details_df.iloc[0]['TABLE_NAME'] + " table.")

        logging.info("Updating incremental column's value in extraction_details")

        # #####Uncomment below code to update incremental_value column in excraction_details table  #######
        # update_extraction_details(connection, config.get('Database', 'tablespace'),
        #                  config.get('Database', 'extraction_tablename'),
        #                  extraction_id, max_value)

        logging.info("####################### JOB COMPLETED SUCCESSFULLY ######################")

    except Exception as ex:
        # err, = ex.args
        error.Errors(ex).errorrun()
        # logging.error("Error code    = ", err.code)
        # logging.error("Error Message = ", err.message)
        connection.close()
        connection_src_extraction.close()
        os._exit(1)
    connection.close()
    connection_src_extraction.close()
    return 0


def get_max(connection_extraction,extraction_details_df):
    """
    Get the maximum value of incremental column from source table
    :param connection_extraction: Connection object
    :param extraction_details_df: extraction dataframe
    :return: max value of incremental column
    """
    max_query = "SELECT max(" + extraction_details_df.iloc[0]['INCREMENTAL_COLUMN'] + ") as MAX_VALUE FROM "\
                + extraction_details_df.iloc[0]['DATABASE_NAME'] + "." \
                + extraction_details_df.iloc[0]['TABLE_NAME']
    logging.info("Maximum of incremental column = "+max_query)
    return pd.read_sql(max_query, con=connection_extraction).iloc[0]['MAX_VALUE']


def update_extraction_details(connection, database_name, tablename, extraction_id, max_value):
    """
    Update extraction_details oracle table's below columns:
        1. incremental_value
        2. modified_by
        3.modified_date
    :param connection: connection object
    :param database_name: db name
    :param tablename: table name
    :param extraction_id: id for which details has to be updated
    :param max_value: incremental columns value
    """
    today = date.today().strftime("%Y-%m-%d")
    update_query = f"UPDATE {database_name}.{tablename} SET INCREMENTAL_VALUE ={max_value.__str__()}, " \
        f"MODIFIED_DATE=date'{today}', MODIFIED_BY='{getpass.getuser()}' " \
        f"WHERE EXTRACTION_ID={extraction_id.__str__()}"

    logging.info("Extraction_details update query = " + update_query)
    cursor = connection.cursor()
    cursor.execute(update_query)
    connection.commit()
    cursor.close()
    return


def insert_file_details(connection, database_name, tablename, column_list):
    insert_query = f"INSERT INTO {database_name}.{tablename} VALUES({column_list})"
    logging.info("File_details insert query = " + insert_query)
    cursor = connection.cursor()
    cursor.execute(insert_query)
    connection.commit()
    cursor.close()
    return


def get_select_query(column_list, database_name, table_name, where_condition):
    query = f"SELECT {column_list} FROM {database_name}.{table_name} WHERE {where_condition}"
    return query








