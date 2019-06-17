import time
from shutil import copyfile
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import logging
import errors as error
import utils

config = utils.get_config_parser()
logging.getLogger().setLevel(logging.INFO)


class FileHandler(FileSystemEventHandler):

    def on_created(self, event):

        logging.info(f'event type: {event.event_type}  path : {event.src_path}')
        try:

            copyfile(event.src_path, config.get('Directory', 'temp') + os.path.basename(event.src_path))
            connection = utils.get_oracle_connection_obj()
            update_file_details(True, connection, config.get('Database', 'tablespace'),
                                config.get('Database', 'file_details_tablename'), os.path.basename(event.src_path))
        except Exception as ex:
            error.Errors(ex).errorrun()
            err, = ex.args
            copyfile(event.src_path, config.get('Directory', 'error') + os.path.basename(event.src_path))
            update_file_details(False, connection, config.get('Database', 'tablespace'),
                                config.get('Database', 'file_details_tablename'), os.path.basename(event.src_path),
                                f"{err.code}:{err.message}")


def update_file_details(success, connection, database_name, table_name, filename, error_msg=None):
    if success:
        update_query = f"UPDATE {database_name}.{table_name} SET PUSHED_TO_FS='True' WHERE FILENAME='{filename}'"
    else:
        update_query = f"UPDATE {database_name}.{table_name} SET PUSHED_TO_ERR_DIR='True', ERROR='{error_msg}' " \
            f"WHERE FILENAME='{filename}'"

    logging.info("file_details update query = " + update_query)
    cursor = connection.cursor()
    cursor.execute(update_query)
    connection.commit()
    cursor.close()
    return


if __name__ == "__main__":
    path = config.get('Directory', 'outbound')
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(5000)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

