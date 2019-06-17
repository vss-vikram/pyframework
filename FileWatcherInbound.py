import time
from shutil import copyfile
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import utils
import os
import logging
import errors as error

config = utils.get_config_parser()
logging.getLogger().setLevel(logging.INFO)


class FileHandler(FileSystemEventHandler):

    def on_created(self, event):

        logging.info(f'event type: {event.event_type}  path : {event.src_path}')
        try:
            copyfile(event.src_path, config.get('Directory', 'outbound') + os.path.basename(event.src_path))
        except Exception as ex:
            error.Errors(ex).errorrun()
            copyfile(event.src_path, config.get('Directory', 'error') + os.path.basename(event.src_path))


if __name__ == "__main__":
    path = config.get('Directory', 'inbound')
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

