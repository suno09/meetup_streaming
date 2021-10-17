import logging

log_format = '%(asctime)s %(levelname)-5s %(name)s: %(message)s'
log_datefmt = '%y-%m-%d %H:%M:%S'


def init_log(log_name: str,
             level: int = logging.INFO,
             formatting: str = log_format,
             datefmt: str = log_datefmt,
             save_to_file: bool = True,
             show_in_console: bool = True) -> logging.Logger:
    handlers = []
    if save_to_file:
        handlers.append(
            logging.FileHandler(filename=f'{log_name}.log', mode='a'))
    if show_in_console:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        format=formatting,
        level=level,
        datefmt=datefmt,
        handlers=handlers)
    log = logging.getLogger(log_name)

    return log
