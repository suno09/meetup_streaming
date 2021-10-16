import logging

log_format = '%(asctime)s %(levelname)-5s %(name)s: %(message)s'
log_datefmt = '%y-%m-%d %H:%M:%S'


def init_log(log_name: str, level: int = logging.INFO) -> logging.Logger:
    file_handler = logging.FileHandler(filename=f'{log_name}.log', mode='a')
    console_handler = logging.StreamHandler()
    logging.basicConfig(
        format=log_format,
        level=level,
        datefmt=log_datefmt,
        handlers=[file_handler, console_handler])
    log = logging.getLogger(log_name)

    return log
