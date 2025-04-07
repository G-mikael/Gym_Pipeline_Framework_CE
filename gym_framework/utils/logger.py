import logging

def get_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        # Console handler
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

        # File handler
        file_handler = logging.FileHandler("pipeline.log", mode='w', encoding='utf-8')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        logger.setLevel(logging.DEBUG)

    return logger