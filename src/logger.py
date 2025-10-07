import os
import logging
from colorama import Fore, Style, init

init(autoreset=True)


class Logger:
    _loggers = {}
    _colors = [
        Fore.BLUE,
        Fore.GREEN,
        Fore.CYAN,
        Fore.MAGENTA,
        Fore.YELLOW,
        Fore.RED,
        Fore.WHITE,
    ]

    @staticmethod
    def _color_for_name(name: str) -> str:
        idx = hash(name) % len(Logger._colors)
        return Logger._colors[idx]

    @staticmethod
    def get_logger(log_name: str):

        if log_name in Logger._loggers:
            return Logger._loggers[log_name]

        logger = logging.getLogger(log_name)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        file_name = "log.log"
        log_path = os.path.join(log_dir, file_name)

        file_handler = logging.FileHandler(log_path)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        color = Logger._color_for_name(log_name)
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            f"{color}%(asctime)s - %(name)s - %(levelname)s - %(message)s{Style.RESET_ALL}"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        Logger._loggers[log_name] = logger
        return logger
