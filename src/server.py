if __name__ == "__main__":
    loggers = [Logger.get_logger(f"node-{i}") for i in range(1, 6)]

    for i in range(5):
        for logger in loggers:
            logger.info(f"Hello from {logger.name} (iteration {i})")
        time.sleep(0.5)
