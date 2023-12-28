import logging 

from datastore.logger import set_logger, get_logger, DATE_FORMAT

def test_set_logger_default():
    # test if logging instance is created
    logger = set_logger("test_logger")
    assert isinstance(logger, logging.Logger)

def test_set_logger_with_source_name():
    # test if logging instance is created with given name
    logger = set_logger("test_logger", source_name="test_source")
    assert isinstance(logger, logging.Logger)
    
    # Ensure the filename of the RotatingFileHandler matches the expected format
    rotating_file_handler = logger.handlers[0] # first handler must be rotating file handler
    expected_log_filename = f"test_source_{DATE_FORMAT}_ingestion.log"
    assert rotating_file_handler.baseFilename.endswith(expected_log_filename)

def test_get_logger():
    # test if logger is instance of logging
    logger = get_logger("test_logger")
    assert isinstance(logger, logging.Logger)