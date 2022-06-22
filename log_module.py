import logging

logger = logging.getLogger("protopool")
hdlr = logging.handlers.TimedRotatingFileHandler('./protopool.log',
                                                 when='midnight')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)
