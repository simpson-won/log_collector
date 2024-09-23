import signal
from log import logger
import sys


def default_exit(sig_num, data):
    logger.info(f'sig_handler: {sig_num}, {data}')
    sys.exit()


registered_signal = {
    signal.SIGTERM: default_exit,
    signal.SIGSEGV: default_exit,
    signal.SIGHUP: default_exit,
    signal.SIGABRT: default_exit,
}


def sig_handler(sig_num, data):
    if sig_num in registered_signal:
        if registered_signal[sig_num] is not None:
            registered_signal[sig_num](sig_num, data)


def sig_init(signals: []):
    for sig in signals:
        if sig[0] not in registered_signal:
            if len(sig) == 2:
                registered_signal[sig[0]] = sig[1]
            else:
                registered_signal[sig[0]] = None
            signal.signal(sig[0], sig_handler)
