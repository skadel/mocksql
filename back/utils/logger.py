import logging

DIAG = 15
logging.addLevelName(DIAG, "DIAG")


def _diag(self, message, *args, **kwargs):
    if self.isEnabledFor(DIAG):
        self._log(DIAG, message, args, **kwargs)


logging.Logger.diag = _diag
