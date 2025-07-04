from enum import ReprEnum


class Status(str, ReprEnum):
  SUBMITTING = "SUBMITTING",
  FAILED_SUBMISSION = "FAILED_SUBMISSION",
  QUEUED = "QUEUED",
  RUNNING = "RUNNING",
  COMPLETED = "COMPLETED",
  FAILED = "FAILED",
  CANCELLED = "CANCELLED",
  TIMEOUT = "TIMEOUT",
  OTHER = "OTHER",
  UNKNOWN = "UNKNOWN"

  def __reduce_ex__(self, proto):
    return self.__class__, (self._value_,)

TERMINAL_STATES = {Status.COMPLETED, Status.FAILED, Status.CANCELLED, Status.UNKNOWN, Status.TIMEOUT, Status.FAILED_SUBMISSION}