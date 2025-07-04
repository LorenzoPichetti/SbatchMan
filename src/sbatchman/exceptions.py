class SbatchManError(Exception):
  """Base exception for SbatchMan."""
  pass

class ProjectNotInitializedError(SbatchManError):
  """Raised when the SbatchMan root directory cannot be found."""
  def __init__(self, message="SbatchMan root not found. Please run 'sbatchman init' or specify a directory."):
    self.message = message
    super().__init__(self.message)

class ProjectExistsError(SbatchManError):
  """Raised when the SbatchMan root directory is already present."""
  def __init__(self, message="SbatchMan root present already. Enjoy using SbatchMan!"):
    self.message = message
    super().__init__(self.message)

class SchedulerMismatchError(SbatchManError):
  """Raised when a new config's scheduler doesn't match the existing one for a cluster."""
  def __init__(self, message: str):
    self.message = message
    super().__init__(self.message)

class ConfigurationError(SbatchManError):
  """Raised when there is an error in the configuration."""
  def __init__(self, message: str):
    self.message = message
    super().__init__(self.message)

class ConfigurationNotFoundError(ConfigurationError):
  """Raised when a specific configuration file is not found."""
  def __init__(self, message: str):
    self.message = message
    super().__init__(self.message)

class JobSubmitError(SbatchManError):
  """Raised when an error occurs during the submission of a job."""
  def __init__(self, message: str):
    self.message = message
    super().__init__(self.message)

class ArchiveExistsError(SbatchManError):
  """Raised when an archive already exists and overwrite is False."""
  def __init__(self, message: str):
    self.message = message
    super().__init__(self.message)

class ClusterNameNotSetError(SbatchManError):
  """Raised when the cluster name is not set in the global configuration."""
  def __init__(self, message="Cluster name not set. Please run 'sbatchman set-cluster-name'."):
    self.message = message
    super().__init__(self.message)

class JobExistsError(SbatchManError):
  """Raised when an identical job already exists."""
  def __init__(self, message: str):
    self.message = message
    super().__init__(self.message)