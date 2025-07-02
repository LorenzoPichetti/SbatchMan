"""A utility to create, launch, and monitor code experiments."""

__version__ = "0.1.0"

from .api import SbatchManAPI
from .exceptions import SbatchManError, ProjectNotInitializedError, ProjectExistsError

__all__ = ["SbatchManAPI", "SbatchManError", "ProjectNotInitializedError", "ProjectExistsError"]