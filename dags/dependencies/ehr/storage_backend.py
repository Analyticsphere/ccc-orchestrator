import os


class StorageBackend:
    """
    Storage backend abstraction for handling different storage systems (GCS, Local).

    This module provides a simple abstraction layer that allows the codebase to work with
    different storage backends by configuring a single environment variable.

    Usage:
        from core.storage_backend import storage

        # Add storage scheme to path for DuckDB, etc.
        uri = storage.get_uri("bucket/path/file.parquet")
        # Returns: "gs://bucket/path/file.parquet" (default GCS)

        # Remove storage scheme from path for parsing
        path = storage.strip_scheme("gs://bucket/path/file.parquet")
        # Returns: "bucket/path/file.parquet"

    Configuration:
        Set the STORAGE_BACKEND environment variable to one of:
        - 'gcs' (default): Google Cloud Storage (gs://)
        - 'local': Local filesystem (file://)
    """

    # Supported storage backends and their URI schemes
    BACKENDS = {
        'gcs': 'gs://',
        'local': 'file://',
    }

    def __init__(self, backend: str = 'gcs'):
        """
        Initialize the storage backend.

        Args:
            backend: The storage backend to use. One of: 'gcs', 'local'
                    Defaults to 'gcs' if not specified or if an invalid value is provided.
        """
        self.backend = backend if backend in self.BACKENDS else 'gcs'
        self.scheme = self.BACKENDS[self.backend]

    def get_uri(self, path: str) -> str:
        """
        Add storage scheme to path if not already present.

        Args:
            path: File path without storage scheme (e.g., "bucket/path/file.parquet")
                  or with any storage scheme (will be normalized)

        Returns:
            Complete URI with the configured storage scheme
        """
        # Strip any existing scheme first to normalize
        path = self.strip_scheme(path)
        return f"{self.scheme}{path}"

    def strip_scheme(self, path: str) -> str:
        """
        Remove any storage scheme from path.

        Args:
            path: File path with or without storage scheme

        Returns:
            Path without any storage scheme prefix
        """
        for scheme in self.BACKENDS.values():
            if path.startswith(scheme):
                return path[len(scheme):]
        return path


# Global storage backend instance
# Configured via STORAGE_BACKEND environment variable (defaults to 'gcs')
storage = StorageBackend(backend=os.getenv('STORAGE_BACKEND', 'gcs'))
