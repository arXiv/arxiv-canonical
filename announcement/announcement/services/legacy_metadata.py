"""Provides a service to retrieve e-print metadata from the legacy system."""

from arxiv.integration.api.service import HTTPIntegration


class LegacyMetadataService(HTTPIntegration):
    """Integration with legacy system to retrieve e-print metadata."""

    class Meta:
        service_name = "legacy_metadata"