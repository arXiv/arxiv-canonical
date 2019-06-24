"""Provides a service to retrieve e-print source from the legacy system."""

from arxiv.integration.api.service import HTTPIntegration


class LegacySourceService(HTTPIntegration):
    """Integration with legacy system to retrieve e-print source."""

    class Meta:
        service_name = "legacy_source"