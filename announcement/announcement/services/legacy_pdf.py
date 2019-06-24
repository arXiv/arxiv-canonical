"""Provides a service to retrieve e-print PDF from the legacy system."""

from arxiv.integration.api.service import HTTPIntegration


class LegacyPDFService(HTTPIntegration):
    """Integration with legacy system to retrieve e-print PDF."""

    class Meta:
        service_name = "legacy_eprint"