"""Service integration module for reading the canonical record."""

from typing import Optional

from flask import Flask, current_app, g

from arxiv.canonical import Repository, NoSuchResource
from arxiv.canonical.services.store import CanonicalStore


class RepositoryService(Repository):
    @classmethod
    def init_app(cls, app: Flask) -> None:
        """Set default configuration parameters for an app instance."""
        app.config.setdefault(f'CANONICAL_BUCKET', 'arxiv-canonical-record')
        app.config.setdefault(f'CANONICAL_VERIFY', True)
        app.config.setdefault('AWS_REGION', 'us-east-1')
        app.config.setdefault('AWS_ACCESS_KEY_ID', None)
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', None)

    @classmethod
    def get_session(cls, app: Optional[Flask] = None) -> 'RepositoryService':
        """Get a new session with the RepositoryService."""
        if app is None:
            app = current_app
        try:
            params = app.config.get_namespace(f'CANONICAL_')
            storage = CanonicalStore(
                params['bucket'],
                verify=params.get('verify', True),
                region_name=app.config['AWS_REGION'],
                endpoint_url=params.get('endpoint_url', None),
                aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY']
            )
        except KeyError as e:
            raise RuntimeError('Must call init_app() on app before use') from e
        return cls(storage, [storage], None)

    @classmethod
    def current_session(cls) -> 'RepositoryService':
        """Get or create a RepositoryService session for this context."""
        if not g:
            return cls.get_session()
        elif 'repository' not in g:
            g.repository = cls.get_session()
        session: RepositoryService = g.repository
        return session