"""Writer schemas by id from the Confluent-compatible registry API, cached on the driver."""

import json
import urllib.error
import urllib.request


class SchemaCache:
    def __init__(self, registry_url: str):
        self._base = f"{registry_url.rstrip('/')}/apis/ccompat/v7"
        self._schemas: dict[int, str | None] = {}

    def __call__(self, schema_id: int) -> str | None:
        """The schema text, or None if the registry does not know the id.

        Any other failure raises: the batch fails and Spark retries it from the checkpoint.
        """
        if schema_id not in self._schemas:
            url = f"{self._base}/schemas/ids/{schema_id}"
            try:
                with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310 (config URL)
                    self._schemas[schema_id] = json.load(resp)["schema"]
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    raise
                # Not cached: an id registered later must decode once it exists.
                return None
        return self._schemas[schema_id]
