"""Survey adapter registry."""

from mospi_microdata.adapters.asi import ASIAdapter

ADAPTER_REGISTRY = {
    "ASI": ASIAdapter,
}
