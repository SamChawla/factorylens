"""Typed exception hierarchy for FactoryLens.

Small and specific on purpose: the CLI can print a meaningful message and the
agent can branch on failure kind, without any layer resorting to a bare
``except Exception``.
"""


class FactoryLensError(Exception):
    """Base for every error this project raises deliberately."""


class PipelineStageError(FactoryLensError):
    """A pipeline stage (ingest/clean/transform/aggregate) failed.

    Raised wrapping the original cause so the stage that failed is always
    identifiable in the message and the span records the error.
    """


class DataQualityError(FactoryLensError):
    """Input data violated an invariant the pipeline cannot recover from.

    Distinct from routine row-level faults (missing readings, malformed rows),
    which are *cleaned and counted*, not raised — this is for a whole batch or
    dataset being unusable.
    """


class LLMProviderError(FactoryLensError):
    """An LLM provider call failed (network, auth, rate-limit, bad response).

    The adapter raises this from the primary provider to trigger fallback, and
    again from the fallback if both are exhausted.
    """


class ConfigError(FactoryLensError):
    """Required configuration is missing or malformed."""
