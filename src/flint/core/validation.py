"""
Validation classes

Available Validators:
    ValidateModelNamesAreUnique: Ensures all model names across extract, transform,
        and load stages are unique within a job configuration.
    ValidateUpstreamNamesExist: Verifies that all upstream references in transforms
        and loads point to existing model names in the job configuration.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from flint.core.job import Job

T = TypeVar("T")


class ValidationBase(ABC, Generic[T]):
    """Abstract base class for validation operations."""

    @abstractmethod
    def __init__(self, data: T) -> None:
        """Initialize validator and perform validation.

        Args:
            data: The data to validate.

        Raises:
            ValueError: If validation fails.
        """


class ValidateModelNamesAreUnique(ValidationBase["Job"]):
    """Validator to ensure all model names within a job are unique."""

    def __init__(self, data: "Job") -> None:
        """Initialize validator and perform validation.

        Args:
            data: The Job instance to validate.

        Raises:
            ValueError: If any model name is not unique.
        """
        model_name_sources: dict[str, str] = {}

        for extract in data.extracts:
            extract_name: str = extract.model.name
            if extract_name in model_name_sources:
                original_stage = model_name_sources[extract_name]
                raise ValueError(
                    f"Duplicate name '{extract_name}' found in extract stage - already used in {original_stage} stage"
                )
            model_name_sources[extract_name] = "extract"

        for transform in data.transforms:
            transform_name: str = transform.model.name
            if transform_name in model_name_sources:
                original_stage = model_name_sources[transform_name]
                raise ValueError(
                    f"Duplicate name '{transform_name}' found in transform stage - "
                    f"already used in {original_stage} stage"
                )
            model_name_sources[transform_name] = "transform"

        for load in data.loads:
            load_name: str = load.model.name
            if load_name in model_name_sources:
                original_stage = model_name_sources[load_name]
                raise ValueError(
                    f"Duplicate name '{load_name}' found in load stage - already used in {original_stage} stage"
                )
            model_name_sources[load_name] = "load"


class ValidateUpstreamNamesExist(ValidationBase["Job"]):
    """Validator to ensure all upstream names in transforms and loads actually exist."""

    def __init__(self, data: "Job") -> None:
        """Initialize validator and perform validation.

        Args:
            data: The Job instance to validate.

        Raises:
            ValueError: If any upstream name does not exist.
        """
        # Collect all available model names
        available_names: set[str] = set()

        for extract in data.extracts:
            available_names.add(extract.model.name)

        for transform in data.transforms:
            available_names.add(transform.model.name)

        # not adding loads here because they are not upstreams for other stages

        # Check transform upstream names

        for transform in data.transforms:
            if transform.model.upstream_name not in available_names:
                raise ValueError(
                    f"Transform '{transform.model.name}' references upstream name "
                    f"'{transform.model.upstream_name}' which does not exist in any stage"
                )

        # Check load upstream names
        for load in data.loads:
            if load.model.upstream_name not in available_names:
                raise ValueError(
                    f"Load '{load.model.name}' references upstream name '{load.model.upstream_name}' "
                    f"which does not exist in any stage"
                )
