# """
# Validation classes

# Available Validators:
#     ValidateModelNamesAreUnique: Ensures all model names across extract, transform,
#         and load stages are unique within a job configuration.
#     ValidateUpstreamNamesExist: Verifies that all upstream references in transforms
#         and loads point to existing model names in the job configuration.
# """

# import logging
# from abc import ABC, abstractmethod
# from typing import TYPE_CHECKING, Generic, TypeVar

# from flint.utils.logger import get_logger

# if TYPE_CHECKING:
#     from flint.runtime.jobs.spark.job import Job

# logger: logging.Logger = get_logger(__name__)

# T = TypeVar("T")


# class ValidateBase(ABC, Generic[T]):
#     """Abstract base class for validation operations."""

#     @abstractmethod
#     def __init__(self, data: T) -> None:
#         """Initialize validator and perform validation.

#         Args:
#             data: The data to validate.

#         Raises:
#             ValueError: If validation fails.
#         """


# class ValidateModelNamesAreUnique(ValidateBase["Job"]):
#     """Validator to ensure all model names within a job are unique."""

#     def __init__(self, data: "Job") -> None:
#         """Initialize validator and perform validation.

#         Args:
#             data: The Job instance to validate.

#         Raises:
#             ValueError: If any model name is not unique.
#         """
#         logger.debug("Starting model name uniqueness validation")
#         model_name_sources: dict[str, str] = {}

#         # Validate extract names
#         logger.debug("Validating %d extract names", len(data.extracts))
#         for extract in data.extracts:
#             extract_name: str = extract.model.name
#             if extract_name in model_name_sources:
#                 original_stage = model_name_sources[extract_name]
#                 raise ValueError(
#                     f"Duplicate name '{extract_name}' found in extract stage - already used in {original_stage} stage"
#                 )
#             model_name_sources[extract_name] = "extract"
#             logger.debug("Extract name '%s' is unique", extract_name)

#         # Validate transform names
#         logger.debug("Validating %d transform names", len(data.transforms))
#         for transform in data.transforms:
#             transform_name: str = transform.model.name
#             if transform_name in model_name_sources:
#                 original_stage = model_name_sources[transform_name]
#                 raise ValueError(
#                     f"Duplicate name '{transform_name}' found in transform stage - "
#                     f"already used in {original_stage} stage"
#                 )
#             model_name_sources[transform_name] = "transform"
#             logger.debug("Transform name '%s' is unique", transform_name)

#         # Validate load names
#         logger.debug("Validating %d load names", len(data.loads))
#         for load in data.loads:
#             load_name: str = load.model.name
#             if load_name in model_name_sources:
#                 original_stage = model_name_sources[load_name]
#                 raise ValueError(
#                     f"Duplicate name '{load_name}' found in load stage - already used in {original_stage} stage"
#                 )
#             model_name_sources[load_name] = "load"
#             logger.debug("Load name '%s' is unique", load_name)

#         logger.info(
#             "Model name uniqueness validation completed successfully - validated %d unique names",
#             len(BaseModel_name_sources),
#         )


# class ValidateUpstreamNamesExist(ValidateBase["Job"]):
#     """Validator to ensure all upstream names in transforms and loads actually exist."""

#     def __init__(self, data: "Job") -> None:
#         """Initialize validator and perform validation.

#         Args:
#             data: The Job instance to validate.

#         Raises:
#             ValueError: If any upstream name does not exist.
#         """
#         logger.debug("Starting upstream name existence validation")

#         # Collect all available model names
#         available_names: set[str] = set()

#         # Add extract names
#         for extract in data.extracts:
#             available_names.add(extract.model.name)
#             logger.debug("Added extract name to available names: %s", extract.model.name)

#         # Add transform names
#         for transform in data.transforms:
#             available_names.add(transform.model.name)
#             logger.debug("Added transform name to available names: %s", transform.model.name)

#         # not adding loads here because they are not upstreams for other stages
#         logger.debug("Total available upstream names: %d", len(available_names))

#         # Check transform upstream names
#         logger.debug("Validating %d transform upstream references", len(data.transforms))
#         for transform in data.transforms:
#             if transform.model.upstream_name not in available_names:
#                 raise ValueError(
#                     f"Transform '{transform.model.name}' references upstream name "
#                     f"'{transform.model.upstream_name}' which does not exist in any stage"
#                 )
#             logger.debug(
#                 "Transform '%s' upstream reference '%s' is valid", transform.model.name, transform.model.upstream_name
#             )

#         # Check load upstream names
#         logger.debug("Validating %d load upstream references", len(data.loads))
#         for load in data.loads:
#             if load.model.upstream_name not in available_names:
#                 raise ValueError(
#                     f"Load '{load.model.name}' references upstream name '{load.model.upstream_name}' "
#                     f"which does not exist in any stage"
#                 )
#             logger.debug("Load '%s' upstream reference '%s' is valid", load.model.name, load.model.upstream_name)

#         logger.info("Upstream name existence validation completed successfully")
