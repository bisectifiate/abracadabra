from collections.abc import Sequence

from dagster_components import (
    AssetSpecModel,
    Component,
    ComponentLoadContext,
    ResolvableModel,
    registered_component_type,
)

import dagster as dg


class ShellCommandParams(ResolvableModel):
    path: str
    asset_specs: Sequence[AssetSpecModel]


@registered_component_type(name="shell_command")
class ShellCommand(Component):
    def __init__(self, params):
        self.params = params

    ...

    def build_defs(self, load_context: ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(...)
