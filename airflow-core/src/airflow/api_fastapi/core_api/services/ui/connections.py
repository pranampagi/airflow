# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import logging
from collections.abc import MutableMapping
from functools import cache
from typing import TYPE_CHECKING, Literal

from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionHookFieldBehavior,
    ConnectionHookMetaData,
    StandardHookFields,
)
from airflow.providers_manager import HookInfo, ProvidersManager
from airflow.serialization.definitions.param import SerializedParam

if TYPE_CHECKING:
    from airflow.providers_manager import ConnectionFormWidgetInfo

log = logging.getLogger(__name__)


class HookMetaService:
    """Service for retrieving details about hooks to render UI."""



    @staticmethod
    def _make_standard_fields(field_behaviour: dict | None) -> StandardHookFields | None:
        if not field_behaviour:
            return None

        def make_field(field_name: str, field_behaviour: dict) -> ConnectionHookFieldBehavior | None:
            hidden_fields = field_behaviour.get("hidden_fields", [])
            relabeling = field_behaviour.get("relabeling", {}).get(field_name)
            placeholder = field_behaviour.get("placeholders", {}).get(field_name)
            if any([field_name in hidden_fields, relabeling, placeholder]):
                return ConnectionHookFieldBehavior(
                    hidden=field_name in hidden_fields,
                    title=relabeling,
                    placeholder=placeholder,
                )
            return None

        return StandardHookFields(
            description=make_field("description", field_behaviour),
            url_schema=make_field("schema", field_behaviour),
            host=make_field("host", field_behaviour),
            port=make_field("port", field_behaviour),
            login=make_field("login", field_behaviour),
            password=make_field("password", field_behaviour),
        )

    @staticmethod
    def _convert_extra_fields(form_widgets: dict[str, ConnectionFormWidgetInfo]) -> dict[str, MutableMapping]:
        result: dict[str, MutableMapping] = {}
        for key, form_widget in form_widgets.items():
            hook_key = key.split("__")[1]
            hook_widgets = result.get(hook_key, {})

            if isinstance(form_widget.field, dict):
                # yaml path, form widgets read from yaml and already present in SerializedParam.dump() format
                hook_widgets[form_widget.field_name] = form_widget.field

            elif type(form_widget.field).__name__ == "UnboundField":
                # handle real WTForms fields gracefully without needing mock patches
                field_class_name = getattr(form_widget.field.field_class, "__name__", "")
                param_type = "string"
                param_format = None
                if field_class_name == "BooleanField":
                    param_type = "boolean"
                elif field_class_name == "IntegerField":
                    param_type = "integer"
                elif field_class_name == "PasswordField":
                    param_format = "password"

                label = (
                    form_widget.field.args[0]
                    if len(form_widget.field.args) > 0
                    else form_widget.field.kwargs.get("label")
                )
                validators = form_widget.field.kwargs.get("validators", [])
                description = form_widget.field.kwargs.get("description", "")
                default = form_widget.field.kwargs.get("default", None)

                enum = {}
                for v in validators:
                    if type(v).__name__ == "AnyOf":
                        enum["enum"] = getattr(v, "values", [])

                types = [param_type, "null"]
                format_dict = {"format": param_format} if param_format else {}

                param = SerializedParam(
                    default=default,
                    title=str(label) if label is not None else None,
                    description=str(description) if description else None,
                    source=None,
                    type=types,
                    **format_dict,
                    **enum,
                ).dump()
                hook_widgets[form_widget.field_name] = param
            else:
                log.error("Unknown form widget in %s: %s", hook_key, form_widget)
                continue

            result[hook_key] = hook_widgets
        return result

    @staticmethod
    @cache
    def hook_meta_data() -> list[ConnectionHookMetaData]:
        pm = ProvidersManager()
        widgets = HookMetaService._convert_extra_fields(pm._connection_form_widgets_from_metadata)
        return [
            ConnectionHookMetaData(
                connection_type=meta.connection_type,
                hook_class_name=meta.hook_class_name,
                default_conn_name=None,
                hook_name=meta.hook_name,
                standard_fields=HookMetaService._make_standard_fields(meta.field_behaviour),
                extra_fields=widgets.get(meta.connection_type),
            )
            for meta in pm.iter_connection_type_hook_ui_metadata()
        ]
