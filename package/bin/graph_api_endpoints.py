from datetime import datetime, timezone
import json
import logging
import sys

import import_declare_test
from solnlib import conf_manager, log
from splunklib import modularinput as smi

from GraphAPI import GraphAPI  # Asegúrate de que el archivo se llame GraphAPI.py

ADDON_NAME = "graph_api_endpoints"


def logger_for_input(input_name: str) -> logging.Logger:
    return log.Logs().get_logger(f"{ADDON_NAME.lower()}_{input_name}")


def get_account_info(session_key: str, account_name: str):
    cfm = conf_manager.ConfManager(
        session_key,
        ADDON_NAME,
        realm=f"__REST_CREDENTIAL__#{ADDON_NAME}#configs/conf-graph_api_endpoints_account",
    )
    account_conf_file = cfm.get_conf("graph_api_endpoints_account")
    credentials = account_conf_file.get(account_name)
    if not credentials:
        raise ValueError(f"Account '{account_name}' not found")
    return credentials.get("client_secret"), credentials.get("client_id")


class Input(smi.Script):
    def get_scheme(self):
        scheme = smi.Scheme("graph_api_endpoints")
        scheme.description = "Generic Microsoft Graph API endpoint input"
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = False
        return scheme

    def validate_input(self, definition: smi.ValidationDefinition):
        pass

    def stream_events(self, inputs: smi.InputDefinition, event_writer: smi.EventWriter):
        for input_name, input_item in inputs.inputs.items():
            normalized_input_name = input_name.split("/")[-1]
            logger = logger_for_input(normalized_input_name)
            log.modular_input_start(logger, normalized_input_name)

            try:
                session_key = inputs.metadata["session_key"]
                log_level = conf_manager.get_log_level(
                    logger=logger,
                    session_key=session_key,
                    app_name=ADDON_NAME,
                    conf_name="graph_api_endpoints_settings",
                )
                logger.setLevel(log_level)

                # Parámetros del input
                account_name = input_item.get("account")
                tenant_id = input_item.get("tenant_id")
                endpoint = input_item.get("endpoint")
                index = input_item.get("index")
                sourcetype = (
                    input_item.get("sourcetype")
                    if input_item.get("sourcetype")
                    else "msgraph:generic"
                )

                if not input_item.get("sourcetype"):
                    logger.info(
                        f"Using default sourcetype 'msgraph:generic' for input {normalized_input_name}"
                    )

                if sourcetype and not sourcetype.startswith("msgraph:"):
                    logger.info(
                        f"Non-standard sourcetype '{sourcetype}' used for input {normalized_input_name}"
                    )

                if not all([account_name, tenant_id, endpoint, index]):
                    missing = [
                        k
                        for k in ["account", "tenant_id", "endpoint", "index"]
                        if not input_item.get(k)
                    ]
                    logger.error(
                        f"Missing required parameters: {','.join(missing)} for input {normalized_input_name}"
                    )
                    continue

                # Obtener credenciales
                try:
                    client_secret, client_id = get_account_info(
                        session_key, account_name
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to retrieve credentials for account {account_name}: {e}"
                    )
                    continue

                if not all([client_id, client_secret]):
                    missing = [
                        k
                        for k in ["client_id", "client_secret"]
                        if not input_item.get(k)
                    ]
                    logger.error(
                        f"Missing required parameters: {','.join(missing)} for input {normalized_input_name}"
                    )
                    continue

                logger.info(
                    f"Initializing GraphAPI for input: {normalized_input_name} | Endpoint: {endpoint}"
                )

                # Autenticación
                graph = GraphAPI(client_id, client_secret, tenant_id)

                try:
                    graph.getAuthToken()
                    logger.info("Access token obtained successfully")
                except Exception as e:
                    logger.exception(
                        f"Authentication failed for input {normalized_input_name}: {e}"
                    )
                    continue

                # Llamada al endpoint + logging unificado
                try:
                    records, status_code = graph.getInfo(endpoint)

                    if status_code == 200:
                        count = len(records) if isinstance(records, list) else 0
                        logger.info(
                            f"Graph API call succeeded | endpoint={endpoint} | status={status_code} | records={count}"
                        )
                    else:
                        # Error devuelto por Graph API → mostramos el cuerpo
                        error_summary = json.dumps(
                            records, ensure_ascii=False, indent=None
                        )
                        if len(error_summary) > 2000:
                            error_summary = error_summary[:1950] + "... [truncated]"

                        logger.error(
                            f"Graph API call failed | endpoint={endpoint} | "
                            f"status={status_code} | error_response={error_summary}"
                        )
                        continue  # No intentamos ingestar

                except Exception as e:
                    logger.exception(
                        f"Exception during Graph API call {normalized_input_name}: {e}"
                    )
                    continue

                # Ingestión (solo si fue exitoso)
                if records and isinstance(records, list) and len(records) > 0:
                    current_time = datetime.now(timezone.utc).isoformat()
                    ingested_count = 0
                    skipped_count = 0
                    for record in records:
                        if not isinstance(record, dict):
                            logger.warning(
                                f"Skipping non-dict record type {type(record).__name__} for input {normalized_input_name}"
                            )
                            skipped_count += 1
                            continue
                        record.update(
                            {
                                "input_name": normalized_input_name,
                                "graph_endpoint": endpoint,
                            }
                        )
                        event = smi.Event()
                        event.stanza = input_name
                        event.index = index
                        event.sourcetype = sourcetype
                        event.source = f"msgraph:{normalized_input_name}"
                        event.time = current_time
                        event.data = json.dumps(record, ensure_ascii=False, default=str)
                        try:
                            event_writer.write_event(event)
                            ingested_count += 1
                        except Exception as e:
                            logger.warning(
                                f"Failed to write event to Splunk for input {normalized_input_name}: {e}"
                            )
                            skipped_count += 1
                    logger.info(
                        f"Ingested {ingested_count} events into Splunk index={index} | skipped={skipped_count} for input {normalized_input_name}"
                    )
                elif records and not isinstance(records, list):
                    # Single object response (not wrapped in 'value' array)
                    logger.warning(
                        f"Endpoint {endpoint} returned a single object (not a list) for input {normalized_input_name}"
                    )
                    records = [records]
                    current_time = datetime.utcnow().isoformat()
                    event = smi.Event()
                    event.stanza = input_name
                    event.index = index
                    event.sourcetype = sourcetype
                    event.source = f"msgraph:{normalized_input_name}"
                    event.time = current_time
                    event.data = json.dumps(records[0], ensure_ascii=False, default=str)
                    try:
                        event_writer.write_event(event)
                        logger.info(
                            f"Ingested 1 event (single object) into Splunk index={index} for input {normalized_input_name}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to write single object event for input {normalized_input_name}: {e}"
                        )
                else:
                    logger.error(
                        f"No records to ingest (empty or invalid result) for input {normalized_input_name}"
                    )

            except Exception as e:
                logger.exception(
                    f"Unexpected error in modular input {normalized_input_name}: {e}"
                )

            log.modular_input_end(logger, normalized_input_name)


if __name__ == "__main__":
    exit_code = Input().run(sys.argv)
    sys.exit(exit_code)
