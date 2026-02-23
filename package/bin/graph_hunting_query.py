import os
import sys
import time
import json
import logging

SPLUNK_HOME = os.environ['SPLUNK_HOME']
APP_NAME = "graph_api_endpoints"
sys.path.append(os.path.join(SPLUNK_HOME, "etc", "apps", APP_NAME, "lib"))

from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option
from solnlib import conf_manager

from GraphAPI import GraphAPI

ADDON_NAME = APP_NAME


def get_account_info(session_key: str, account_name: str):
    cfm = conf_manager.ConfManager(
        session_key,
        ADDON_NAME,
        realm=f"__REST_CREDENTIAL__#{ADDON_NAME}#configs/conf-graph_api_endpoints_account",
    )
    account_conf_file = cfm.get_conf("graph_api_endpoints_account")
    stanza = account_conf_file.get(account_name)

    if not stanza:
        raise ValueError(f"Account '{account_name}' not found")

    client_id = stanza.get("client_id")
    client_secret = stanza.get("client_secret")

    if not client_id or not client_secret:
        raise ValueError(f"Account '{account_name}' missing client_id/client_secret")

    return client_id, client_secret


@Configuration()
class GraphHuntingQueryCommand(GeneratingCommand):

    account = Option(require=True)
    tenant_id = Option(require=True)
    query = Option(require=True)

    timespan = Option(require=False)
    api_version = Option(require=False)
    limit = Option(require=False)

    def generate(self):
        logger = logging.getLogger("graphhunting")
        logger.setLevel(logging.INFO)

        try:
            session_key = self.service.token

            client_id, client_secret = get_account_info(session_key, self.account)

            graph = GraphAPI(client_id, client_secret, self.tenant_id)
            graph.getAuthToken()

            api_version = (self.api_version or "v1.0").strip("/")

            rows, status = graph.runHuntingQuery(
                query=self.query,
                timespan=self.timespan,
                api_version=api_version
            )

            if status != 200:
                evt = {
                    "ok": False,
                    "status": status,
                    "error": rows,
                    "account": self.account,
                    "tenant_id": self.tenant_id,
                    "api_version": api_version,
                    "timespan": self.timespan,
                }
                evt["_time"] = time.time()
                evt["_raw"] = json.dumps(evt, ensure_ascii=False, default=str)
                yield evt
                return

            if not isinstance(rows, list):
                rows = [rows]

            if self.limit:
                try:
                    lim = int(self.limit)
                    if lim > 0:
                        rows = rows[:lim]
                except Exception:
                    pass

            now = time.time()
            for r in rows:
                if isinstance(r, dict):
                    r.update({
                        "ok": True,
                        "account": self.account,
                        "tenant_id": self.tenant_id,
                        "api_version": api_version,
                        "timespan": self.timespan,
                    })
                    evt = r
                else:
                    evt = {
                        "ok": True,
                        "account": self.account,
                        "tenant_id": self.tenant_id,
                        "api_version": api_version,
                        "timespan": self.timespan,
                        "result": r,
                    }

                evt["_time"] = now
                evt["_raw"] = json.dumps(evt, ensure_ascii=False, default=str)
                yield evt

        except Exception as e:
            err = {"ok": False, "exception": str(e)}
            err["_time"] = time.time()
            err["_raw"] = json.dumps(err, ensure_ascii=False, default=str)
            yield err


dispatch(GraphHuntingQueryCommand, sys.argv, sys.stdin, sys.stdout, __name__)
