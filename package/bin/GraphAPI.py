import requests
import json

class GraphAPI:
    def __init__(self, ClientID, ClientSecret, TenantID):
        self.ClientID = ClientID
        self.ClientSecret = ClientSecret
        self.TenantID = TenantID
        self.scope = "https://graph.microsoft.com/.default"
        self.url = "https://graph.microsoft.com/"
        self.timeout = 60
        self.max_retries = 3

    def getAuthToken(self):
        payload = {
            "client_id": self.ClientID,
            "client_secret": self.ClientSecret,
            "scope": self.scope,
            "grant_type": "client_credentials"
        }
        url = f"https://login.microsoftonline.com/{self.TenantID}/oauth2/v2.0/token"
        try:
            response = requests.post(url, data=payload)
            data = response.json()
        except Exception as e:
            print(e)
        
        self.token = data["access_token"]

    def getInfo(self, endpoint):
        headers = {
            "Authorization": f"Bearer {self.token}"
        }

        base_url = f"{self.url.rstrip('/')}/{endpoint.lstrip('/')}"
        url = base_url

        all_data = []
        final_status = None
        last_response = None

        while url:
            try:
                response = requests.get(url, headers=headers)
                final_status = response.status_code
                last_response = response

                if response.status_code == 200:
                    data = response.json()

                    if 'value' in data:
                        all_data.extend(data['value'])
                    else:
                        # Caso de endpoint que devuelve un solo objeto (no colección)
                        all_data.append(data)
                        break

                    url = data.get('@odata.nextLink')
                    if not url:
                        break

                else:
                    # Si NO es 200 → devolvemos directamente el JSON de error
                    try:
                        error_data = response.json()
                    except ValueError:
                        error_data = {
                            "error": "non_json_response",
                            "status_code": response.status_code,
                            "raw_content": response.text[:1000]  # limitamos para no saturar
                        }
                    
                    return error_data, response.status_code

            except requests.exceptions.RequestException as e:
                print(f"Error de red/conexión: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Respuesta: {e.response.text[:500]}")
                return {"error": "request_exception", "detail": str(e)}, 0

            except ValueError as e:
                print("Error: Respuesta no es JSON válido")
                print(response.text[:500])
                return {"error": "invalid_json", "raw_content": response.text[:1000]}, response.status_code

            except Exception as e:
                print(f"Error inesperado: {e}")
                return {"error": "unexpected_error", "detail": str(e)}, 0

        # Solo llegamos aquí si todo fueron 200 OK
        return all_data, final_status or 200

    def runHuntingQuery(self, query, timespan=None, api_version="v1.0"):
        url = f"{self.url.strip('/')}/{api_version.strip('/')}/security/runHuntingQuery"

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        page_size = 1000
        max_pages = 200
        all_rows = []
        skip = 0

        for _ in range(max_pages):
            body = {"Query": query}
            if timespan:
                body["Timespan"] = timespan
            body["Top"] = page_size
            body["Skip"] = skip

            resp = requests.post(url, headers=headers, json=body, timeout=self.timeout)

            if resp.status_code != 200:
                try:
                    return resp.json(), resp.status_code
                except Exception:
                    return {"error": resp.text}, resp.status_code

            try:
                data = resp.json()
            except Exception:
                return {"error": resp.text}, resp.status_code

            rows = data.get("results", [])
            if not isinstance(rows, list):
                rows = [rows]

            all_rows.extend(rows)

            if len(rows) < page_size:
                break

            skip += page_size

        return all_rows, 200