import os
import json
import base64
from requests import post
from project.config.logging_config import setup_logging

class Connector():
    """
    A class used to represent a Connector to the Spotify API.

    Attributes
    ----------
    client_id : str
        Spotify client ID obtained from environment variables
    client_secret : str
        Spotify client secret obtained from environment variables
    token : str
        Access token for Spotify API
    auth_headers : dict
        Authorization headers for Spotify API requests

    Methods
    -------
    _fetch_access_token()
        Fetches the access token from Spotify API
    get_auth_headers()
        Returns the authorization headers for Spotify API requests
    """

    TOKEN_URL = "https://accounts.spotify.com/api/token"

    def __init__(self) -> None:
        """
        Initializes the Connector with client ID and client secret from environment variables.
        """
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        self.token = None
        self.auth_headers = None
        self.logger = setup_logging()

    def _fetch_access_token(self) -> None:
        """
        Fetches the access token from Spotify API using client credentials.

        Raises
        ------
        Exception
            If there is an error while fetching the access token.
        """
        try:
            auth_string = f"{self.client_id}:{self.client_secret}"
            auth_bytes = auth_string.encode("utf-8")
            auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

            headers = {
                "Authorization": f"Basic {auth_base64}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            data = {"grant_type": "client_credentials"}
            
            response = post(self.TOKEN_URL, headers=headers, data=data)
            if response.status_code == 200:
                response_json = json.loads(response.content)
                self.token = response_json["access_token"]
                self.logger.info("Access token fetched successfully")
            else:
                self.logger.error(f"Error while fetching access token: {response.status_code} - {response.content}")
                raise
        except Exception as e:
            self.logger.error(f"Error while fetching access token: {e}")
            raise

    def get_auth_headers(self) -> dict[str, str]:
        """
        Returns the authorization headers for Spotify API requests.

        Returns
        -------
        dict
            A dictionary containing the authorization headers.
        """
        self._fetch_access_token()
        self.auth_headers = {"Authorization": f"Bearer {self.token}"}
        return self.auth_headers