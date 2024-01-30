import logging
import requests
from exceptions import HttpStatusException
import yaml


class KafkaPublish:
    def __init__(self, payload: str, env: str = "", client_id: str = "", client_secret: str = "") -> None:
        logging.basicConfig(level=logging.INFO)

        with open("streaming_cofig.yaml", "r") as yamlfile:
            config = yaml.load(yamlfile, Loader=yaml.FullLoader)

        self.config = config
        self.env = env
        self.payload = payload
        oath_config = config["oath_config"]
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_endpoint_uri = oath_config["token_endpoint_uri"]
        self.http_endpoint_url = config["http_endpoint_url"]

    def get_jwt_token(self) -> str:
        headers = '{"Content-Type": "application/x-www-form-urlencoded"}'

        jwt_token = generate_okta_token(self.client_id, self.client_secret, self.token_endpoint_uri, None, headers)
        return jwt_token

    def publish_message(self, jwt_token: str) -> str:
        """
        This Function posts xml to http endpoint
        :param jwt_token: okta token
        :rtype: string
        :return: Result of published message
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {jwt_token}",
        }

        try:
            response = requests.post(self.http_endpoint_url, data=self.payload, headers=headers, timeout=10)

            if response.status_code == 200:
                content = response.content
                content_string = content.decode("utf-8")
                logging.info("Data published successfully - %s", content_string)
                return content_string
            else:
                logging.info("Status Code: %s", str(response.status_code))
                raise HttpStatusException("Unsuccessful status code")
        except HttpStatusException as e:
            raise HttpStatusException(f"produce_message: exception in  publishing to kafka - {str(e)}")

    def publish_xmldata(self) -> str:
        jwt_token = self.get_jwt_token()
        result = self.publish_message(jwt_token)
        return result

def main():
    """
    Main function to execute the streaming pipeline.
    """
    # Serialize the XML data (e.g., as JSON)
    json_data = '{"xml_payload": "' + xml_data.replace('"', '\\"') + '"}'
    KafkaPublish = KafkaPublish(json_data, "prod", "<client_id>", "<client_secret>")
    KafkaPublish.publish_xmldata()

# Execute the main function
if __name__ == "__main__":
    main()
