from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import os
import argparse
import logging
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def main(cert, key, root_ca, endpoint, port, topic, client_id):
    working_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Paths for certificate, private key, and CA root certificate
    caPath = os.path.join(working_dir, root_ca)
    certPath = os.path.join(working_dir, cert)
    keyPath = os.path.join(working_dir, key)

    # Initialize the MQTT client
    myMQTTClient = AWSIoTMQTTClient(client_id)
    myMQTTClient.configureEndpoint(endpoint, port)
    myMQTTClient.configureCredentials(caPath, keyPath, certPath)

    # MQTT Client connection setup
    myMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
    myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline publish queueing
    myMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
    myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
    myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

    # Connect to AWS IoT Core
    try:
        myMQTTClient.connect()
        logger.info("Connected to AWS IoT Core")

        while True:
            message_dict = {"message": f"Hello from {client_id}"}
            message_json = json.dumps(message_dict)
            myMQTTClient.publish(topic, message_json, 0)
            logger.info(f"Published data to topic {topic}: {message_json}")
            time.sleep(5)  # Delay remains the same

    except KeyboardInterrupt:
        logger.info("Script stopped by the user.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MQTT Client for AWS IoT Core')
    parser.add_argument('--cert', required=True, help='Certificate for AWS IoT Core connection')
    parser.add_argument('--key', required=True, help='Private key for AWS IoT Core connection')
    parser.add_argument('--root-ca', required=True, help='Root CA certificate for AWS IoT Core connection')
    parser.add_argument('--endpoint', required=True, help='Endpoint for AWS IoT Core connection')
    parser.add_argument('--port', type=int, default=8883, help='Port for AWS IoT Core connection')
    parser.add_argument('--topic', default='hello/sensor_01', help='MQTT topic to publish to')
    parser.add_argument('--client-id', default='sensor_01', help='MQTT client ID')

    args = parser.parse_args()

    main(args.cert, args.key, args.root_ca, args.endpoint, args.port, args.topic, args.client_id)
  
