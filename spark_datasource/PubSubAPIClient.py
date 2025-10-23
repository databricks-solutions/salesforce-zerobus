"""
PubSubAPIClient.py

Parameterized Salesforce Pub/Sub API Python client for Spark integration.
This version removes hardcoded credentials and is designed for use with
Spark readStream options.
"""

import io
import json
import threading
import time
from urllib.parse import urlparse

import avro.io
import avro.schema
import certifi
import grpc
import requests

from .proto import pubsub_api_pb2 as pb2
from .proto import pubsub_api_pb2_grpc as pb2_grpc


class PubSubAPIClient:
    """
    Parameterized Salesforce Pub/Sub API Client for Spark integration.
    
    This version is designed to work with configuration passed from Spark
    readStream options rather than hardcoded values.
    """
    
    def __init__(self, grpc_host='api.pubsub.salesforce.com', grpc_port=7443):
        """
        Initialize the PubSub API client.
        
        Args:
            grpc_host (str): gRPC server host
            grpc_port (int): gRPC server port
        """
        # Connection info (stored for lazy initialization)
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port
        
        # Auth fields - will be set during authentication
        self.session_token = None
        self.instance_url = None
        self.tenant_id = None
        self.schema_cache = {}
        
        # gRPC connection objects (initialized lazily to avoid serialization issues)
        self.channel = None
        self.stub = None
        self._connection_initialized = False
    
    def _ensure_connection(self):
        """Lazily initialize gRPC connection to avoid serialization issues."""
        if not self._connection_initialized:
            try:
                with open(certifi.where(), 'rb') as f:
                    creds = grpc.ssl_channel_credentials(f.read())
                
                self.channel = grpc.secure_channel(f'{self.grpc_host}:{self.grpc_port}', creds)
                self.stub = pb2_grpc.PubSubStub(self.channel)
                self._connection_initialized = True
                
            except Exception as e:
                print(f"⚠️ Failed to initialize gRPC connection: {e}")
                raise RuntimeError(f"Failed to initialize gRPC connection: {e}")
    
    def __getstate__(self):
        """Custom pickle behavior to exclude non-serializable gRPC objects."""
        state = self.__dict__.copy()
        # Remove non-serializable gRPC objects
        state['channel'] = None
        state['stub'] = None
        state['_connection_initialized'] = False
        return state
    
    def __setstate__(self, state):
        """Custom unpickle behavior to restore serializable state."""
        self.__dict__.update(state)
        # gRPC objects will be lazily initialized when needed
    
    @classmethod
    def from_config(cls, config):
        """
        Create a client from configuration dictionary.
        
        Args:
            config (dict): Configuration with grpc_host, grpc_port, etc.
            
        Returns:
            PubSubAPIClient: Configured client instance
        """
        return cls(
            grpc_host=config.get('grpc_host', 'api.pubsub.salesforce.com'),
            grpc_port=int(config.get('grpc_port', 7443))
        )
    
    def authenticate(self, username, password, security_token=None, login_url='https://login.salesforce.com'):
        """
        Authenticate with Salesforce using provided credentials.
        
        Args:
            username (str): Salesforce username
            password (str): Salesforce password
            security_token (str, optional): Security token (can be None if included in password)
            login_url (str): Salesforce login URL
            
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            # Prepare password with security token if provided separately
            full_password = password
            if security_token:
                full_password = password + security_token
            
            # SOAP login request
            soap_body = "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' " + \
                       "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' " + \
                       "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>" + \
                       "<urn:login><urn:username><![CDATA[" + username + \
                       "]]></urn:username><urn:password><![CDATA[" + full_password + \
                       "]]></urn:password></urn:login></soapenv:Body></soapenv:Envelope>"
            
            headers = {'Content-Type': 'text/xml', 'SOAPAction': 'Login'}
            
            response = requests.post(
                f'{login_url}/services/Soap/u/60.0',
                data=soap_body,
                headers=headers
            )
            
            if response.status_code == 200:
                # Parse SOAP response
                import xml.etree.ElementTree as ET
                root = ET.fromstring(response.text)
                res_xml = root[0][0][0]  # Body -> loginResponse -> result
                
                # Extract auth info
                server_url = res_xml[3].text
                url_parts = urlparse(server_url)
                self.instance_url = f"{url_parts.scheme}://{url_parts.netloc}"
                self.session_token = res_xml[4].text
                uinfo = res_xml[6]
                self.tenant_id = uinfo[8].text
                
                return True
            else:
                print(f"Authentication failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"Authentication error: {e}")
            return False
    
    def get_auth_metadata(self):
        """
        Get gRPC auth metadata for authenticated requests.
        
        Returns:
            list: Authentication metadata for gRPC calls
        """
        if not self.session_token:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        
        return [
            ('accesstoken', self.session_token),
            ('instanceurl', self.instance_url),
            ('tenantid', self.tenant_id)
        ]
    
    def get_topic(self, topic_name):
        """
        Get topic information from Salesforce.
        
        Args:
            topic_name (str): Name of the topic (e.g., '/data/AccountChangeEvent')
            
        Returns:
            TopicResponse or None: Topic information or None if error
        """
        try:
            self._ensure_connection()
            request = pb2.TopicRequest(topic_name=topic_name)
            return self.stub.GetTopic(request, metadata=self.get_auth_metadata())
        except grpc.RpcError as e:
            print(f"Error getting topic {topic_name}: {e}")
            return None
    
    def get_schema(self, schema_id):
        """
        Get and cache Avro schema by ID.
        
        Args:
            schema_id (str): Schema ID from Salesforce
            
        Returns:
            avro.schema.Schema or None: Parsed Avro schema or None if error
        """
        if schema_id in self.schema_cache:
            return self.schema_cache[schema_id]
            
        try:
            self._ensure_connection()
            request = pb2.SchemaRequest(schema_id=schema_id)
            response = self.stub.GetSchema(request, metadata=self.get_auth_metadata())
            
            schema_json = json.loads(response.schema_json)
            schema = avro.schema.parse(json.dumps(schema_json))
            self.schema_cache[schema_id] = schema
            return schema
            
        except grpc.RpcError as e:
            print(f"Error getting schema {schema_id}: {e}")
            return None
        except Exception as e:
            print(f"Error parsing schema {schema_id}: {e}")
            return None
    
    def subscribe(self, topic_name, callback=None, replay_preset=None, replay_id_binary=None, num_requested=50):
        """
        Subscribe to topic events with configurable parameters.
        
        Args:
            topic_name (str): Topic to subscribe to
            callback (callable, optional): Custom event handler function
            replay_preset (int, optional): Replay preset (defaults to EARLIEST)
            replay_id_binary (bytes, optional): Binary replay ID to start from
            num_requested (int): Number of events to request per batch
            
        Returns:
            threading.Thread: Subscription thread
        """
        # Ensure connection is available
        self._ensure_connection()
        
        # Get topic info
        topic_info = self.get_topic(topic_name)
        if not topic_info:
            print(f"Topic {topic_name} not found")
            return None
        
        # Default replay preset
        if replay_preset is None:
            replay_preset = pb2.ReplayPreset.EARLIEST
        
        # Subscription control
        subscription_semaphore = threading.Semaphore(1)
        
        def fetch_req_stream():
            """Generate FetchRequests when needed"""
            while True:
                subscription_semaphore.acquire()
                
                # Create fetch request with appropriate replay settings
                if replay_id_binary:
                    # Use binary replay ID - pass original binary data to Salesforce
                    yield pb2.FetchRequest(
                        topic_name=topic_name,
                        replay_preset=pb2.ReplayPreset.CUSTOM,  # ← CRITICAL: Must specify CUSTOM!
                        replay_id=replay_id_binary,
                        num_requested=num_requested
                    )
                else:
                    # Use replay preset (EARLIEST or LATEST)
                    yield pb2.FetchRequest(
                        topic_name=topic_name,
                        replay_preset=replay_preset,
                        num_requested=num_requested
                    )
        
        def handle_events():
            """Process subscription events"""
            try:
                event_stream = self.stub.Subscribe(
                    fetch_req_stream(),
                    metadata=self.get_auth_metadata()
                )
                
                for event in event_stream:
                    if event.events:
                        for received_event in event.events:
                            if callback:
                                callback(received_event)
                            else:
                                self.default_event_handler(received_event, topic_info.schema_id)
                    
                    # Allow next fetch request
                    subscription_semaphore.release()
                    
            except grpc.RpcError as e:
                print(f"Subscription error for {topic_name}: {e}")
            except Exception as e:
                print(f"Unexpected subscription error for {topic_name}: {e}")
        
        # Start subscription thread
        thread = threading.Thread(target=handle_events, name=f"PubSub-{topic_name}")
        thread.daemon = True
        thread.start()
        
        # Start the initial request
        subscription_semaphore.release()
        
        return thread
    
    def default_event_handler(self, event, schema_id):
        """
        Default event handler with decoding.
        
        Args:
            event: Received event from Salesforce
            schema_id (str): Schema ID for decoding
        """
        try:
            print(f"\n🎉 Event received!")
            print(f"   Replay ID: {event.replay_id.decode('utf-8')}")
            print(f"   Time: {time.strftime('%H:%M:%S')}")
            
            # Decode event
            schema = self.get_schema(schema_id)
            if schema:
                reader = avro.io.DatumReader(schema)
                binary_data = io.BytesIO(event.event.payload)
                decoder = avro.io.BinaryDecoder(binary_data)
                decoded_event = reader.read(decoder)
                
                print(f"   Event data: {json.dumps(decoded_event, indent=2)}")
            else:
                print("   Could not decode event")
                
        except Exception as e:
            print(f"   Error processing event: {e}")
    
    def test_connection(self):
        """
        Test the connection and authentication status.
        
        Returns:
            bool: True if connection is healthy, False otherwise
        """
        if not self.session_token:
            return False
        
        try:
            self._ensure_connection()
            # Try to get a simple topic to test connection
            test_topic = "/data/AccountChangeEvent"  # Common topic for testing
            topic_info = self.get_topic(test_topic)
            return topic_info is not None
        except Exception:
            return False
    
    def get_connection_info(self):
        """
        Get connection information for debugging.
        
        Returns:
            dict: Connection information
        """
        return {
            'grpc_host': self.grpc_host,
            'grpc_port': self.grpc_port,
            'instance_url': self.instance_url,
            'tenant_id': self.tenant_id,
            'authenticated': bool(self.session_token),
            'connection_initialized': self._connection_initialized,
            'schema_cache_size': len(self.schema_cache)
        }
    
    def publish_event(self, topic_name, events):
        """
        Publish a single batch of events synchronously.
        
        Args:
            topic_name (str): Topic to publish to
            events (list): List of event dictionaries or ProducerEvent objects
            
        Returns:
            PublishResponse: Response from Salesforce with publish results
        """
        try:
            self._ensure_connection()
            
            # Convert events to ProducerEvent objects if needed
            producer_events = []
            for event in events:
                if isinstance(event, dict):
                    # Convert dict to ProducerEvent
                    producer_event = pb2.ProducerEvent(
                        id=event.get('id', ''),
                        schema_id=event.get('schema_id', ''),
                        payload=event.get('payload', b'')
                    )
                    
                    # Add headers if provided
                    if 'headers' in event and event['headers']:
                        for key, value in event['headers'].items():
                            header = pb2.EventHeader(key=key, value=value)
                            producer_event.headers.append(header)
                    
                    producer_events.append(producer_event)
                else:
                    # Assume it's already a ProducerEvent
                    producer_events.append(event)
            
            # Create publish request
            request = pb2.PublishRequest(
                topic_name=topic_name,
                events=producer_events
            )
            
            # Send publish request
            response = self.stub.Publish(request, metadata=self.get_auth_metadata())
            return response
            
        except grpc.RpcError as e:
            print(f"Error publishing to {topic_name}: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error publishing to {topic_name}: {e}")
            raise
    
    def create_publish_stream(self, topic_name):
        """
        Create a streaming publish session for high-throughput publishing.
        
        Args:
            topic_name (str): Topic to publish to
            
        Returns:
            tuple: (request_queue, response_stream) for streaming publish
        """
        try:
            self._ensure_connection()
            
            import queue
            import threading
            
            # Queue for publish requests
            request_queue = queue.Queue()
            
            def request_generator():
                """Generate publish requests from queue"""
                while True:
                    try:
                        request = request_queue.get(timeout=1)
                        if request is None:  # Sentinel to stop
                            break
                        yield request
                    except queue.Empty:
                        continue
            
            # Start streaming publish
            response_stream = self.stub.PublishStream(
                request_generator(),
                metadata=self.get_auth_metadata()
            )
            
            return request_queue, response_stream
            
        except grpc.RpcError as e:
            print(f"Error creating publish stream for {topic_name}: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error creating publish stream for {topic_name}: {e}")
            raise
    
    def encode_event_to_avro(self, event_data, schema):
        """
        Encode event data to Avro binary format.
        
        Args:
            event_data (dict): Event data to encode
            schema (avro.schema.Schema): Avro schema for encoding
            
        Returns:
            bytes: Avro-encoded binary data
        """
        try:
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(event_data, encoder)
            return bytes_writer.getvalue()
        except Exception as e:
            print(f"Error encoding event to Avro: {e}")
            raise
    
    def close(self):
        """Close gRPC channel and cleanup resources."""
        if self.channel:
            self.channel.close()
        
        # Reset connection state
        self.channel = None
        self.stub = None
        self._connection_initialized = False
        
        # Clear auth info
        self.session_token = None
        self.instance_url = None
        self.tenant_id = None
        self.schema_cache.clear()


# Utility functions for easy client creation
def create_client_from_options(options):
    """
    Create a PubSubAPIClient from Spark readStream options.
    
    Args:
        options (dict): Spark readStream options
        
    Returns:
        PubSubAPIClient: Configured and authenticated client
    """
    # Extract connection options
    grpc_host = options.get('grpcHost', 'api.pubsub.salesforce.com')
    grpc_port = int(options.get('grpcPort', 7443))
    
    # Create client
    client = PubSubAPIClient(grpc_host=grpc_host, grpc_port=grpc_port)
    
    # Authenticate
    username = options.get('username')
    password = options.get('password')
    login_url = options.get('loginUrl', 'https://login.salesforce.com')
    
    if not username or not password:
        raise ValueError("Username and password are required in options")
    
    if not client.authenticate(username=username, password=password, login_url=login_url):
        raise RuntimeError("Failed to authenticate with Salesforce")
    
    return client


def create_client(grpc_host='api.pubsub.salesforce.com', grpc_port=7443):
    """
    Create a new PubSubAPIClient instance (legacy function for compatibility).
    
    Args:
        grpc_host (str): gRPC host
        grpc_port (int): gRPC port
        
    Returns:
        PubSubAPIClient: New client instance
    """
    return PubSubAPIClient(grpc_host, grpc_port)


# Example usage for testing (removed hardcoded credentials)
if __name__ == "__main__":
    print("""
PubSubAPIClient - Parameterized Version

Example usage:

# Method 1: Direct instantiation
client = PubSubAPIClient('api.pubsub.salesforce.com', 7443)
success = client.authenticate(
    username='your-username@example.com',
    password='your-password-and-token',
    login_url='https://login.salesforce.com'
)

# Method 2: From Spark options (recommended for Spark integration)
options = {
    'username': 'your-username@example.com',
    'password': 'your-password-and-token',
    'grpcHost': 'api.pubsub.salesforce.com',
    'grpcPort': '7443',
    'loginUrl': 'https://login.salesforce.com'
}
client = create_client_from_options(options)

# Subscribe to events
client.subscribe('/data/AccountChangeEvent')

# Test connection
if client.test_connection():
    print('✅ Connection healthy')

# Get connection info
print(client.get_connection_info())
""") 